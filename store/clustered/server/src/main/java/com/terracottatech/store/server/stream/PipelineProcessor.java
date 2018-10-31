/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.terracottatech.store.server.stream;

import com.tc.classloader.CommonComponent;
import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.common.messages.UniversalNopMessage;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.PipelineRequestProcessingResponse;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.server.management.StreamShape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.ExplicitRetirementHandle;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.MessageCodecException;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import static com.terracottatech.store.common.InterruptHelper.getUninterruptibly;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DELETE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN_INTERNAL;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.DELETE;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MUTATE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.PIPELINE_REQUEST_RESULT_MESSAGE;
import static com.terracottatech.store.server.management.StreamShape.shapeOf;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;

@CommonComponent
public abstract class PipelineProcessor implements AutoCloseable {

  protected static final SovereignDataset.Durability DURABILITY = SovereignDataset.Durability.IMMEDIATE;
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineProcessor.class);
  private static final Set<IntermediateOperation> BLOCKING_INTERMEDIATES = EnumSet.of(MUTATE_THEN, DELETE_THEN, MUTATE_THEN_INTERNAL);
  private static final Set<TerminalOperation> BLOCKING_TERMINALS = EnumSet.of(MUTATE, DELETE);

  protected final IEntityMessenger<EntityMessage, ?> entityMessenger;

  private final AtomicReference<Object> explanation = new AtomicReference<>();

  private final Executor asyncExecutor;
  private final UUID streamId;

  private final String streamIdentifier;

  private final StreamShape shape;
  private final long openTimestamp;
  private volatile Long closeTimestamp = null;
  private volatile long processingTime = 0;

  /**
   * Indicates whether or not this pipeline associated with this {@link PipelineProcessor} contains an operation that can
   * block (or be blocked) by any other operations.
   */
  private final boolean blocking;

  private volatile CompletableFuture<DatasetEntityResponse> pendingRequestResponse;

  protected PipelineProcessor(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger, ClientDescriptor owner, String datasetAlias, UUID streamId, List<PipelineOperation> intermediate) {
    this(asyncExecutor, entityMessenger, owner, datasetAlias, streamId,
            intermediate.stream().map(PipelineOperation::getOperation).anyMatch(BLOCKING_INTERMEDIATES::contains), shapeOf(intermediate, null));
  }

  protected PipelineProcessor(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger, ClientDescriptor owner, String datasetAlias, UUID streamId, List<PipelineOperation> intermediate, PipelineOperation terminal) {
    this(asyncExecutor, entityMessenger, owner, datasetAlias, streamId,
            intermediate.stream().map(PipelineOperation::getOperation).anyMatch(BLOCKING_INTERMEDIATES::contains) ||
            BLOCKING_TERMINALS.contains(terminal.getOperation()), shapeOf(intermediate, terminal));
   }

  protected PipelineProcessor(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger, ClientDescriptor owner, String datasetAlias, UUID streamId, boolean blocking, StreamShape shape) {
    this.shape = shape;
    this.openTimestamp = System.nanoTime();
    this.asyncExecutor = requireNonNull(asyncExecutor, "asyncExecutor");
    this.entityMessenger = requireNonNull(entityMessenger, "entityMessenger");
    this.streamId = requireNonNull(streamId, "streamId");
    this.blocking = blocking;
    this.streamIdentifier = "remote stream[" + streamId + "] on dataset '" + requireNonNull(datasetAlias, "datasetAlias") + "' for client " + owner;
  }


  /**
   * Creates a {@link BaseStream} from the {@code link SovereignDataset} and portable operations supplied.
   *
   * @param <S> the stream type
   *
   * @param dataset the {@code SovereignDataset} on which the stream is created
   * @param portableOperations the sequence of portable operations to use in the stream pipeline
   * @param requiresExplanation {@code true} if a stream explanation should be generated
   * @return a new {@code StreamDescriptor} having a new stream with the given pipeline and matching the specified type
   *
   * @throws IllegalArgumentException  if the stream resulting from pipeline reconstruction is not the expected type,
   *        if {@code portableOperations} is not all {@code IntermediateOperation} instances,
   *        if a {@code filter} argument is a {@link WaypointMarker}, if {@code mutateThenInternal} is present,
   *        or if {@code mutateThen} argument is not an {@link IntrinsicUpdateOperation}
   */
  protected <K extends Comparable<K>, T, S extends BaseStream<T, S>>
  S createStream(SovereignDataset<K> dataset, List<PipelineOperation> portableOperations, boolean requiresExplanation)
          throws IllegalStateException {

    S stream = reconstructPipeline(dataset, portableOperations, requiresExplanation);
    try {
      if (!getElementType().validateStreamType(stream)) {
        throw new IllegalStateException("ElementType mismatch: have: " + getElementType()
                + "; generated stream type: " + stream.getClass().getName());
      }
    } catch (Exception e) {
      try {
        stream.close();
      } catch (Exception closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }

    return stream;
  }

  /**
   * Opens a {@link Stream} against a {@link SovereignDataset} with a pipeline constructed from the
   * given <i>portable</i> pipeline segment.
   *
   * @param <S> the stream type
   *
   * @param dataset the {@code SovereignDataset} on which the stream is opened
   * @param portableOperations the portable pipeline operation sequence; this sequence must contain only
   *                           {@link PipelineOperation.IntermediateOperation} instances
   *
   * @return a new {@code StreamDescriptor} instance holding the reconstructed stream
   *
   * @throws IllegalArgumentException  if {@code portableOperations} is not all {@code IntermediateOperation} instances,
   *        if a {@code filter} argument is a {@link WaypointMarker}, if {@code mutateThenInternal} is present,
   *        or if {@code mutateThen} argument is not an {@link IntrinsicUpdateOperation}
   */
  @SuppressWarnings("unchecked")
  private <K extends Comparable<K>, T, S extends BaseStream<T, S>>
  S reconstructPipeline(SovereignDataset<K> dataset, List<PipelineOperation> portableOperations, boolean requiresExplanation) {

    /*
     * Final enforcement of IntermediateOperation restriction ...
     */
    for (PipelineOperation op : portableOperations) {
      if (!(op.getOperation() instanceof PipelineOperation.IntermediateOperation)) {
        throw new IllegalArgumentException("portableOperations limited to IntermediateOperations: " + op);
      }
    }

    BaseStream<?, ?> stream = null;     // Intermediate types are unknown
    try {
      stream = dataset.records();

      ((RecordStream<?>) stream).explain(plan -> shape.setIndexed(plan.isSortedCellIndexUsed()));
      if (requiresExplanation) {
        stream = ((RecordStream<?>) stream).explain(explanation::set);
      }

      for (PipelineOperation portableOperation : portableOperations) {
        PipelineOperation.IntermediateOperation operation = (PipelineOperation.IntermediateOperation)portableOperation.getOperation();
        List<Object> operationArguments = new ArrayList<>(portableOperation.getArguments());

        /*
         * Operations supporting mutative functions need special handling -- the mutative functions
         * need to be re-expressed using SovereignDataset methods.
         */
        switch (operation) {
          case DELETE_THEN: {
            /* Maps to a map(dataset.delete(Durability, Function)) operation. */
            operation = PipelineOperation.IntermediateOperation.MAP;
            operationArguments.add(0, dataset.delete(DURABILITY, identity()));
            break;
          }

          case MUTATE_THEN: {
            /*
             * Maps to a map(dataset.applyMutation(Durability, Function, BiFunction)) operation.
             * This operation is used only when mutateThen is both in the portable segment of the
             * pipeline and has a portable transform.
             *
             * This operation MUST have a portable transform and MUST not be preceded by a waypoint.
             */
            Object mutationFunction = operationArguments.get(0);
            if (!(mutationFunction instanceof IntrinsicUpdateOperation)) {
              throw new IllegalArgumentException("Unsupported argument in " + portableOperation
                      + "; " + mutationFunction + " not supported");
            }
            UpdateOperation<K> transform = (UpdateOperation<K>)mutationFunction;
            operation = PipelineOperation.IntermediateOperation.MAP;
            operationArguments.set(0, dataset.applyMutation(DURABILITY, transform::apply, Tuple::of));
            break;
          }

          case FILTER: {
            if (operationArguments.get(0) instanceof WaypointMarker) {
              throw new IllegalArgumentException("Waypoint markers should have been filtered (are you using a batched stream?)");
            }
            break;
          }
          case MUTATE_THEN_INTERNAL: {
            throw new IllegalArgumentException("Mutate should have been filtered (are you using a batched stream?)");
          }
        }

        stream = operation.reconstruct(stream, operationArguments);
      }

    } catch (RuntimeException e) {
      if (stream != null) {
        try {
          stream.close();
        } catch (Exception closeException) {
          e.addSuppressed(closeException);
        }
      }
      throw e;
    }

    return (S)stream;
  }

  /**
   * Gets the stream identifier assigned to this {@code PipelineProcessor}.
   * @return the stream identifier
   */
  public final UUID getStreamId() {
    return streamId;
  }

  protected String getStreamIdentifier() {
    return streamIdentifier;
  }

  protected <M extends PipelineProcessorMessage> DatasetEntityResponse processAsynchronously(M message, Function<M, DatasetEntityResponse> processing) {
    if (pendingRequestResponse != null) {
      throw new IllegalStateException("Stream request pending for " + getStreamIdentifier());
    } else {
      pendingRequestResponse = supplyAsync(() -> processing.apply(message), asyncExecutor);
      ExplicitRetirementHandle<?> retirementHandle = entityMessenger.deferRetirement(getStreamIdentifier(), message, new UniversalNopMessage());
      pendingRequestResponse.whenComplete((r, t) -> {
        try {
          retirementHandle.release();
        } catch (MessageCodecException e) {
          LOGGER.error("Unable to release response to fetch from {}", getStreamIdentifier(), e);
        }
      });
      return new PipelineRequestProcessingResponse(streamId);
    }
  }

  private DatasetEntityResponse requestAsynchronousResponse() {
    if (pendingRequestResponse == null) {
      /* Protocol fault ... */
      throw new IllegalStateException("Stream request not pending for " + getStreamIdentifier());
    } else {
      try {
        return getUninterruptibly(pendingRequestResponse);
      } catch (ExecutionException t) {
        Throwable cause = t.getCause();
        if (cause instanceof Error) {
          throw (Error) cause;
        } else if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else {
          throw new StoreRuntimeException(cause);
        }
      } finally {
        pendingRequestResponse = null;
      }
    }
  }


  @Override
  public void close() {
    try {
      if (pendingRequestResponse != null) {
        pendingRequestResponse.completeExceptionally(
            new StoreRuntimeException(String.format("Stream request failed for %s: pipeline processor closed", getStreamIdentifier())));
      }
    } finally {
      if (closeTimestamp == null) {
        closeTimestamp = System.nanoTime();
      }
    }
  }

  public boolean isComplete() {
    //Regular streams are complete only when they are closed
    return closeTimestamp != null;
  }

  public abstract ElementType getElementType();

  /**
   * Gets the explanation of how this stream is being executed.
   * @return the stream explanation
   */
  public final Optional<Object> getExplanation() {
    return ofNullable(explanation.get());
  }

  /**
   * Indicates whether or not processing of the supplied message may block.
   * @return {@code true} if processing may block awaiting resolution of some externally-managed state
   */
  protected boolean isBlocking(PipelineProcessorMessage message) {
    /*
     * Mutation operations can block because of the locking performed in Sovereign stream support.
     */
    return blocking;
  }

  public DatasetEntityResponse invoke(PipelineProcessorMessage message) {
    if (PIPELINE_REQUEST_RESULT_MESSAGE.equals(message.getType())) {
      return requestAsynchronousResponse();
    } else if (isBlocking(message)) {
      return processAsynchronously(message, this::timedHandleMessage);
    } else {
      return timedHandleMessage(message);
    }
  }

  private DatasetEntityResponse timedHandleMessage(PipelineProcessorMessage message) {
    long messageStart = System.nanoTime();
    try {
      return handleMessage(message);
    } finally {
      processingTime += (System.nanoTime() - messageStart);
    }
  }

  protected DatasetEntityResponse handleMessage(PipelineProcessorMessage message) {
    throw new IllegalArgumentException("Unhandled message type : " + message);
  }

  protected Element elementFor(Object data) {
    //TODO: do we really need to switch on the elementType?
    // using ElementValue in TryAdvanceFetchApplyResponse would skip such need,
    if (data instanceof Record) {
      return new Element(ElementType.RECORD, getRecordData((Record<?>) data));
    } else if (data instanceof Tuple) {
      Object first = ((Tuple) data).getFirst();
      Object second = ((Tuple) data).getSecond();
      if (first instanceof Record) first = getRecordData((Record<?>) first);
      if (second instanceof Record) second = getRecordData((Record<?>) second);
      return getElementType().from(Tuple.of(first, second));
    } else {
      return getElementType().from(data);
    }
  }

  protected static <K extends Comparable<K>> RecordData<K> getRecordData(Record<K> record) {
    return new RecordData<>(getMSN(record), record.getKey(), record);
  }

  private static long getMSN(Record<?> record) {
    return ((SovereignPersistentRecord) record).getMSN();
  }

  public StreamShape getStreamShape() {
    return shape;
  }

  public long getServerTime() {
    return processingTime;
  }

  public long getTotalTime() {
    if (closeTimestamp == null) {
      return System.nanoTime() - openTimestamp;
    } else {
      return closeTimestamp - openTimestamp;
    }
  }
}
