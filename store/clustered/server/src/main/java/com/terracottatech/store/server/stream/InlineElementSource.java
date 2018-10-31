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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.store.common.exceptions.RemoteStreamClosedException;
import com.terracottatech.store.common.exceptions.RemoteStreamException;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.NonPortableTransform;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchApplyResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchConsumedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchExhaustedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseResponse;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchWaypointResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceMutateMessage;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.IEntityMessenger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP;
import static com.terracottatech.store.server.management.StreamShape.shapeOf;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Supports element-at-a-time access to an element source obtained from a {@link SovereignDataset}.
 * An {@code InlineElementSource} instance is expected to be <i>fetched</i> from a single consumer; consumption
 * from multiple, simultaneous consumers is not defined.
 *
 * @param <T> the stream element type
 */
public class InlineElementSource<K extends Comparable<K>, T> extends PipelineProcessor implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(InlineElementSource.class);

  private final SovereignDataset<K> dataset;
  private final ElementType elementType;
  private final BaseStream<?, ?> serverStream;

  /**
   * Indicates if this {@code InlineElementSource} serves a pipeline that returns no terminal result.
   * This occurs when the client-side pipeline is terminated by a mutative consuming operation.
   * A stream of this returns a {@link TryAdvanceFetchConsumedResponse} when element processing
   * is complete and does not require a {@link TryAdvanceReleaseMessage} to resume processing.
   */
  private final boolean consumedStream;

  private final Lock elementSourceLock = new ReentrantLock();
  private final Supplier<Spliterator<T>> elementSourceSpliteratorSupplier;
  private Spliterator<T> elementSourceSpliterator;

  private final SynchronousQueue<CompletableFuture<Object>> transferQueue = new SynchronousQueue<>();
  private final Phaser releaseGate = new Phaser(2);

  /**
   * Holds the {@link CompletableFuture} used to present a fetched element or {@link WaypointRequest}
   * to the {@link #fetch()} or {@link #continueFetch(int, Object)} method.
   */
  private final AtomicReference<CompletableFuture<Object>> opFuture = new AtomicReference<>(null);

  private volatile WaypointRequest<?> pendingWaypoint = null;

  private Thread elementProducerThread = null;

  private final ReadWriteLock stateLock = new ReentrantReadWriteLock();
  /**
   * The foreground {@code InlineElementSource} state.
   */
  @GuardedBy("stateLock")
  private State currentState = State.READY;

  private enum State {
    /**
     * Indicates ready to accept fetch requests.
     */
    READY {
      @Override
      EnumSet<State> validTargetStates() {
        return EnumSet.of(FETCH, CLOSED);
      }
    },
    /**
     * Indicates fetch operation underway.
     */
    FETCH {
      @Override
      EnumSet<State> validTargetStates() {
        return EnumSet.of(WAYPOINT_PENDING, READY, RELEASE_REQUIRED, DRAINED, CLOSED);
      }
    },
    /**
     * Indicates remote compute/waypoint operation underway.
     */
    WAYPOINT_PENDING {
      @Override
      EnumSet<State> validTargetStates() {
        return EnumSet.of(FETCH, READY, CLOSED);
      }
    },
    /**
     * Indicates fetch complete and release needed.
     */
    RELEASE_REQUIRED {
      @Override
      EnumSet<State> validTargetStates() {
        return EnumSet.of(READY, CLOSED);
      }
    },
    /**
     * Indicates pipeline is exhausted.
     * Can move into {@code CLOSED}.
     */
    DRAINED {
      @Override
      EnumSet<State> validTargetStates() {
        return EnumSet.of(CLOSED);
      }
    },
    /**
     * Indicates the pipeline is closed.
     */
    CLOSED {
      @Override
      EnumSet<State> validTargetStates() {
        return EnumSet.noneOf(getDeclaringClass());
      }
    },
    ;

    abstract EnumSet<State> validTargetStates();

    State validateInto(State targetState) {
      if (targetState == this || validTargetStates().contains(targetState)) {
        return targetState;
      } else {
        throw new IllegalStateException("Cannot move from " + name() + " to " + targetState.name());
      }
    }
  }

  /**
   * Creates a new {@code InlineElementSource} instance reconstructing stream against the designated {@link SovereignDataset}
   * using the portable operations supplied.
   *
   * @param owner the {@code ClientDescriptor} of the owner of this element source
   * @param streamId the identifier of this element source
   * @param elementType the stream type of this element source
   * @param dataset the {@code SovereignDataset} on which this element source is built
   * @param portableOperations the sequence of portable operations for the source stream
   *
   * @throws IllegalArgumentException  if the stream resulting from pipeline reconstruction is not the expected type,
   *        if {@code portableOperations} is not all {@code IntermediateOperation} instances,
   *        if a {@code filter} argument is a {@code WaypointMarker}, if {@code mutateThenInternal} is present,
   *        or if {@code mutateThen} argument is not an {@code IntrinsicUpdateOperation}
   */
  @SuppressWarnings("unchecked")
  public InlineElementSource(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger,
                             ClientDescriptor owner, UUID streamId, ElementType elementType,
                             SovereignDataset<K> dataset, List<PipelineOperation> portableOperations, boolean requiresExplanation) {
    super(asyncExecutor, entityMessenger, owner, dataset.getAlias(), streamId, portableOperations);
    this.dataset = dataset;
    this.elementType = elementType;
    this.serverStream = createStream(dataset, processIntermediates(portableOperations), requiresExplanation);
    this.consumedStream = false;
    this.elementSourceSpliteratorSupplier = () -> (Spliterator<T>)this.serverStream.spliterator();   // unchecked
  }

  /**
   * Creates a new {@code InlineElementSource} instance reconstructing stream against the designated {@link SovereignDataset}
   * using the portable operations supplied.  This constructor appends the non-terminal equivalent of a mutative
   * terminal pipeline operation to the current pipeline.  This constructor is used to extend the pipeline in the
   * {@code InlineElementSource} with the proper intermediate operation equivalent of a non-portable or partially-portable
   * mutative terminal operation.
   *
   * @param owner the {@code ClientDescriptor} of the owner of this element source
   * @param streamId the identifier of this element source
   * @param elementType the stream type of this element source
   * @param dataset the {@code SovereignDataset} on which this element source is built
   * @param portableOperations the sequence of portable operations for the source stream
   * @param terminalOperation the mutative terminal operation to append to the pipeline
   *
   * @throws IllegalArgumentException  if the stream resulting from pipeline reconstruction is not the expected type,
   *        if {@code portableOperations} is not all {@code IntermediateOperation} instances,
   *        if a {@code filter} argument is a {@code WaypointMarker}, if {@code mutateThenInternal} is present,
   *        or if {@code mutateThen} argument is not an {@code IntrinsicUpdateOperation}
   */
  @SuppressWarnings("unchecked")
  public InlineElementSource(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger,
                             ClientDescriptor owner, UUID streamId, ElementType elementType,
                             SovereignDataset<K> dataset, List<PipelineOperation> portableOperations,
                             PipelineOperation terminalOperation, boolean requiresExplanation) {
    super(asyncExecutor, entityMessenger, owner, dataset.getAlias(), streamId, portableOperations, terminalOperation);
    this.dataset = dataset;
    this.elementType = elementType;
    this.serverStream = createStream(dataset, processIntermediates(portableOperations, terminalOperation), requiresExplanation);
    this.consumedStream = true;
    this.elementSourceSpliteratorSupplier = () -> (Spliterator<T>)this.serverStream.spliterator();   // unchecked
  }

  /**
   * Creates a new {@code InlineElementSource}.  This internal constructor is package-private for unit testing purposes.
   *
   * @param dataset the {@code SovereignDataset} on which this element source is built
   * @param owner the {@code ClientDescriptor} of the owner of this element source
   * @param streamId the identifier of this element source
   * @param elementType the stream type of this element source
   * @param serverStream the stream serving as the element source; the pipeline built on the stream must be
   *                     non-mutative
   */
  @SuppressWarnings("unchecked")
  InlineElementSource(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger,
                      SovereignDataset<K> dataset, ClientDescriptor owner, UUID streamId, ElementType elementType, BaseStream<?, ?> serverStream) {
    super(asyncExecutor, entityMessenger, owner, dataset.getAlias(), streamId, false, shapeOf(Collections.emptyList(), null));
    this.dataset = requireNonNull(dataset, "dataset");
    this.elementType = requireNonNull(elementType, "elementType");
    this.serverStream = requireNonNull(serverStream, "serverStream");
    this.consumedStream = false;
    this.elementSourceSpliteratorSupplier = () -> (Spliterator<T>)this.serverStream.spliterator();   // unchecked
  }

  private List<PipelineOperation> processIntermediates(List<PipelineOperation> intermediateOperations, PipelineOperation mutativeTerminal) {

    Map<Integer, Waypoint> waypoints = new HashMap<>();

    List<PipelineOperation> filteredPipeline = filterAndProcessWaypoints(intermediateOperations, waypoints);

    /*
     * Appends the non-terminal equivalent of a mutative terminal pipeline operation to the current
     * pipeline.  This is used to extend the pipeline in this {@code InlineElementSource}
     * with the proper intermediate operation equivalent of a non-portable or partially-portable
     * mutative terminal operation.  (Fully-portable pipelines must be handled using
     * {@link #executeTerminatedPipeline(PipelineOperation)}.)
     */

    List<Object> operationArguments = new ArrayList<>(mutativeTerminal.getArguments());
    switch ((TerminalOperation)mutativeTerminal.getOperation()) {
      case DELETE: {
        /* Maps to map(dataset.delete(Duration, identity)) operation. */
        filteredPipeline.add(MAP.newInstance(dataset.delete(DURABILITY, identity())));
        break;
      }

      case MUTATE: {
        /*
         * Maps to map(dataset.applyMutation(Duration, Function, BiFunction)) operation.
         *
         * This operation could have a portable transform or a non-portable transform.  For
         * a portable transform, a waypoint is used but is not relevant to the update -- the
         * transform can be performed inline.  For a non-portable transform, a waypoint must be present
         * to enable client-side calculation of the mutation.
         */
        Object mutationFunction = operationArguments.get(0);
        if (!(mutationFunction instanceof IntrinsicUpdateOperation)) {
          throw new IllegalArgumentException("Unsupported argument in " + mutativeTerminal
                  + "; " + mutationFunction + " not supported");
        }
        @SuppressWarnings("unchecked")
        UpdateOperation<K> transform = (UpdateOperation<K>)mutationFunction;  // unchecked

        if (transform instanceof NonPortableTransform) {
          // Non-portable transform
          int waypointId = ((NonPortableTransform<K>)transform).getWaypointId();
          Waypoint waypoint = waypoints.get(waypointId);
          if (waypoint == null) {
            throw new IllegalStateException("Improperly formed pipeline: waypoint " + waypointId
                    + " is missing before " + mutativeTerminal);
          }
          filteredPipeline.add(MAP.newInstance(dataset.applyMutation(DURABILITY, r -> waypoint.getCells(), (oldRecord, newRecord) -> newRecord)));
        } else {
          // Portable transform
          if (waypoints.isEmpty()) {
            throw new IllegalStateException("Improperly formed pipeline: waypoint is missing before " + mutativeTerminal);
          }
          filteredPipeline.add(MAP.newInstance(dataset.applyMutation(DURABILITY, transform::apply, (oldRecord, newRecord) -> newRecord)));
        }
        break;
      }

      default: {
        throw new IllegalArgumentException("Unsupported terminal operation - " + mutativeTerminal);
      }
    }

    return filteredPipeline;
  }

  private List<PipelineOperation> processIntermediates(List<PipelineOperation> intermediateOperations) {
    return filterAndProcessWaypoints(intermediateOperations, new HashMap<>());
  }

  private List<PipelineOperation> filterAndProcessWaypoints(List<PipelineOperation> intermediateOperations, Map<Integer, Waypoint> waypoints) {

    List<PipelineOperation> filteredPipeline = new ArrayList<>(intermediateOperations.size());

    for (PipelineOperation intermediate : intermediateOperations) {
      /*
       * Final enforcement of IntermediateOperation restriction ...
       */
      if (!(intermediate.getOperation() instanceof IntermediateOperation)) {
        throw new IllegalArgumentException("portableOperations limited to IntermediateOperations: " + intermediate);
      }

      IntermediateOperation operation = (IntermediateOperation) intermediate.getOperation();
      List<Object> operationArguments = intermediate.getArguments();

      /*
       * Operations supporting mutative functions need special handling -- the mutative functions
       * need to be re-expressed using SovereignDataset methods.
       */
      switch (operation) {
        case FILTER: {
          Object predicate = operationArguments.get(0);
          if (predicate instanceof WaypointMarker) {
            /*
             * A WaypointMarker is used to indicate a server-to-client information exchange
             * in a mutative pipeline.  At the waypoint, the server returns the current
             * pipeline element to the client and awaits a response before continuing with
             * the server-side pipeline.
             */
            int waypointId = ((WaypointMarker) predicate).getWaypointId();
            Waypoint waypoint = new Waypoint(waypointId);
            waypoints.put(waypointId, waypoint);
            filteredPipeline.add(FILTER.newInstance(waypointPredicate(waypoint)));
          } else {
            filteredPipeline.add(intermediate);
          }
          break;
        }

        case MUTATE_THEN_INTERNAL: {
          /*
           * Maps to a map(dataset.applyMutation(Durability, Function, BiFunction)) operation.
           * This operation is used only when mutateThen is in the non-portable segment of the
           * pipeline or has a non-portable transform.  This operation's output is Record<K> and,
           * because of the API difference, can be followed by no other.
           *
           * This operation MAY have a portable transform and MUST be preceded by a waypoint.
           */
          Object mutationFunction = operationArguments.get(0);
          if (!(mutationFunction instanceof IntrinsicUpdateOperation)) {
            throw new IllegalArgumentException("Unsupported argument in " + intermediate
                    + "; " + mutationFunction + " not supported");
          }
          @SuppressWarnings("unchecked")
          UpdateOperation<K> transform = (UpdateOperation<K>) mutationFunction;

          /*
           * This operation can have either a portable transform or a non-portable transform.  For
           * a non-portable transform or a portable transform in a non-portable pipeline, a waypoint
           * is used synchronize operations with the client-side pipeline.  For a non-portable transform,
           * the waypoint provides the update cell set.
           */
          if (waypoints.isEmpty()) {
            throw new IllegalStateException("Improperly formed pipeline: no waypoint present before " + intermediate);
          }
          if (transform instanceof NonPortableTransform) {
            // Non-portable transform -- waypoint is required
            int waypointId = ((NonPortableTransform<K>) transform).getWaypointId();
            Waypoint waypoint = waypoints.get(waypointId);
            if (waypoint == null) {
              throw new IllegalStateException("Improperly formed pipeline: waypoint " + waypointId + " is missing");
            }
            filteredPipeline.add(MAP.newInstance(dataset.applyMutation(DURABILITY, r -> waypoint.getCells(), (oldRecord, newRecord) -> newRecord)));

          } else {
            filteredPipeline.add(MAP.newInstance(dataset.applyMutation(DURABILITY, transform::apply, (oldRecord, newRecord) -> newRecord)));
          }
          break;
        }

        default: {
          filteredPipeline.add(intermediate);
          break;
        }
      }
    }
    return filteredPipeline;
  }

  /**
   * Returns a {@link Predicate} serving as a mutative pipeline <i>waypoint</i>.  The waypoint completes the
   * current {@link #fetch()} or {@link #continueFetch(int, Object) continueFetch} operation with
   * the {@link Record} from the pipeline and suspends the pipeline awaiting a response, from the client
   * via {@code continueFetch}, containing the {@link Iterable Iterable<Cell<?>>} to use for the mutation.
   *
   * @param waypoint the {@link Waypoint} instance through which communication with downstream pipeline
   *                 elements is done
   *
   * @return a new {@code Predicate} performing the waypoint function; if the waypoint is informed that
   *        the client dropped the element (as it might for a non-portable filter), the {@code Predicate}
   *        throws an {@link ElementDroppedException}
   */
  private Predicate<Record<K>> waypointPredicate(Waypoint waypoint) {
    return new WaypointPredicate(waypoint);
  }

  /**
   * Gets the {@link ElementType} declared for this {@code InlineElementSource}.
   * @return the declared element type
   */
  @Override
  public ElementType getElementType() {
    return elementType;
  }

  @Override
  protected boolean isBlocking(PipelineProcessorMessage message) {
    switch (message.getType()) {
      case TRY_ADVANCE_MUTATE_MESSAGE:
        return true;
      case TRY_ADVANCE_FETCH_MESSAGE:
        return super.isBlocking(message);
      default:
        return false;
    }
  }

  @Override
  public DatasetEntityResponse handleMessage(PipelineProcessorMessage message) {
    switch (message.getType()) {
      case TRY_ADVANCE_FETCH_MESSAGE:
        return tryAdvanceFetch();
      case TRY_ADVANCE_MUTATE_MESSAGE:
        return tryAdvanceMutate((TryAdvanceMutateMessage) message);
      case TRY_ADVANCE_RELEASE_MESSAGE:
        return release();
      default:
        return super.handleMessage(message);
    }
  }

  private DatasetEntityResponse tryAdvanceFetch() {
    /*
     * Non-blocking fetch can be handled in message handling thread.
     */
    return convertFetchResponse(fetchElement(this::fetch));
  }

  private DatasetEntityResponse tryAdvanceMutate(TryAdvanceMutateMessage message) {
    /*
     * Resumes processing of an element in a mutative pipeline previously suspended on reaching a waypoint.
     * This operation can end with the return of an element or an exception.
     */
    Supplier<Object> dataSource = () -> continueFetch(message.getWaypointId(), message.getCells());

    return convertFetchResponse(fetchElement(dataSource));
  }

  /**
   * Converts the response from {@link InlineElementSource#fetch()} or {@link InlineElementSource#continueFetch(int, Object)}
   * to the response required to the client.
   *
   * @param data the {@code fetch} or {@code continueFetch} result
   * @return a {@code DatasetEntityResponse} instance to return to the client
   *
   * @throws IllegalStateException if the type of {@code data} is not what was expected
   */
  private DatasetEntityResponse convertFetchResponse(Object data) {
    if (data == null) {
      // Stream exhausted
      return new TryAdvanceFetchExhaustedResponse(getStreamId());

    } else {
      if (data == NonElement.CONSUMED) {
        return new TryAdvanceFetchConsumedResponse(getStreamId());

      } else if (data instanceof WaypointRequest) {
        WaypointRequest<?> computeRequest = (WaypointRequest) data;
        Object element = computeRequest.getPendingElement();
        if (!(element instanceof Record)) {
          throw new IllegalStateException("Unexpected element type for WaypointRequest: " + element.getClass().getName());
        }
        @SuppressWarnings("unchecked") Record<K> record = (Record<K>)Record.class.cast(element);
        return new TryAdvanceFetchWaypointResponse<>(getStreamId(), getRecordData(record), computeRequest.getWaypointId());

      } else {
        return new TryAdvanceFetchApplyResponse<>(getStreamId(), elementFor(data));
      }
    }
  }

  /**
   * Invokes the data source {@link Supplier} built using {@link InlineElementSource#fetch()} or
   * {@link InlineElementSource#continueFetch(int, Object)} and returns the element obtained.
   *
   * @param dataSource the {@code Supplier} through which the {@code InlineElementSource} fetch is made
   * @return the element representation from {@code dataSource}; this value must be presented to
   *      {@link #convertFetchResponse(Object)} for interpretation
   *
   * @throws NoSuchElementException if thrown by the fetch operation
   * @throws ClassCastException if thrown by the fetch operation
   * @throws StoreRuntimeException if an unexpected exception is thrown by the fetch operation
   */
  private static Object fetchElement(Supplier<Object> dataSource) {
    try {
      return dataSource.get();
    } catch (RemoteStreamException e) {
      Throwable cause = e.getCause();

      // NoSuchElementException is thrown by Optional.get when no element is present
      // ClassCastException is thrown by sorted() if the stream element isn't Comparable
      // Unwrap these exceptions to make them more directly visible

      if (cause instanceof NoSuchElementException) {
        throw (NoSuchElementException) cause;
      }

      if (cause instanceof ClassCastException) {
        throw (ClassCastException) cause;
      }

      throw new StoreRuntimeException(e);
    }
  }

  /**
   * Package-private method for unit testing.
   */
  String getCurrentState() {
    return currentState.name();
  }

  /**
   * Package-private method for unit testing.
   */
  synchronized Thread getElementProducerThread() {
    return elementProducerThread;
  }

  /**
   * Gets the element source {@link Spliterator}.  The first call to this method creates the {@code Spliterator}.
   * @return the element source {@code Spliterator}
   */
  public Spliterator<T> getSourceSpliterator() {
    elementSourceLock.lock();
    try {
      if (elementSourceSpliterator == null) {
        elementSourceSpliterator = elementSourceSpliteratorSupplier.get();
      }
      return elementSourceSpliterator;
    } finally {
      elementSourceLock.unlock();
    }
  }

  /**
   * Read elements from the {@link #getSourceSpliterator() element source} and
   * make each element available to the {@link #fetch()} method.  The element source is not opened
   * until the first call to {@code fetch}.  An exception raised from the element source is re-thrown
   * by the {@link #fetch()} method.
   */
  @Override
  public void run() {
    synchronized (this) {
      elementProducerThread = Thread.currentThread();
    }

    try {
      /*
       * A close can sneak in before elementProducerThread is set resulting in this thread not being
       * interrupted.  If closed at this point, just exit.
       */
      if (isClosed()) {
        return;
      }

      LOGGER.debug("Starting background thread for {}", getStreamIdentifier());

      try {
        CompletableFuture<Object> completableFuture;
        do {
          /*
           * Fetch is enabled when a CompletableFuture, the device through which element
           * responses are returned to the 'fetch' method, is presented.  Closure is
           * indicated by thread interruption.
           */
          completableFuture = transferQueue.take();

        } while (tryAdvance(getSourceSpliterator(), completableFuture));

        LOGGER.debug("Exhausted feed from background thread for {}", getStreamIdentifier());
        completableFuture.complete(NonElement.EXHAUSTED);

      } catch (InlineElementSourceClosedException | InterruptedException | InlineElementSourceException e) {
        close();     // possibly redundant
      }

    } finally {
      synchronized (this) {
        elementProducerThread = null;
        Thread.interrupted();     // Clear the interrupt state if close beat us here
      }

      LOGGER.debug("Exiting background thread for {}", getStreamIdentifier());
    }
  }

  /**
   * {@link CompletableFuture#complete(Object) Completes} the {@code CompletableFuture} with the next element from
   * the pipeline behind the {@link Spliterator} provided.  If the pipeline includes a client-side (remote)
   * computation, the {@code CompletableFuture} is completed with a {@link WaypointRequest} indicating the
   * current stream element for which a computation is needed.  If the pipeline throws an exception, the
   * {@code CompletableFuture} is {@link CompletableFuture#completeExceptionally(Throwable) completed "exceptionally"}.
   *
   * @param spliterator the {@code Spliterator} from which the element is to be obtained
   * @param completableFuture the {@code CompletableFuture} into which the element is "completed"
   *
   * @return {@code true} if an element was obtained from the spliterator; {@code false} if the spliterator
   *        was exhausted
   *
   * @throws InlineElementSourceClosedException if the stream is closed
   * @throws InlineElementSourceException if the pipeline threw an exception
   */
  private boolean tryAdvance(Spliterator<T> spliterator, CompletableFuture<Object> completableFuture)
      throws InlineElementSourceClosedException, InlineElementSourceException {
    opFuture.set(completableFuture);
    try {
      return spliterator.tryAdvance(element -> {
        if (isClosed()) {
          throw new InlineElementSourceClosedException();
        }
        /*
         * If the pipeline holds a remote operation, the CompletableFuture is completed with a
         * WaypointRequest and the pipeline is suspended until that request is satisfied.  When
         * resuming the fetch (via continueFetch), the CompletableFuture in opFuture is replaced
         * with fresh one allocated in continueFetch and the pipeline is resumed.  If the pipeline
         * logically ends with with a consumer (consumedStream == true), then the element from the
         * spliterator is "consumed" and not returned to the client and the element does not require
         * "releasing".
         */
        CompletableFuture<Object> future = opFuture.getAndSet(null);
        if (future == null) {
          throw new IllegalStateException(
              "tryAdvance() called on " + getStreamIdentifier() + " with null CompletableFuture");
        }

        if (consumedStream) {
          future.complete(NonElement.CONSUMED);

        } else {
          future.complete(element);
          try {
            if (releaseGate.awaitAdvanceInterruptibly(releaseGate.arrive()) < 0) {
              throw new InlineElementSourceClosedException();
            }
          } catch (InterruptedException e) {
            throw new InlineElementSourceClosedException(e);
          }
        }

      });
    } catch (ElementDroppedException e) {
      /*
       * An element dropped at a waypoint is not consumed by the remainder of the pipeline
       * -- processing needs to be returned to the client for a new fetch.
       */
      return true;
    } catch (InlineElementSourceClosedException e) {
      throw e;
    } catch (RemoteStreamClosedException e) {
      // Closure observed via WaypointPredicate call to getElements
      throw new InlineElementSourceClosedException(e);
    } catch (RuntimeException e) {
      // NoSuchElementException is thrown by Optional.get() when no value is present; don't log this one
      if (!(e instanceof NoSuchElementException)) {
        LOGGER.warn("{} exception: " + e, getStreamIdentifier(), e);
      }
      completableFuture.completeExceptionally(e);
      throw new InlineElementSourceException(e);
    }
  }

  /**
   * Fetch the next element from the element source.  If the background element producer thread raised
   * an exception, that exception is reflected from this method.  If a {@link WaypointRequest} is
   * returned, {@link #continueFetch(int, Object)} must be called to complete the fetch operation.
   *
   * @return an element, a {@link WaypointRequest}, or {@code null} indicating stream exhaustion
   *
   * @throws IllegalStateException if this method is called with an unexpected internal state
   * @throws RemoteStreamClosedException if this {@code InlineElementSource} is closed
   * @throws RemoteStreamException if the background element producer for this {@code InlineElementSource} failed
   */
  public Object fetch() throws RemoteStreamClosedException, RemoteStreamException {
    Lock lock = stateLock.writeLock();
    lock.lock();
    try {
      switch (currentState) {
        case CLOSED:
          raiseClosed();
          throw new AssertionError("unreachable");
        case DRAINED:
          return null;
        case READY:
          setState(State.FETCH);
          break;
        default:
          throw new IllegalStateException("fetch() called on " + getStreamIdentifier() + " with " + currentState);
      }
    } finally {
      lock.unlock();
    }

    CompletableFuture<Object> completableFuture = new CompletableFuture<>();
    boolean interrupted = Thread.interrupted();
    try {
      do {
        if (isClosed()) {
          raiseClosed();
        }
      } while (!transferQueue.offer(completableFuture, 100L, TimeUnit.MILLISECONDS));
    } catch (InterruptedException e) {
      interrupted = true;
    } finally {
      if (interrupted) {
        // Re-raise the interrupt, if any
        Thread.currentThread().interrupt();
      }
    }

    return getElement(completableFuture, false);
  }

  /**
   * Continues fetch for the next element from the element source.  If the background element producer thread
   * raised an exception, that exception is reflected from this method.  This method is called after a
   * {@link WaypointRequest} is returned from {@link #fetch()}.  If a {@link WaypointRequest} is
   * returned, {@code continueFetch} must be called again to continue the fetch operation.
   *
   * @return an element, {@link NonElement#CONSUMED}, or a {@link WaypointRequest}
   *
   * @throws IllegalStateException if this method is called with an unexpected internal state
   * @throws RemoteStreamClosedException if this {@code InlineElementSource} is closed
   * @throws RemoteStreamException if the background element producer for this {@code InlineElementSource} failed
   */
  public <R> Object continueFetch(int waypointId, R response) {
    Lock lock = stateLock.writeLock();
    lock.lock();
    try {
      switch (currentState) {
        case CLOSED:
          raiseClosed();
          throw new AssertionError("unreachable");
        case WAYPOINT_PENDING:
          setState(State.FETCH);
          break;
        default:
          throw new IllegalStateException(
              "continueFetch() called on " + getStreamIdentifier() + " with " + currentState);
      }
    } finally {
      lock.unlock();
    }

    WaypointRequest<?> pendingWaypoint = this.pendingWaypoint;
    if (pendingWaypoint == null) {
      throw new IllegalStateException("continueFetch called with pendingWaypoint == null");
    }
    if (pendingWaypoint.getWaypointId() != waypointId) {
      throw new IllegalStateException("continueFetch(" + waypointId
          + ") called when pending waypointId=" + pendingWaypoint.getWaypointId());
    }

    /*
     * Set future for next completion stage.
     */
    CompletableFuture<Object> completableFuture = new CompletableFuture<>();
    opFuture.set(completableFuture);

    /*
     * Complete the waypoint and permit the pipeline to continue
     */
    pendingWaypoint.getResponseFuture().complete(response);

    return getElement(completableFuture, false);
  }

  /**
   * Gets the next element or request from the element source.
   *
   * @param completableFuture the {@code CompletableFuture} through which the background element source
   *                          provides the element or request
   * @param fromWaypoint if {@code true}, indicates this call is from within the {@link WaypointPredicate}
   *                     running in the background pipeline and state management is <b>not</b> to be
   *                     performed; if {@code false}, state management is to be performed.
   *
   * @return an element, a {@link NonElement}, or {@code null} indicating stream exhaustion
   *
   * @throws RemoteStreamException if the background element producer completed {@code completableFuture} with
   *      an exception
   * @throws IllegalStateException if the background element producer provided an unexpected response
   */
  private Object getElement(CompletableFuture<Object> completableFuture, boolean fromWaypoint) {
    boolean interrupted = Thread.interrupted();
    Object sentinel = new Object();
    try {
      Object element = sentinel;
      do {
        // Might be closed due to error completion -- error completion dominates
        if (isClosed() && !completableFuture.isCompletedExceptionally()) {
          raiseClosed();
        }
        try {
          element = completableFuture.get(100L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (TimeoutException e) {
          // Ignored -- just retry
        } catch (ExecutionException e) {
          // cause() is a RuntimeException thrown from the pipeline
          Throwable cause = e.getCause();
          throw new RemoteStreamException("Exception raised on" + getStreamIdentifier() + ": " + cause, cause);
        }
      } while (element == sentinel);

      if (fromWaypoint) {
        /*
         * The WaypointPredicate handles it's own result interpretation and does not manage state.
         */
        return element;
      } else if (element instanceof NonElement) {
        if (element == NonElement.EXHAUSTED) {
          setState(State.DRAINED);
          return null;

        } else if (element == NonElement.CONSUMED) {
          setState(State.READY);
          return element;

        } else if (element == NonElement.RELEASED) {
          setState(State.READY);
          return element;

        } else if (element instanceof WaypointRequest) {
          pendingWaypoint = (WaypointRequest) element;
          setState(State.WAYPOINT_PENDING);
          return element;

        } else {
          throw new IllegalStateException("Unexpected response observed by getElements() on "
              + getStreamIdentifier() + ": " + element);
        }
      } else {
        setState(State.RELEASE_REQUIRED);
        return element;
      }
    } finally {
      if (interrupted) {
        // Re-raise the interrupt, if any
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Releases the last fetched {@link Record}.
   */
  public DatasetEntityResponse release() throws RemoteStreamException {
    Lock lock = stateLock.writeLock();
    lock.lock();
    try {
      switch (currentState) {
        case CLOSED:
          // Release is unnecessary after close
          return new TryAdvanceReleaseResponse(getStreamId());
        case RELEASE_REQUIRED:
          break;
        case WAYPOINT_PENDING:
          /*
           * The stream element is being released without completing pipeline processing.
           * At a pending waypoint, the releaseGate is not involved ...
           */
          pendingWaypoint.getResponseFuture().complete(NonElement.RELEASED);
          setState(State.READY);
          return new TryAdvanceReleaseResponse(getStreamId());
        default:
          throw new IllegalStateException("release() called on " + getStreamIdentifier() + " with " + currentState);
      }
    } finally {
      lock.unlock();
    }

    releaseGate.awaitAdvance(releaseGate.arrive());
    // Normal completion of the wait indicates closure or forEachRemaining loop release;
    // in either event, release is no longer needed.
    setState(State.READY);
    return new TryAdvanceReleaseResponse(getStreamId());
  }

  /**
   * Closes this {@code InlineElementSource} interrupting the background element provider thread.
   * This method <i>interrupts</i> the background element producer thread but does <b>not</b>
   * interrupt possible waiting consumer threads.
   */
  @Override
  public void close() {
    boolean closing = setState(State.CLOSED);
    if (closing) {
      synchronized (this) {
        if (elementProducerThread != null && !Thread.currentThread().equals(elementProducerThread)) {
          elementProducerThread.interrupt();
        }
      }
      releaseGate.forceTermination();                 // Break the barrier
      elementSourceLock.lock();
      try {
        elementSourceSpliterator = null;
      } finally {
        elementSourceLock.unlock();
      }
      try {
        serverStream.close();
      } catch (Exception e) {
        LOGGER.warn("Exception raised while closing {} - {}", getStreamIdentifier(), e, e);
      } finally {
        super.close();
      }
      LOGGER.debug("Closed {}", getStreamIdentifier());
    }
  }

  private boolean isClosed() {
    Lock readLock = stateLock.readLock();
    readLock.lock();
    try {
      return currentState == State.CLOSED;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Sets {@link #currentState} to a new value.
   * @param newState the new {@code State} value
   * @return {@code true} if {@code currentState} was <i>changed</i> to {@code newState};
   *      {@code false} if {@code currentState} was already set to {@code newState}
   */
  private boolean setState(State newState) {
    Lock lock = stateLock.writeLock();
    lock.lock();
    try {
      if (currentState == newState) {
        return false;
      } else {
        currentState = currentState.validateInto(newState);
        return true;
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Handles mainline notification for a close.
   *
   * @throws RemoteStreamClosedException if the element source is closed "normally"
   */
  private void raiseClosed() throws RemoteStreamException, RemoteStreamClosedException {
    throw new RemoteStreamClosedException("Element source closed on " + getStreamIdentifier());
  }

  /**
   * A {@link Predicate} which, when provided to {@link Stream#filter(Predicate)} operation in a pipeline,
   * provides a <i>waypoint</i> or interception point in the pipeline for an interaction with the client-side
   * portion of the pipeline.
   */
  private final class WaypointPredicate implements Predicate<Record<K>> {
    private final Waypoint waypoint;

    private WaypointPredicate(Waypoint waypoint) {
      this.waypoint = waypoint;
    }

    @Override
    public boolean test(Record<K> record) {
      /*
       * Signal sending a TryAdvanceFetchWaypointResponse containing the Record to the client.
       */
      CompletableFuture<Object> future = opFuture.getAndSet(null);
      WaypointRequest<Record<K>> computeRequest = new WaypointRequest<>(waypoint.getWaypointId(), record);
      waypoint.setRequestPending();
      future.complete(computeRequest);

      /*
       * Now await the computation result from the client and resume the pipeline.
       */
      Object element = InlineElementSource.this.getElement(computeRequest.getResponseFuture(), true);
      if (element == NonElement.RELEASED) {
        waypoint.resetRequestPending();
        throw new ElementDroppedException();
      } else {
        @SuppressWarnings("unchecked") Iterable<Cell<?>> cells = (Iterable<Cell<?>>)element;
        waypoint.setCells(cells);
        return true;
      }
    }

    @Override
    public String toString() {
      return "WaypointPredicate{waypointId=" + waypoint.getWaypointId() + '}';
    }
  }

  /**
   * Represents a waypoint state.
   */
  private static final class Waypoint {
    private final int waypointId;

    /**
     * Indicates if a {@link TryAdvanceFetchWaypointResponse}
     * has been returned to the client and this waypoint is awaiting fulfillment.
     */
    private boolean requestPending = false;

    /**
     * The cells obtained from a {@link TryAdvanceMutateMessage}.
     */
    private Iterable<Cell<?>> cells = null;

    Waypoint(int waypointId) {
      this.waypointId = waypointId;
    }

    int getWaypointId() {
      return waypointId;
    }

    void setRequestPending() {
      this.requestPending = true;
      this.cells = null;
    }

    void resetRequestPending() {
      this.requestPending = false;
    }

    Iterable<Cell<?>> getCells() {
      if (cells == null) {
        throw new IllegalStateException("getCells called when cells == null");
      }
      return cells;
    }

    void setCells(Iterable<Cell<?>> cells) {
      if (!requestPending) {
        throw new IllegalStateException("setCells called when request is not pending");
      }
      this.cells = cells;
      this.requestPending = false;
    }
  }

  /**
   * Internal exception indicating closure.
   */
  private static final class InlineElementSourceClosedException extends RuntimeException {
    private static final long serialVersionUID = 1493325057088016956L;

    InlineElementSourceClosedException() {
    }

    InlineElementSourceClosedException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Internal exception indicating an element presented to the client via waypoint was dropped
   * from the pipeline by the client.
   */
  private static final class ElementDroppedException extends RuntimeException {
    private static final long serialVersionUID = -5335678968534473874L;

    private ElementDroppedException() {
    }
  }

  /**
   * Internal exception indicating an exception thrown from the source pipeline.
   */
  private static final class InlineElementSourceException extends RuntimeException {
    private static final long serialVersionUID = 5915663887593187426L;

    InlineElementSourceException(Throwable cause) {
      super(cause);
    }
  }
}
