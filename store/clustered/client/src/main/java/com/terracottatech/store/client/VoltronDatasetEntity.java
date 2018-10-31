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
package com.terracottatech.store.client;

import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreBusyException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.indexing.ClusteredIndexing;
import com.terracottatech.store.client.message.EndpointMessageSender;
import com.terracottatech.store.client.message.ErrorResponseProcessor;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.ProcessingMessageSender;
import com.terracottatech.store.client.message.RetryMessageAssessorImpl;
import com.terracottatech.store.client.message.RetryMessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.client.reconnectable.ReconnectableDatasetEntity;
import com.terracottatech.store.client.reconnectable.ReconnectingMessageSender;
import com.terracottatech.store.client.stream.RemoteMutableRecordStream;
import com.terracottatech.store.client.stream.RemoteSpliterator;
import com.terracottatech.store.client.stream.RootRemoteRecordStream;
import com.terracottatech.store.client.stream.RootStreamDescriptor;
import com.terracottatech.store.client.stream.batched.BatchedRemoteRecordSpliterator;
import com.terracottatech.store.client.stream.batched.BatchedRemoteSpliteratorOfDouble;
import com.terracottatech.store.client.stream.batched.BatchedRemoteSpliteratorOfInt;
import com.terracottatech.store.client.stream.batched.BatchedRemoteSpliteratorOfLong;
import com.terracottatech.store.client.stream.batched.BatchedRemoteSpliteratorOfObj;
import com.terracottatech.store.client.stream.inline.InlineRemoteRecordSpliterator;
import com.terracottatech.store.client.stream.inline.InlineRemoteSpliteratorOfDouble;
import com.terracottatech.store.client.stream.inline.InlineRemoteSpliteratorOfInt;
import com.terracottatech.store.client.stream.inline.InlineRemoteSpliteratorOfLong;
import com.terracottatech.store.client.stream.inline.InlineRemoteSpliteratorOfObj;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.store.common.exceptions.ClientIdCollisionException;
import com.terracottatech.store.common.exceptions.RemoteStreamException;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.IdentifyClientMessage;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.common.messages.crud.AddRecordFullResponse;
import com.terracottatech.store.common.messages.crud.AddRecordMessage;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.GetRecordMessage;
import com.terracottatech.store.common.messages.crud.GetRecordResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.ElementValue;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenResponse;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineResponse;
import com.terracottatech.store.common.reconnect.ReconnectState;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EntityClientEndpoint;

import java.util.EnumSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.terracottatech.store.client.stream.AbstractRemoteSpliterator.openRemoteSpliterator;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DELETE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN_INTERNAL;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.DELETE;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MUTATE;
import static java.util.Optional.ofNullable;

public class VoltronDatasetEntity<K extends Comparable<K>> implements DatasetEntity<K> {
  private static final Logger LOGGER = LoggerFactory.getLogger(VoltronDatasetEntity.class);
  private static final EnumSet<TerminalOperation> TERMINALMUTATIVEOPS = EnumSet.of(MUTATE, DELETE);
  private static final EnumSet<IntermediateOperation> INTERMEDIATEMUTATIVEOPS = EnumSet.of(MUTATE_THEN_INTERNAL, DELETE_THEN, MUTATE_THEN);

  private final Type<K> keyType;
  private final EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint;

  private final MessageSender messageSender;
  private final ChangeListenerRegistry<K> changeListenerRegistry;
  private final AtomicBoolean closed;
  private final StreamIDGenerator streamIDGenerator;
  private final Set<RootRemoteRecordStream<K>> openStreams;
  private final Indexing indexing;

  /**
   * A client identifier stable across failover.
   */
  private final UUID stableClientId;

  private VoltronDatasetEntity(Type<K> keyType, EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint, Parameters datasetEntityParameters) {
    this.keyType = keyType;
    this.endpoint = endpoint;

    this.closed = new AtomicBoolean();
    this.streamIDGenerator = new StreamIDGenerator();
    this.openStreams = ConcurrentHashMap.newKeySet();
    this.messageSender = createMessageSender(endpoint, datasetEntityParameters);
    this.stableClientId = identifyClient();
    this.changeListenerRegistry = new ChangeListenerRegistry<>(messageSender);
    installDelegate();
    this.indexing = new ClusteredIndexing(messageSender, closed::get, stableClientId);
  }

  public static <K extends Comparable<K>> VoltronDatasetEntity<K> newInstance(
      Type<K> keyType,
      EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint,
      Parameters datasetEntityParameters) {
    return new VoltronDatasetEntity<>(keyType, endpoint, datasetEntityParameters);
  }

  /**
   * Negotiate with the server to establish a stable client ID for the new {@code VoltronDatasetEntity}.
   * @return the stable client identifier
   */
  private UUID identifyClient() {
    while (true) {
      UUID clientId = UUID.randomUUID();
      try {
        messageSender.sendMessageAwaitResponse(new IdentifyClientMessage(clientId), SendConfiguration.ONE_SERVER);
        return clientId;
      } catch (ClientIdCollisionException e) {
        // Retry after generating a new UUID
      }
    }
  }

  private MessageSender createMessageSender(EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint, Parameters datasetEntityParameters) {
    MessageSender endpointMessageSender = new EndpointMessageSender(endpoint);
    MessageSender errorMessageSender = new ProcessingMessageSender(endpointMessageSender, new ErrorResponseProcessor());
    MessageSender messageSender = new RetryMessageSender(errorMessageSender, new RetryMessageAssessorImpl());

    if (datasetEntityParameters instanceof ReconnectableDatasetEntity.Parameters) {
      messageSender = new ReconnectingMessageSender(messageSender, ((ReconnectableDatasetEntity.Parameters)datasetEntityParameters).reconnectController());
    }

    return messageSender;
  }

  private void installDelegate() {
    ChangeEventManager<K> changeEventManager = new ChangeEventManager<>(keyType, changeListenerRegistry);

    DatasetEndpointDelegate<K> endpointDelegate = new DatasetEndpointDelegate<>(
            changeEventManager,
            this::createReconnectState,
            openStreams,
            this::dispose
    );

    this.endpoint.setDelegate(endpointDelegate);
  }

  private ReconnectState createReconnectState() {
    boolean sendChangeEvents = changeListenerRegistry.sendChangeEvents();
    return new ReconnectState(sendChangeEvents, stableClientId);
  }

  @Override
  public Type<K> getKeyType() {
    return keyType;
  }

  private void dispose() {
    changeListenerRegistry.close();
  }

  @Override
  public void close() {
    boolean closing = closed.compareAndSet(false, true);

    if (closing) {
      dispose();
      try {
        endpoint.close();
      } catch (Exception e) {
        LOGGER.debug("Endpoint.close failed; Dataset close continuing - {}", e, e);
      }

      int streamCount = 0;
      for (RootRemoteRecordStream<K> openStream : openStreams) {
        streamCount++;
        try {
          openStream.close();
        } catch (Exception e) {
          // ignored
        }
      }
      if (streamCount > 0) {
        LOGGER.warn("Closed {} streams at Dataset close; for best resource management, each stream should be explicitly closed", streamCount);
      }
    }
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    changeListenerRegistry.registerChangeListener(listener);
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    changeListenerRegistry.deregisterChangeListener(listener);
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    checkIfClosed();

    AddRecordMessage<K> message = new AddRecordMessage<>(stableClientId, key, cells, false);
    AddRecordSimplifiedResponse response = messageSender.sendMessageAwaitResponse(message, SendConfiguration.FULL);

    return response.isAdded();
  }

  @Override
  public Record<K> addReturnRecord(K key, Iterable<Cell<?>> cells) {
    checkIfClosed();

    AddRecordMessage<K> message = new AddRecordMessage<>(stableClientId, key, cells, true);
    AddRecordFullResponse<K> response = messageSender.sendMessageAwaitResponse(message, SendConfiguration.FULL);

    RecordData<K> existing = response.getExisting();
    if (existing == null) {
      return null;
    } else {
      return new RecordImpl<>(existing);
    }
  }

  @Override
  public RecordImpl<K> get(K key) {
    return get(key, AlwaysTrue.alwaysTrue());
  }

  @Override
  public RecordImpl<K> get(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    checkIfClosed();

    DatasetEntityMessage message = new GetRecordMessage<>(key, predicate);
    GetRecordResponse<K> response = messageSender.sendMessageAwaitResponse(message, SendConfiguration.ONE_SERVER);

    RecordData<K> data = response.getData();
    if (data == null) {
      return null;
    } else {
      return new RecordImpl<>(data);
    }
  }

  @Override
  public boolean update(K key, IntrinsicPredicate<? super Record<K>> predicate,
                        IntrinsicUpdateOperation<? super K> transform) {
    checkIfClosed();

    PredicatedUpdateRecordMessage<K> message =
        new PredicatedUpdateRecordMessage<>(stableClientId, key, predicate, transform, false);   // Java 10 needs type inference help
    PredicatedUpdateRecordSimplifiedResponse response = messageSender.sendMessageAwaitResponse(message, SendConfiguration.FULL);

    return response.isUpdated();
  }

  @Override
  public Tuple<Record<K>, Record<K>> updateReturnTuple(K key, IntrinsicPredicate<? super Record<K>> predicate,
                                                       IntrinsicUpdateOperation<? super K> transform) {
    checkIfClosed();

    PredicatedUpdateRecordMessage<K> message =
            new PredicatedUpdateRecordMessage<>(stableClientId, key, predicate, transform, true);    // Java 10 needs type inference help
    PredicatedUpdateRecordFullResponse<K> response = messageSender.sendMessageAwaitResponse(message, SendConfiguration.FULL);

    RecordData<K> before = response.getBefore();
    RecordData<K> after = response.getAfter();
    if (before == null) {
      return null;
    } else {
      return Tuple.of(new RecordImpl<>(before), new RecordImpl<>(after));
    }
  }

  @Override
  public boolean delete(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    checkIfClosed();

    PredicatedDeleteRecordMessage<K> message = new PredicatedDeleteRecordMessage<>(stableClientId, key, predicate,
        false);
    PredicatedDeleteRecordSimplifiedResponse response = messageSender.sendMessageAwaitResponse(message, SendConfiguration.FULL);

    return response.isDeleted();
  }

  @Override
  public Record<K> deleteReturnRecord(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    checkIfClosed();

    PredicatedDeleteRecordMessage<K> message = new PredicatedDeleteRecordMessage<>(stableClientId, key, predicate,
        true);
    PredicatedDeleteRecordFullResponse<K> response = messageSender.sendMessageAwaitResponse(message, SendConfiguration.FULL);

    RecordData<K> deleted = response.getDeleted();
    if (deleted == null) {
      return null;
    } else {
      return new RecordImpl<>(deleted);
    }
  }

  @Override
  public Indexing getIndexing() {
    return indexing;
  }

  /**
   * Opens a record {@link Spliterator} over the {@link ClusteredDataset} backed by this entity.
   *
   * @param rootStreamDescriptor a descriptor of the root {@link Stream} for which the {@code Spliterator}
   *                             is being opened
   *
   * @return a new {@code Spliterator} providing elements of type {@code Record<K>}
   *
   * @throws StoreBusyException if the server is unable to open the remote stream due to resource constraints
   * @throws StoreRuntimeException if the server encounters an error while attempt to open the remote stream
   * @throws IllegalArgumentException if {@code portableOperations} contains an operation that is not a
   *          {@link PipelineOperation.IntermediateOperation IntermediateOperation}
   */
  public RemoteSpliterator<Record<K>> spliterator(RootStreamDescriptor rootStreamDescriptor) {
    checkIfClosed();
    PipelineProcessorOpenResponse response = openRemoteSpliterator(messageSender, streamIDGenerator.next(), ElementType.RECORD, rootStreamDescriptor);
    switch (response.getSourceType()) {
      case INLINE:
        return new InlineRemoteRecordSpliterator<>(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      case BATCHED:
        return new BatchedRemoteRecordSpliterator<>(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      default:
        throw new RemoteStreamException("Unknown remote stream type " + response.getSourceType());
    }
  }

  /**
   * Opens a non-{@code Record} {@link Spliterator} over the {@link ClusteredDataset} backed by this entity.
   *
   * @param <T> the {@code Spliterator} element type
   * @param rootStreamDescriptor a descriptor of the root {@link Stream} for which the {@code Spliterator}
   *                             is being opened
   *
   * @return a new {@code Spliterator} providing elements of type {@code <T>}
   *
   * @throws StoreBusyException if the server is unable to open the remote stream due to resource constraints
   * @throws StoreRuntimeException if the server encounters an error while attempt to open the remote stream
   * @throws IllegalArgumentException if {@code portableOperations} contains an operation that is not a
   *          {@link PipelineOperation.IntermediateOperation IntermediateOperation}
   */
  public <T> RemoteSpliterator<T> objSpliterator(RootStreamDescriptor rootStreamDescriptor) {
    checkIfClosed();
    PipelineProcessorOpenResponse response = openRemoteSpliterator(messageSender, streamIDGenerator.next(), ElementType.ELEMENT_VALUE, rootStreamDescriptor);
    switch (response.getSourceType()) {
      case INLINE:
        return new InlineRemoteSpliteratorOfObj<>(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      case BATCHED:
        return new BatchedRemoteSpliteratorOfObj<>(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      default:
        throw new RemoteStreamException("Unknown remote stream type " + response.getSourceType());
    }
  }

  /**
   * Opens a {@link Spliterator.OfDouble} over the {@link ClusteredDataset} backed by this entity.
   *
   * @param rootStreamDescriptor a descriptor of the root {@link Stream} for which the {@code Spliterator.OfDouble}
   *                             is being opened
   *
   * @return a new {@code Spliterator.OfDouble}
   *
   * @throws StoreBusyException if the server is unable to open the remote stream due to resource constraints
   * @throws StoreRuntimeException if the server encounters an error while attempt to open the remote stream
   * @throws IllegalArgumentException if {@code portableOperations} contains an operation that is not a
   *          {@link PipelineOperation.IntermediateOperation IntermediateOperation}
   */
  public RemoteSpliterator.OfDouble doubleSpliterator(RootStreamDescriptor rootStreamDescriptor) {
    checkIfClosed();
    PipelineProcessorOpenResponse response = openRemoteSpliterator(messageSender, streamIDGenerator.next(), ElementType.DOUBLE, rootStreamDescriptor);
    switch (response.getSourceType()) {
      case INLINE:
        return new InlineRemoteSpliteratorOfDouble<>(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      case BATCHED:
        return new BatchedRemoteSpliteratorOfDouble(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      default:
        throw new RemoteStreamException("Unknown remote stream type " + response.getSourceType());
    }
  }

  /**
   * Opens a {@link Spliterator.OfInt} over the {@link ClusteredDataset} backed by this entity.
   *
   * @param rootStreamDescriptor a descriptor of the root {@link Stream} for which the {@code Spliterator.OfInt}
   *                             is being opened
   *
   * @return a new {@code Spliterator.OfInt}
   *
   * @throws StoreBusyException if the server is unable to open the remote stream due to resource constraints
   * @throws StoreRuntimeException if the server encounters an error while attempt to open the remote stream
   * @throws IllegalArgumentException if {@code portableOperations} contains an operation that is not a
   *          {@link PipelineOperation.IntermediateOperation IntermediateOperation}
   */
  public RemoteSpliterator.OfInt intSpliterator(RootStreamDescriptor rootStreamDescriptor) {
    checkIfClosed();
    PipelineProcessorOpenResponse response = openRemoteSpliterator(messageSender, streamIDGenerator.next(), ElementType.INT, rootStreamDescriptor);
    switch (response.getSourceType()) {
      case INLINE:
        return new InlineRemoteSpliteratorOfInt<>(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      case BATCHED:
        return new BatchedRemoteSpliteratorOfInt(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      default:
        throw new RemoteStreamException("Unknown remote stream type " + response.getSourceType());
    }
  }

  /**
   * Opens a {@link Spliterator.OfLong} over the {@link ClusteredDataset} backed by this entity.
   *
   * @param rootStreamDescriptor a descriptor of the root {@link Stream} for which the {@code Spliterator.OfLong}
   *                             is being opened
   *
   * @return a new {@code Spliterator.OfLong}
   *
   * @throws StoreBusyException if the server is unable to open the remote stream due to resource constraints
   * @throws StoreRuntimeException if the server encounters an error while attempt to open the remote stream
   * @throws IllegalArgumentException if {@code portableOperations} contains an operation that is not a
   *          {@link PipelineOperation.IntermediateOperation IntermediateOperation}
   */
  public RemoteSpliterator.OfLong longSpliterator(RootStreamDescriptor rootStreamDescriptor) {
    checkIfClosed();
    PipelineProcessorOpenResponse response = openRemoteSpliterator(messageSender, streamIDGenerator.next(), ElementType.LONG, rootStreamDescriptor);
    switch (response.getSourceType()) {
      case INLINE:
        return new InlineRemoteSpliteratorOfLong<>(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      case BATCHED:
        return new BatchedRemoteSpliteratorOfLong(messageSender, response.getStreamId(), rootStreamDescriptor, () -> !closed.get());
      default:
        throw new RemoteStreamException("Unknown remote stream type " + response.getSourceType());
    }
  }

  /**
   * Execute a fully portable, terminated pipeline over a stream.
   *
   * @param rootStreamDescriptor a descriptor of the root {@link Stream} for which the pipeline is being run
   * @return the stream's execution result
   */
  public ElementValue executeTerminatedStream(RootStreamDescriptor rootStreamDescriptor) {
    checkIfClosed();
    UUID streamId = streamIDGenerator.next();
    boolean isMutative = scanForMutativeOperationInPipeline(rootStreamDescriptor);
    ExecuteTerminatedPipelineMessage executeTerminatedStreamMessage =
            new ExecuteTerminatedPipelineMessage(streamId,
                    rootStreamDescriptor.getPortableIntermediatePipelineSequence(),
                    rootStreamDescriptor.getPortableTerminalOperation(),
                    rootStreamDescriptor.getServerPlanConsumer().isPresent(), isMutative);
    ExecuteTerminatedPipelineResponse response = null;
    try {
      response = messageSender.sendMessageAwaitResponse(executeTerminatedStreamMessage, SendConfiguration.ONE_SERVER);
    } finally {
      String plan = ofNullable(response).flatMap(ExecuteTerminatedPipelineResponse::getExplanation).orElse("unexplained");
      rootStreamDescriptor.getServerPlanConsumer().ifPresent(c -> c.accept(streamId, plan));
    }
    return response.getElementValue();
  }

  @Override
  public RecordStream<K> nonMutableStream() {
    return registerStream(new RootRemoteRecordStream<>(this));
  }

  @Override
  public MutableRecordStream<K> mutableStream() {
    return registerStream(new RemoteMutableRecordStream<>(this));
  }

  private <S extends RootRemoteRecordStream<K>> S registerStream(S stream) {
    stream.onClose(() -> openStreams.remove(stream));
    try {
      checkIfClosed();
      openStreams.add(stream);
      checkIfClosed();
    } catch (StoreRuntimeException e) {
      stream.close();
      throw e;
    }
    return stream;
  }

  private static boolean scanForMutativeOperationInPipeline(RootStreamDescriptor rootStreamDescriptor) {
    PipelineOperation.Operation terminalOp = rootStreamDescriptor.getPortableTerminalOperation().getOperation();
    if (TERMINALMUTATIVEOPS.contains(terminalOp)) {
      return true;
    }
    return rootStreamDescriptor.getPortableIntermediatePipelineSequence().stream().map(PipelineOperation::getOperation)
            .anyMatch(INTERMEDIATEMUTATIVEOPS::contains);
  }

  private void checkIfClosed() throws StoreRuntimeException {
    if (closed.get()) {
      throw new StoreRuntimeException("Attempt to use Dataset after close()");
    }
  }

  /**
   * Generates sequential stream identifiers from a base pseudo-random UUID.
   * Since remote streams are managed <i>per-client, per-dataset</i>, it is sufficient that the
   * stream identifier generated be unique within the allocating {@code VoltronDatasetEntity}.  However,
   * since the concurrency strategy for remote stream messages is based solely on the stream
   * identifier, it is helpful for the identifier to have some uniqueness across clients and
   * datasets to avoid unnecessary concurrency collisions.
   */
  // Package-private for testing purposes
  static final class StreamIDGenerator {
    private final AtomicLong sequence = new AtomicLong();
    private final int version;
    private final long lsb;

    StreamIDGenerator() {
      UUID seed = UUID.randomUUID();
      this.version = seed.version();
      this.lsb = seed.getLeastSignificantBits();
      long msb = seed.getMostSignificantBits();
      long sequence = (msb & 0x0FFF) << 48;
      sequence |= (msb & 0xFFFF0000L) << 16;
      sequence |= msb >>> 32;
      this.sequence.set(sequence);
    }

    UUID next() {
      long sequence = this.sequence.getAndIncrement();
      long msb = sequence << 32;
      msb |= (sequence >>> 16) & 0xFFFF0000L;
      msb |= (sequence >>> 48) & 0x0FFF;
      msb |= (this.version << 12) & 0xF000;
      return new UUID(msb, this.lsb);
    }
  }
}
