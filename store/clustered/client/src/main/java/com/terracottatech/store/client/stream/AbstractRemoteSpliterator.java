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
package com.terracottatech.store.client.stream;

import com.terracottatech.store.Record;
import com.terracottatech.store.StoreBusyException;
import com.terracottatech.store.StoreRetryableStreamTerminatedException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.StoreStreamNotFoundException;
import com.terracottatech.store.StoreStreamTerminatedException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.client.RecordImpl;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.exceptions.RemoteStreamClosedException;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenResponse;
import com.terracottatech.store.common.messages.stream.RemoteStreamType;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static com.terracottatech.store.client.stream.AbstractRemoteBaseStream.STREAM_USED_OR_CLOSED;
import static java.util.Objects.requireNonNull;

/**
 * The foundation for a {@link Spliterator} implementation backed by a remote stream.
 */
public abstract class AbstractRemoteSpliterator<T> implements RemoteSpliterator<T>, AutoCloseable {

  private final UUID streamId;
  private final Optional<BiConsumer<UUID, String>> serverPlanConsumer;
  private final BooleanSupplier closeUsingServer;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final BooleanSupplier remoteStreamDead;
  private final BooleanSupplier remoteStreamRetryable;

  protected AbstractRemoteSpliterator(UUID streamId, RootStreamDescriptor descriptor,
                                             BooleanSupplier closeUsingServer) {
    this.streamId = streamId;
    this.closeUsingServer = closeUsingServer;
    this.serverPlanConsumer = descriptor.getServerPlanConsumer();
    this.remoteStreamDead = descriptor::isDead;
    this.remoteStreamRetryable = descriptor::isRetryable;
  }

  /**
   * Gets the stream ID assigned to this {@code Spliterator}.
   * @return the stream ID assigned to this spliterator
   */
  @Override
  public UUID getStreamId() {
    return streamId;
  }

  @Override
  public Spliterator<T> trySplit() {
    // A RemoteSpliterator can not be split
    return null;
  }

  @Override
  public long estimateSize() {
    // The size of a RemoteSpliterator is unknown
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return Spliterator.CONCURRENT;
  }

  @Override
  public void close() {
    boolean closing = closed.compareAndSet(false, true);

    if (closing) {
      if (closeUsingServer.getAsBoolean()) {
        DatasetEntityResponse response = sendReceive("closing", new PipelineProcessorCloseMessage(streamId));
        if (response instanceof PipelineProcessorCloseResponse) {
          serverPlanConsumer.ifPresent(c -> c.accept(getStreamId(), ((PipelineProcessorCloseResponse) response).getExplanation().orElse("<unexplained>")));
        } else {
          throw new IllegalStateException("Error closing " + this.getClass().getSimpleName()
                  + " remote data source: unexpected response " + response);
        }
      }
    }
  }

  protected void checkIfClosed() throws IllegalStateException {
    if (closed.get()) {
      throw new IllegalStateException(STREAM_USED_OR_CLOSED);
    }
  }

  /**
   * Opens the remote end of a remote {@link Spliterator}.
   *
   * @param messageSender the {@code MessageSender} associated with the target dataset
   * @param streamId the identifier to assign to the newly-opened stream
   * @param elementType the expected type of elements from the new stream
   * @param rootStreamDescriptor a descriptor of the root {@link Stream} for which the {@code Spliterator}
   *                             is being opened
   *
   * @return the server response
   *
   * @throws StoreBusyException if the server is unable to open the remote stream due to resource constraints
   * @throws StoreRuntimeException if the server encounters an error while attempt to open the remote stream
   * @throws IllegalStateException if {@code streamId} is already registered in the server
   * @throws IllegalArgumentException if {@code portableOperations} contains an operation that is not a
   *          {@link PipelineOperation.IntermediateOperation}
   * @throws UnsupportedOperationException if the remote pipeline on the remote stream being opened is
   *          not supported by the server
   */
  public static PipelineProcessorOpenResponse openRemoteSpliterator(
          MessageSender messageSender,
          UUID streamId,
          ElementType elementType,
          RootStreamDescriptor rootStreamDescriptor) {

    requireNonNull(streamId, "streamId");
    /*
     * Ensure that all portableOperations are, additionally, IntermediateOperations -- the server-side
     * logic can't accept a TerminalOperation
     */
    List<PipelineOperation> portableOperations =
            requireNonNull(rootStreamDescriptor, "portableOperations").getPortableIntermediatePipelineSequence();
    for (PipelineOperation op : portableOperations) {
      if (!(op.getOperation() instanceof PipelineOperation.IntermediateOperation)) {
        throw new IllegalArgumentException("portableOperations limited to IntermediateOperations: " + op);
      }
    }

    PipelineOperation terminalOperation = rootStreamDescriptor.getPortableTerminalOperation();
    RemoteStreamType type = rootStreamDescriptor.getExecutionMode().orElse(null);
    PipelineProcessorOpenMessage openMessage = new PipelineProcessorOpenMessage(streamId, type, elementType, portableOperations, terminalOperation,
            rootStreamDescriptor.getServerPlanConsumer().isPresent());

    DatasetEntityResponse response = messageSender.sendMessageAwaitResponse(openMessage, SendConfiguration.ONE_SERVER);
    if (response instanceof PipelineProcessorOpenResponse) {
      return (PipelineProcessorOpenResponse) response;
    } else {
      throw new IllegalStateException("Error opening remote data source: unexpected response - " + response);
    }
  }

  protected RuntimeException processStreamFailure(String action, Throwable e) {
    if (e instanceof NoSuchElementException) {
      throw (NoSuchElementException) e;
    }

    if (e instanceof ClassCastException) {
      throw (ClassCastException) e;
    }

    if (e instanceof StoreStreamNotFoundException) {

      if (remoteStreamDead.getAsBoolean()) {

        if (remoteStreamRetryable.getAsBoolean()) {
          throw new StoreRetryableStreamTerminatedException("Stream execution was prematurely terminated due to an active server failover");
        }

        throw new StoreStreamTerminatedException("Stream execution was prematurely terminated due to an active server failover");
      }

      throw (StoreStreamNotFoundException) e;
    }

    if (e instanceof RemoteStreamClosedException) {
      throw new IllegalStateException(STREAM_USED_OR_CLOSED, e);
    }

    throw new StoreRuntimeException("Error " + action + " " + this.getClass().getSimpleName()
            + " remote data source: " + e, e);
  }

  /**
   * Decode an {@link Element} obtaining the contained value.  This method handles
   * only {@link ElementType#RECORD RECORD} and {@link ElementType#ELEMENT_VALUE ELEMENT_VALUE}
   * value types.
   * @param element the {@code Element} to decode
   * @return the reconstructed value from {@code element} cast (unchecked) to type {@code T}
   */
  @SuppressWarnings("unchecked")
  protected T decodeElement(Element element) {
    Object dataValue;
    switch (element.getType()) {
      case RECORD:
        dataValue = toRecord(element.getRecordData());
        break;
      case ELEMENT_VALUE:
        dataValue = element.getElementValue().getValue(this::toRecord);
        if (dataValue instanceof Tuple) {
          Tuple<?, ?> tuple = (Tuple) dataValue;
          if (tuple.getFirst() instanceof RecordData && tuple.getSecond() instanceof RecordData) {
            dataValue = Tuple.of(toRecord((RecordData<?>) tuple.getFirst()), toRecord((RecordData<?>) tuple.getSecond()));
          }
        }
        break;
      default:
        throw new IllegalStateException("Unexpected result type - " + element.getType());
    }
    return (T)dataValue;    // unchecked
  }

  private Record<?> toRecord(RecordData<?> data) {
    return new RecordImpl<>(data);
  }
}
