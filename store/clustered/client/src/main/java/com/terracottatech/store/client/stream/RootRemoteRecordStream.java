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
import com.terracottatech.store.client.VoltronDatasetEntity;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.WrappedStream;
import com.terracottatech.store.common.messages.stream.ElementValue;
import com.terracottatech.store.common.messages.stream.RemoteStreamType;
import com.terracottatech.store.stream.RecordStream;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.BaseStream;

import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.DOUBLE_STREAM;
import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.INT_STREAM;
import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.LONG_STREAM;
import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.RECORD_STREAM;
import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.REFERENCE_STREAM;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * Provides a specialized implementation of {@code Stream<Record<K>>} supporting a Java stream pipeline over
 * a remote {@link Record} stream.  An instance of this class represents the <i>head</i> of a remote pipeline
 * sequence.  Portable operations chained from this instance are represented solely as metadata held in this
 * instance; non-portable operations result in new {@code Stream} instances chained from this <i>head</i>.
 *
 * <h3>Implementation Notes</h3>
 * The methods in this class assemble a sequence of <i>portable</i> operations "chained" from
 * an instance.  On reaching the first non-portable operation, the portable operation sequence is
 * presented to the {@code Supplier} allocated to provide the {@code Spliterator} for the pipeline
 * then control of pipeline assembly exits this class.  As a consequence, no more than one call to
 * {@link Consumer#accept(Object) remoteSpliteratorSupplier.accept} is made.
 * <p>
 * The designation of portable operations must be consistent with those handled by
 * {@code com.terracottatech.store.server.stream.PipelineProcessor}.
 *
 * @param <K> the {@code Record} key type
 */
public class RootRemoteRecordStream<K extends Comparable<K>>
    extends RemoteRecordStream<K>
    implements RemoteObject, RootStreamDescriptor {

  /**
   * The dataset, represented by its {@link VoltronDatasetEntity}, to which this stream is bound.
   */
  private final VoltronDatasetEntity<K> dataset;

  /**
   * Describes the sequence of <i>portable</i>, intermediate pipeline operations at the head of this
   * stream's pipeline.
   * <p>
   * This list is <i>observable</i> via {@link #getPortableIntermediatePipelineSequence()}.  It is
   * not <i>finalized</i> and consumed until a {@link Spliterator} is opened on this
   * stream via a call the {@link AbstractRemoteSpliteratorSupplier#get()} method (from
   * one of the {@link VoltronDatasetEntity#spliterator(RootStreamDescriptor) VoltronDatasetEntity.spliterator} methods).
   */
  private final List<PipelineOperation> portablePipelineSequence = new ArrayList<>();

  /**
   * The sequence's <i>portable</i> terminal pipeline operation, if any.
   */
  private PipelineOperation portableTerminalOperation;

  /*
   * Indicates if the current operation append point is the tail of a <i>portable</i>
   * pipeline sequence. Initially, a {@code RootRemoteRecordStream} pipeline is portable
   * and remains so until a non-portable operation is reached.
   */
  private boolean inPortablePipeline = true;

  /**
   * Indicates the ordered/unordered state of the portable portion of the pipeline based
   * on this stream.  Since Sovereign does not guarantee ordering from the {@code records()}
   * method, this stream is initially unordered.  This value is managed though the
   * {@link AbstractRemoteBaseStream#setStreamIsOrdered(boolean)} method.
   */
  private boolean streamIsOrdered = false;

  /**
   * Indicates the sequential/parallel state of the portable portion of the pipeline
   * based on this stream.  Sovereign streams are initially sequential.  The current
   * value is returned by the {@link BaseStream#isParallel()} method.  This value is
   * managed through the {@link AbstractRemoteBaseStream#setStreamIsParallel(boolean)} method.
   */
  private boolean streamIsParallel = false;

  /**
   * Indicates the batch size the client will request the server to use if a batching spliterator
   * is used to drive this stream.
   */
  private Integer batchSizeHint = null;

  /**
   * Indicates the execution mode that will be requested from the server.
   */
  private RemoteStreamType executionMode = null;

  /**
   * The {@link #onClose(Runnable)} handlers added to the stream chain based on this {@code RootRemoteRecordStream}.
   * This value is managed through the {@link AbstractRemoteBaseStream#onClose(Runnable)} method.
   */
  private AtomicReference<Runnable> onCloseHandler = new AtomicReference<>();

  /**
   * The {@link #explain(Consumer)}} consumers added to the stream chain based on this {@code RootRemoteRecordStream}.
   * This value is managed through the {@link RootRemoteRecordStream#explain(Consumer)} method.
   */
  private AtomicReference<Consumer<Object>> explainConsumer = new AtomicReference<>();

  /**
   * The server execution plans provided by either the remote spliterator or the terminated pipeline execution
   */
  private List<Map.Entry<UUID, String>> serverExecutionPlans = new ArrayList<>(1);


  /**
   * Indicates if the {@code Stream} based on this {@code RootRemoteRecordStream} is closed.  This value
   * is managed through the {@link AbstractRemoteBaseStream#close()} method.
   */
  private volatile boolean closed = false;

  /**
   * The head of the (wrapped) native stream chain providing the pipeline for the client-side
   * (non-portable) pipeline sequence.
   */
  private WrappedStream<?, ?> rootWrappedStream;

  private RemoteStreamKind remoteStreamKind;

  public enum RemoteStreamKind {
    RECORD_STREAM,
    REFERENCE_STREAM,
    INT_STREAM,
    LONG_STREAM,
    DOUBLE_STREAM
  }

  /**
   * The {@link Spliterator} providing elements to the pipeline headed by this {@code RootRemoteRecordStream}.
   * This field is set only after the pipeline is terminated and pipeline operations initiated.
   */
  private RemoteSpliterator<?> sourceSpliterator;

  private boolean isDead = false;

  private boolean retryable = true;

  public void setNonRetryable() {
    this.retryable = false;
  }

  /**
   * Constructs a new {@code RootRemoteRecordStream} as the head of a new stream/pipeline chain.
   * @param dataset the {@code DatasetEntity} through which access to the backing dataset is gained
   */
  public RootRemoteRecordStream(VoltronDatasetEntity<K> dataset) {
    super(null);        // Flag as rootStream
    this.dataset = dataset;
  }

  @Override
  protected final boolean isPipelinePortable() {
    return inPortablePipeline;
  }

  @Override
  protected final void setNonPortablePipeline() {
    this.inPortablePipeline = false;
  }

  @Override
  protected final boolean isStreamOrdered() {
    return streamIsOrdered;
  }

  @Override
  protected final void setStreamIsOrdered(boolean streamIsOrdered) {
    this.streamIsOrdered = streamIsOrdered;
  }

  @Override
  protected final boolean isStreamParallel() {
    return streamIsParallel;
  }

  @Override
  protected final void setStreamIsParallel(boolean streamIsParallel) {
    this.streamIsParallel = streamIsParallel;
  }

  @Override
  protected final void setBatchSizeHint(int batchSizeHint) {
    this.batchSizeHint = batchSizeHint;
  }

  @Override
  protected final void setExecutionMode(RemoteStreamType type) {
    this.executionMode = type;
  }

  /**
   * Indicates if this stream is closed.
   * @return {@code true} if this stream is closed; {@code false} otherwise
   */
  protected final boolean isClosed() {
    return closed;
  }

  @Override
  public boolean isDead() {
    return isDead;
  }

  @Override
  public boolean isRetryable() {
    return retryable;
  }

  public void failoverOccurred() {
    isDead = true;
  }

  /**
   * Sets a reference to the source {@link Spliterator} feeding elements to the pipeline headed by this
   * {@link RootRemoteRecordStream}.
   * @param sourceSpliterator the non-null {@code Spliterator} reference
   */
  void setSourceSpliterator(RemoteSpliterator<?> sourceSpliterator) {
    this.sourceSpliterator = requireNonNull(sourceSpliterator, "sourceSpliterator");
  }

  /**
   * Gets a reference to the source {@link Spliterator} feeding elements to the pipeline headed by this
   * {@code RootRemoteRecordStream}.
   * @return the source {@code Spliterator}
   */
  RemoteSpliterator<?> getSourceSpliterator() {
    return sourceSpliterator;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@inheritDoc}; if this stream is not yet opened, {@code null} is returned
   */
  @Override
  public UUID getStreamId() {
    return this.sourceSpliterator == null ? null : sourceSpliterator.getStreamId();
  }

  /**
   * Appends a portable {@link PipelineOperation} to the portable pipeline operation sequence.
   * @param portableOperation a non-{@code null} {@code PipelineOperation};
   *                          {@link PipelineOperation#getOperation() portableOperation.getOperation} must
   *                          return an {@link IntermediateOperation}
   * @throws IllegalArgumentException if {@code portableOperation.getOperation} is not an {@code IntermediateOperation}
   */
  void appendPortablePipelineOperation(PipelineOperation portableOperation) {
    if (!(portableOperation.getOperation() instanceof IntermediateOperation)) {
      throw new IllegalArgumentException("Must be an IntermediateOperation");
    }
    if (portableTerminalOperation != null) {
      throw new IllegalStateException("Pipeline sequence is terminated");
    }
    portablePipelineSequence.add(requireNonNull(portableOperation));
  }

  void setPortableTerminalOperation(PipelineOperation portableOperation) {
    if (!(portableOperation.getOperation() instanceof PipelineOperation.TerminalOperation)) {
      throw new IllegalArgumentException("Must be a TerminalOperation");
    }
    if (portableTerminalOperation != null) {
      throw new IllegalStateException("Pipeline sequence is terminated");
    }
    this.portableTerminalOperation = portableOperation;
  }

  /**
   * Evaluates a fully-portable (including the terminal operation) pipeline and returns the
   * result as an {@link ElementValue}.  This stream is closed once pipeline evaluation is
   * complete.
   *
   * @return an {@code ElementValue} containing the result
   *
   * @throws RuntimeException if an exception is thrown during pipeline evaluation
   */
  ElementValue executeTerminatedPipeline() {
    RuntimeException pipelineException = null;
    try {
      if (portableTerminalOperation == null) {
        throw new IllegalStateException("Pipeline sequence is not terminated");
      }
      return dataset.executeTerminatedStream(this);
    } catch (RuntimeException e) {
      pipelineException = e;
      throw e;
    } finally {
      try {
        close();
      } catch (Exception e) {
        if (pipelineException != null) {
          pipelineException.addSuppressed(e);
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  public List<PipelineOperation> getPortableIntermediatePipelineSequence() {
    return Collections.unmodifiableList(portablePipelineSequence);
  }

  @Override
  public PipelineOperation getPortableTerminalOperation() {
    return portableTerminalOperation;
  }

  @Override
  public List<PipelineOperation> getDownstreamPipelineSequence() {
    WrappedStream<?, ?> downstream = getRootWrappedStream();
    if (downstream == null) {
      return Collections.emptyList();
    } else {
      return downstream.getPipeline();
    }
  }

  @Override
  public OptionalInt getBatchSizeHint() {
    Integer hint = batchSizeHint;
    if (hint == null) {
      return OptionalInt.empty();
    } else {
      return OptionalInt.of(hint);
    }
  }

  @Override
  public Optional<RemoteStreamType> getExecutionMode() {
    return ofNullable(executionMode);
  }

  public VoltronDatasetEntity<K> getDataset() {
    return dataset;
  }

  /**
   * Gets the root native stream for this {@code RootRemoteRecordStream}.
   * @return the root stream supporting the client-side pipeline; may be {@code null} if not set via
   * `        {@link #setRootWrappedStream(WrappedStream, RemoteStreamKind)}
   */
  WrappedStream<?, ?> getRootWrappedStream() {
    return rootWrappedStream;
  }

  /**
   * Sets the head of the stream providing the anchor for the client-side pipeline for this stream to feed and
   * chains closure between the streams.
   * @param rootWrappedStream a {@link WrappedStream} instance at the head of the client-side pipeline
   */
  void setRootWrappedStream(WrappedStream<?, ?> rootWrappedStream, RemoteStreamKind remoteStreamKind) {
    if (this.rootWrappedStream != null) {
      throw new IllegalStateException("Root native stream already set - " + this.rootWrappedStream);
    }
    requireNonNull(rootWrappedStream);
    this.rootWrappedStream = rootWrappedStream;
    this.remoteStreamKind = remoteStreamKind;

    // TODO: Address onClose handler execution order
    onClose(rootWrappedStream::close);
    rootWrappedStream.onClose(this::close);
  }

  public Optional<BiConsumer<UUID, String>> getServerPlanConsumer() {
    return ofNullable(explainConsumer.get()).map(c -> (k, v) -> serverExecutionPlans.add(new SimpleImmutableEntry<>(k, v)));
  }

  private SingleStripeExplanation explain() {
    WrappedStream<?, ?> wrappedStream = getRootWrappedStream();
    List<PipelineOperation> client = wrappedStream == null ? Collections.emptyList() : wrappedStream.getPipeline();
    List<PipelineOperation> server = new ArrayList<>(getPortableIntermediatePipelineSequence());
    PipelineOperation serverTerminal = getPortableTerminalOperation();
    if (serverTerminal != null) {
      server.add(serverTerminal);
    }
    return new SingleStripeExplanation(client, server, serverExecutionPlans);
  }

  @Override
  public RootRemoteRecordStream<K> explain(Consumer<Object> consumer) {
    requireNonNull(consumer);
    checkClosed();
    /*
     * Explain consumers are accumulated in this root RootRemoteRecordStream.
     */
    explainConsumer.accumulateAndGet(consumer, (a, b) -> a == null ? b : a.andThen(b));
    return this;
  }

  @Override
  public RootRemoteRecordStream<K> batch(int sizeHint) {
    if (sizeHint <= 0) {
      throw new IllegalArgumentException("Batch size must be >= 1");
    }
    checkClosed();
    setBatchSizeHint(sizeHint);
    return this;
  }

  @Override
  public RootRemoteRecordStream<K> inline() {
    checkClosed();
    setExecutionMode(RemoteStreamType.INLINE);
    return this;
  }

  @Override
  public final void close() {
    closed = true;
    Runnable closeHandler = onCloseHandler.getAndSet(null);
    if (closeHandler != null) {
      try {
        closeHandler.run();
      } finally {
        ofNullable(explainConsumer.get()).ifPresent(c -> c.accept(explain()));
      }
    }
  }

  @Override
  public RecordStream<K> onClose(Runnable closeHandler) {
    checkClosed();        // Consistent with Java 9
    /*
     * OnClose handlers are accumulated in this root RootRemoteRecordStream.
     */
    onCloseHandler.accumulateAndGet(closeHandler, RootRemoteRecordStream::join);
    return this;
  }

  @SuppressWarnings("Duplicates")
  private static Runnable join(Runnable first, Runnable second) {
    if (first == null) {
      return second;
    } else if (second == null) {
      return first;
    }
    return () -> {
      try {
        first.run();
      } catch (Throwable t1) {
        try {
          second.run();
        } catch (Throwable t2) {
          try {
            // addSuppressed is not supported for some Throwables
            t1.addSuppressed(t2);
          } catch (Exception ignored) {
          }
        }
        throw t1;
      }
      second.run();
    };
  }

  protected BaseStream<?, ?> reconstructStream() {
    RootRemoteRecordStream<K> rootStream = getRootStream();
    WrappedStream<?, ?> wrappedStream;


    if (remoteStreamKind.equals(RECORD_STREAM)) {
      wrappedStream = RemoteRecordStream.recreateWrappedRecordStream(rootStream);
    } else if (remoteStreamKind.equals(REFERENCE_STREAM)) {
      wrappedStream = RemoteReferenceStream.recreateWrappedReferenceStream(rootStream);
    } else if (remoteStreamKind.equals(INT_STREAM)) {
      wrappedStream = RemoteIntStream.recreateWrappedIntStream(rootStream);
    } else if (remoteStreamKind.equals(LONG_STREAM)) {
      wrappedStream = RemoteLongStream.recreateWrappedLongStream(rootStream);
    } else if (remoteStreamKind.equals(DOUBLE_STREAM)) {
      wrappedStream = RemoteDoubleStream.recreateWrappedDoubleStream(rootStream);
    } else {
      throw new UnsupportedOperationException();
    }

    rootStream.onClose(wrappedStream::close);
    wrappedStream.onClose(rootStream::close);

    BaseStream<?, ?> stream = wrappedStream;

    for (PipelineOperation pipelineOperation : this.getRootWrappedStream().getPipeline()) {
      if (pipelineOperation.getOperation() instanceof IntermediateOperation) {
        IntermediateOperation operation = (IntermediateOperation) pipelineOperation.getOperation();
        stream = operation.reconstruct(stream, pipelineOperation.getArguments());
      }
    }

    return stream;
  }
}
