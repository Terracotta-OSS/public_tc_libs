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

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.StoreBusyException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.common.dataset.stream.PipelineMetaData;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.stream.ElementValue;
import com.terracottatech.store.common.messages.stream.RemoteStreamType;
import com.terracottatech.store.intrinsics.Intrinsic;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;

import static java.util.Objects.requireNonNull;

/**
 * The base for TC Store proxies of (partially) portable {@link BaseStream} instances.
 * {@link RootRemoteRecordStream} is a special subclass of this class -- it is considered the <i>root</i>
 * (or head) stream of a stream/pipeline chain.
 *
 * @param <K> the key type of the dataset anchoring this stream
 * @param <T> the element type of this stream
 * @param <S> the class type of this stream
 */
public abstract class AbstractRemoteBaseStream<K extends Comparable<K>, T, S extends BaseStream<T, S>>
    implements BaseStream<T, S> {

  /**
   * Same text used in Java streams (java.util.stream.AbstractPipeline.MSG_STREAM_LINKED); unfortunately
   * not public.
   */
  public static final String STREAM_USED_OR_CLOSED = "stream has already been operated upon or closed";

  /**
   * The {@link RootRemoteRecordStream} obtained from {@link DatasetReader#records()}.  This is the
   * stream used to track the portable pipeline sequence and {@link #onClose(Runnable)} handlers.
   */
  private final RootRemoteRecordStream<K> rootStream;

  /**
   * Creates a new {@code AbstractRemoteBaseStream}.
   * @param rootStream the head of the stream/pipeline chain of which this instance is a part.
   *                   If {@code null}, {@code this} instance becomes the head of the stream/pipeline chain;
   *                   otherwise, identifies the root {@code RootRemoteRecordStream}.
   */
  @SuppressWarnings("unchecked")
  AbstractRemoteBaseStream(RootRemoteRecordStream<K> rootStream) {
    this.rootStream = (rootStream == null ? (RootRemoteRecordStream<K>)this : rootStream);
  }

  /**
   * Creates a new {@code AbstractRemoteBaseStream} using the root stream of the specified parent.
   * @param parentStream the {@code AbstractRemoteBaseStream} from which the root stream is obtained
   */
  AbstractRemoteBaseStream(AbstractRemoteBaseStream<K, ?, ? extends BaseStream<?, ?>> parentStream) {
    this(parentStream.rootStream);
  }

  protected final RootRemoteRecordStream<K> getRootStream() {
    return rootStream;
  }

  /**
   * Indicates if the current operation append point is the tail of a <i>portable</i>
   * pipeline sequence.
   * @return {@code true} if the operation append point is <i>portable</i>; {@code false} otherwise
   */
  protected boolean isPipelinePortable() {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    return rootStream.isPipelinePortable();
  }

  /**
   * Marks the current operation append point as non-portable.  Once marked non-portable, a pipeline cannot
   * become portable.
   *
   * @see #isPipelinePortable()
   */
  protected void setNonPortablePipeline() {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    rootStream.setNonPortablePipeline();
  }

  /**
   * Indicates whether or not the portable portion of the pipeline anchored on this {@code RootRemoteRecordStream}
   * is ordered or not.  Sovereign streams are initially <i>unordered</i>.
   * @return {@code true} if the portable portion of the pipeline is ordered (sorted); {@code false} otherwise
   */
  boolean isStreamOrdered() {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    return rootStream.isStreamOrdered();
  }

  /**
   * Sets whether or not the portable portion of the pipeline based on the root {@code RootRemoteRecordStream} is
   * ordered (sorted).
   * @param streamIsOrdered {@code true} if the pipeline is sorted
   */
  protected void setStreamIsOrdered(boolean streamIsOrdered) {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    rootStream.setStreamIsOrdered(streamIsOrdered);
  }

  /**
   * Indicates whether or not the portable portion of the pipeline anchored on this {@code RootRemoteRecordStream}
   * is sequential or parallel.
   * @return {@code true} if the stream is parallel; {@code false} if the stream is sequential
   */
  protected boolean isStreamParallel() {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    return rootStream.isStreamParallel();
  }

  /**
   * Indicates whether or not the portable portion of the pipeline anchored on the root {@code RootRemoteRecordStream}
   * is sequential or parallel.
   * @param streamIsParallel {@code true} if the stream is parallel; {@code false} if the stream is sequential
   */
  protected void setStreamIsParallel(boolean streamIsParallel) {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    rootStream.setStreamIsParallel(streamIsParallel);
  }

  /**
   * Sets the batch size hint for portable portion of the pipeline anchored on the root {@code RootRemoteRecordStream}.
   */
  protected void setBatchSizeHint(int batchSizeHint) {
    assert rootStream != this;
    rootStream.setBatchSizeHint(batchSizeHint);
  }

  /**
   * Sets the execution mode for the portable portion of the pipeline anchored on the root {@code RootRemoteRecordStream}
   */
  protected void setExecutionMode(RemoteStreamType type) {
    assert rootStream != this;
    rootStream.setExecutionMode(type);
  }

  /**
   * Adds a <i>portable</i> intermediate operation to the portable pipeline sequence.  If the
   * pipeline is currently <i>non-portable</i>, the operation is not checked and {@code false}
   * is returned.
   *
   * @param op the operation to test for portability
   * @param arguments the operation arguments
   * @return {@code true} if the operation is portable; {@code false} otherwise
   */
  protected final boolean tryAddPortable(PipelineOperation.IntermediateOperation op, Object... arguments) {
    if (!isPipelinePortable()) {
      return false;
    }
    if (isPortable(op, arguments)) {
      rootStream.appendPortablePipelineOperation(op.newInstance(arguments));
      return true;
    } else {
      setNonPortablePipeline();
      return false;
    }
  }

  /**
   * Adds a <i>portable</i> terminal operation to the portable pipeline sequence. This
   * method must not be called once the non-portable operation sequence is detected or when
   * the terminal operation has already been added.
   *
   * @param op the operation to test for portability
   * @param arguments the operation arguments
   * @return {@code true} if the operation is portable; {@code false} otherwise
   *
   * @throws IllegalStateException if the stream is closed or terminated
   */
  protected final boolean tryAddPortable(PipelineOperation.TerminalOperation op, Object... arguments) {
    if (!isPipelinePortable()) {
      return false;
    }
    if (isPortable(op, arguments)) {
      rootStream.setPortableTerminalOperation(op.newInstance(arguments));
      return true;
    } else {
      setNonPortablePipeline();
      return false;
    }
  }

  /**
   * Adds a <i>portable</i> intermediate operation to the portable pipeline sequence.  Unlike
   * {@link #tryAddPortable(PipelineOperation.IntermediateOperation, Object...)}, this method does
   * not fail if the pipeline is currently non-portable -- this method is used to append a mutative
   * operation sequence (which must always be performed on the server) to a non-portable pipeline.
   * @param op the portable intermediate operation
   * @param arguments the operation arguments
   * @throws IllegalStateException if the operation is non-portable
   */
  protected final void addPortable(PipelineOperation.IntermediateOperation op, Object... arguments) {
    if (isPortable(op, arguments)) {
      rootStream.appendPortablePipelineOperation(op.newInstance(arguments));
    } else {
      throw new IllegalStateException(op.newInstance(arguments) + " is not portable");
    }
  }

  /**
   * Adds a <i>portable</i> terminal operation to the portable pipeline sequence.  Unlike
   * {@link #tryAddPortable(PipelineOperation.TerminalOperation, Object...)}, this method does
   * not fail if the pipeline is currently non-portable -- this method is used to append a mutative
   * terminal operation (which must always be performed on the server) to a non-portable pipeline.
   * @param op the portable terminal operation
   * @param arguments the operation arguments
   * @throws IllegalStateException if the operation is non-portable
   */
  protected final void addPortable(PipelineOperation.TerminalOperation op, Object... arguments) {
    if (isPortable(op, arguments)) {
      rootStream.setPortableTerminalOperation(op.newInstance(arguments));
    } else {
      throw new IllegalStateException(op.newInstance(arguments) + " is not portable");
    }
  }

  /**
   * Indicates if the pipeline operation and arguments constitute a portable, <i>intrinsic</i> operation.
   * @param op the {@code Operation} to check
   * @param arguments the arguments to {@code op} to check
   * @return {@code true} if the operation and its arguments make up an intrinsic (portable) operation
   */
  protected boolean isPortable(PipelineOperation.Operation op, Object... arguments) {
    for (Object argument : arguments) {
      if (!(argument instanceof Intrinsic)) {
        return false;
      }
    }

    return op.supportsReconstruction();
  }

  /**
   * Returns the characteristics to apply to the {@link Spliterator} allocated for the head of the
   * non-portable (client-side) stream.  The element-specific stream may apply additional characteristics.
   * @return {@code Spliterator} characteristics determined from portable stream state
   */
  protected int getSpliteratorCharacteristics() {
    int characteristics = Spliterator.CONCURRENT;     // Remote streams can be concurrently modified
    if (isStreamOrdered()) {
      characteristics |= Spliterator.ORDERED;
    }
    return characteristics;
  }

  /**
   * Execute a terminated portable pipeline sequence.
   *
   * @return The result of the stream's execution.
   */
  protected final ElementValue executeTerminated() {
    return rootStream.executeTerminatedPipeline();
  }

  @Override
  public final boolean isParallel() {
    return rootStream.isStreamParallel();
  }

  @Override
  public abstract Iterator<T> iterator();

  @Override
  public abstract Spliterator<T> spliterator();

  @Override
  public abstract S sequential();

  @Override
  public abstract S parallel();

  @Override
  public abstract S unordered();

  @Override
  public void close() {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    rootStream.close();
  }

  /**
   * {@inheritDoc}
   *
   * <h3>Behavior Statement</h3>
   * Java 9 introduced a behavior change from Java 1.8 -- {@link BaseStream#onClose(Runnable)} under Java 1.8
   * did not check for stream termination/closure; under Java 9, stream termination/closure state is checked.
   * The Java 9 behavior is arguably more "proper" - {@code onClose} is documented as being an
   * <i>intermediate</i> operation most of which actually check for stream termination/closure.
   * TC Store is adopting the Java 9 closure behavior even in a Java 1.8 environment.
   *
   * @param closeHandler {@inheritDoc}
   * @return {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public S onClose(Runnable closeHandler) {
    assert rootStream != this;        // Method must be overridden in RootRemoteRecordStream
    // Close handlers are accumulated in the root stream of this chain
    rootStream.onClose(closeHandler);
    return (S)this;       // unchecked
  }

  /**
   * Throws an {@link IllegalStateException} if this stream is closed.
   */
  protected final void checkClosed() {
    if (rootStream.isClosed()) {
      throw new IllegalStateException(STREAM_USED_OR_CLOSED);
    }
  }

  /**
   * The base spliterator supplier for remote streams.  This class is specialized for each of the
   * specific spliterator types.
   *
   * @param <K> the key type of the source dataset
   * @param <T> the element type of the stream
   * @param <T_SPLIT> the type of {@code Spliterator} supplied
   */
  protected static class AbstractRemoteSpliteratorSupplier<K extends Comparable<K>, T, T_SPLIT extends RemoteSpliterator<T>>
      implements Consumer<PipelineMetaData>, Supplier<T_SPLIT> {

    private final Closer closer;
    private final RootRemoteRecordStream<K> rootStream;
    private final Function<RootStreamDescriptor, T_SPLIT> spliteratorGetter;

    private volatile boolean terminalOperationAppended = false;

    protected AbstractRemoteSpliteratorSupplier(
        RootRemoteRecordStream<K> rootStream,
        Function<RootStreamDescriptor, T_SPLIT> spliteratorGetter) {
      this.rootStream = requireNonNull(rootStream);
      this.closer = new Closer();
      this.rootStream.onClose(this.closer::close);
      this.spliteratorGetter = requireNonNull(spliteratorGetter);
    }

    protected RootRemoteRecordStream<K> getRootStream() {
      return rootStream;
    }

    /**
     * Flags the pipeline being fed from the {@code Spliterator} provided by this {@code Supplier} as complete.
     * This method is called once the terminal operation of the pipeline is encountered.
     *
     * @param metaData <i>ignored</i>
     */
    @Override
    public void accept(PipelineMetaData metaData) {
      if (terminalOperationAppended) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + ".accept called more than once");
      }
      terminalOperationAppended = true;
    }

    /**
     * Obtains a {@code Spliterator} from the {@link DatasetEntity} associated with this stream.
     * If the spliterator implements {@link AutoCloseable}, a close handler to close the spliterator is
     * added to the root {@code Stream} provided to the
     * {@link #AbstractRemoteSpliteratorSupplier(RootRemoteRecordStream, Function) constructor}.
     *
     * @return the {@code Spliterator} created from stream pipeline analysis
     *
     * @throws StoreBusyException if the server is unable to open the remote stream due to resource constraints
     * @throws StoreRuntimeException if the server encounters an error while attempt to open the remote stream
     */
    @Override
    public T_SPLIT get() {
      if (!terminalOperationAppended) {
        throw new IllegalStateException("terminal operation not appended");
      }

      /*
       * We're opening the remote stream; set a closure guard on this stream to ensure resources related to
       * this stream get de-allocated when the stream becomes unreferenced (without explicit closure).
       */
      rootStream.getRootWrappedStream().setClosureGuard();

      T_SPLIT spliterator;
      try {
        spliterator = spliteratorGetter.apply(this.rootStream);
      } catch (RuntimeException e) {
        // Failed to open the underlying Spliterator -- stream needs to close
        rootStream.close();
        throw e;
      }

      rootStream.setSourceSpliterator(spliterator);

      /*
       * If AutoCloseable, chain closure of the Spliterator to the root stream
       */
      if (spliterator instanceof AutoCloseable) {
        closer.autoCloseable = (AutoCloseable)spliterator;
      }

      return spliterator;
    }
  }

  private static final class Closer implements AutoCloseable {
    volatile AutoCloseable autoCloseable;

    @Override
    public void close() {
      if (autoCloseable != null) {
        try {
          autoCloseable.close();
        } catch (RuntimeException e) {
          // Just rethrow a RuntimeException
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
