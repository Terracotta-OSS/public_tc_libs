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
package com.terracottatech.store.common.dataset.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Provides the base on which implementations of {@link WrappedStream} are built.  Subclasses
 * built on this class delegate {@code Stream} operations to a wrapped Java
 * {@link BaseStream BaseStream} implementation.
 *
 * @param <W> the type of this {@code AbstractWrappedStream}
 * @param <T> the type of the elements delivered by this {@code AbstractWrappedStream}
 * @param <S> the type of the native {@code Stream} wrapped by this {@code AbstractWrappedStream}
 *
 * @author Clifford W. Johnson
 */
abstract class AbstractWrappedStream<W extends AbstractWrappedStream<W, T, S>, T, S extends BaseStream<T, S>>
    implements WrappedStream<T, S> {

  /**
   * Enables {@link PipelineMetaData#closureGuard} allocation trace back when assertions are enabled.
   */
  private static final boolean CAPTURE_CLOSURE_GUARD_ALLOCATION_TRACE =
      AbstractWrappedStream.class.desiredAssertionStatus();

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWrappedStream.class);

  /**
   * The {@link java.util.stream.Stream Stream} wrapped by this {@code AbstractWrappedStream} implementation.
   */
  protected final S nativeStream;

  /**
   * The meta data describing and options applied to the pipeline of which this {@code Stream} is
   * an element.  An instance of {@code PipelineMetaData} is created for the first {@code Stream}
   * in the stream pipeline and must be passed along to all chained streams.
   *
   * @see #terminal(TerminalOperation, Object...)
   * @see #chain(IntermediateOperation, Supplier, Object...)
   */
  private PipelineMetaData metaData;

  /**
   * Local indication of invocation of the {@link #close()} method.  This local
   * status is used during construction to ensure addition of pipeline operations to a
   * closed stream are properly flagged.
   */
  private volatile boolean closed = false;

  /**
   * Internal constructor.
   *
   * @param ignored differentiates this constructor from
   *      {@link AbstractWrappedStream#AbstractWrappedStream(BaseStream, boolean) AbstractWrappedStream(BaseStream, boolean)}
   * @param nativeStream the Java {@code Stream} to wrap; must not be {@code null}
   */
  private AbstractWrappedStream(@SuppressWarnings("UnusedParameters") final boolean ignored, final S nativeStream) {
    requireNonNull(nativeStream, "nativeStream");
    if (nativeStream instanceof AbstractWrappedStream) {
      throw new IllegalArgumentException("nativeStream can not be subclass of AbstractWrappedStream");
    }

    this.nativeStream = nativeStream;
  }

  /**
   * Creates a {@code WrappedStream} holding a reference to the {@code Stream} provided.  This
   * constructor is used to create the {@code WrappedStream} instance representing the <i>head</i>
   * (or first) {@code Stream} in a stream pipeline.
   *
   * @param nativeStream the {@code Stream} to wrap as the head stream; must not be {@code null}
   *
   * @throws NullPointerException if {@code stream} is {@code null}
   * @throws IllegalArgumentException is {@code stream} is a subclass of {@code AbstractWrappedStream}
   */
  protected AbstractWrappedStream(final S nativeStream) {
    this(true, nativeStream);
    this.metaData = new PipelineMetaData(this.nativeStream);
  }

  /**
   * Creates a {@code WrappedStream} holding a reference to the {@code Stream} provided.  This
   * constructor is used to create a {@code WrappedStream} instance to be chained from a
   * previously instantiated {@code WrappedStream} instance.
   *
   * @param nativeStream the {@code Stream} to wrap as an intermediate part of a stream pipeline; must
   *                      not be {@code null}
   * @param isHead ignored
   *
   * @throws NullPointerException if {@code stream} is {@code null}
   * @throws IllegalArgumentException is {@code stream} is a subclass of {@code AbstractWrappedStream}
   */
  protected AbstractWrappedStream(final S nativeStream, final boolean isHead) {
    this(isHead, nativeStream);
  }

  /**
   * Wraps a {@link java.util.stream.Stream Stream} in a {@link WrappedStream} implementation.
   * Implementations of this method <b>must</b> return a <i>non-head</i> stream (instantiated
   * using {@link AbstractWrappedStream#AbstractWrappedStream(BaseStream, boolean)}.
   *
   * @param stream the {@code Stream} to wrap
   *
   * @return a new {@link WrappedStream} instance wrapping {@code stream}
   *
   * @throws NullPointerException if {@code stream} is {@code null}
   */
  protected abstract W wrap(S stream);

  protected <R> WrappedReferenceStream<R> wrapReferenceStream(final Stream<R> stream) {
    return new WrappedReferenceStream<>(stream, false);
  }

  protected WrappedIntStream wrapIntStream(final IntStream stream) {
    return new WrappedIntStream(stream, false);
  }

  protected WrappedLongStream wrapLongStream(final LongStream stream) {
    return new WrappedLongStream(stream, false);
  }

  protected WrappedDoubleStream wrapDoubleStream(final DoubleStream stream) {
    return new WrappedDoubleStream(stream, false);
  }

  /**
   * Chains this {@code WrappedStream} into the specified {@code WrappedStream}.
   *
   * @param operation identification of the {@code Stream} operation being appended
   * @param wrappingStreamSupplier the supplier of the {@code AbstractWrappedStream} to append to the stream
   *                      pipeline; the specified {@code Stream} must be {@code this} stream or must not represent
   *                      a <i>head</i> stream -- the specified stream must be instantiated using
   *                      {@link AbstractWrappedStream#AbstractWrappedStream(BaseStream, boolean)}
   * @param operationArguments the relevant operation arguments, if any; only those arguments
   *      of relevance to {@code WrappedStream} processing should be provided
   *
   * @param <S_ALT> the type of the {@code Stream} wrapped by {@code wrappingStream}
   * @param <T_ALT> the type of the elements delivered by {@code wrappingStream}
   * @param <W_ALT> the type of {@code wrappingStream}
   *
   * @return a reference to {@code wrappingStream} cast as {@code <S_ALT>}
   *
   * @throws NullPointerException if {@code operation} or {@code wrappingStream} is {@code null}
   * @throws IllegalArgumentException if {@code wrappingStream} is a <i>head</i> stream
   */
  @SuppressWarnings("unchecked")
  protected final <W_ALT extends AbstractWrappedStream<W_ALT, T_ALT, S_ALT>, T_ALT, S_ALT extends BaseStream<T_ALT, S_ALT>>
      S_ALT chain(final IntermediateOperation operation,
                  final Supplier<W_ALT> wrappingStreamSupplier,
                  final Object... operationArguments) {
    return chain(operation, wrappingStreamSupplier, null, operationArguments);
  }

  /**
   * Chains this {@code WrappedStream} into the specified {@code WrappedStream}.
   *
   * @param operation identification of the {@code Stream} operation being appended
   * @param wrappingStreamSupplier the supplier of the {@code AbstractWrappedStream} to append to the stream
   *                      pipeline; the specified {@code Stream} must be {@code this} stream or must not represent
   *                      a <i>head</i> stream -- the specified stream must be instantiated using
   *                      {@link AbstractWrappedStream#AbstractWrappedStream(BaseStream, boolean)}
   * @param operationMetaData an application-provided object to associate with the operation; may be {@code null}
   * @param operationArguments the relevant operation arguments, if any; only those arguments
   *      of relevance to {@code WrappedStream} processing should be provided
   *
   * @param <S_ALT> the type of the {@code Stream} wrapped by {@code wrappingStream}
   * @param <T_ALT> the type of the elements delivered by {@code wrappingStream}
   * @param <W_ALT> the type of {@code wrappingStream}
   *
   * @return a reference to {@code wrappingStream} cast as {@code <S_ALT>}
   *
   * @throws NullPointerException if {@code operation} or {@code wrappingStream} is {@code null}
   * @throws IllegalArgumentException if {@code wrappingStream} is a <i>head</i> stream
   */
  @SuppressWarnings("unchecked")
  protected final <W_ALT extends AbstractWrappedStream<W_ALT, T_ALT, S_ALT>, T_ALT, S_ALT extends BaseStream<T_ALT, S_ALT>>
      S_ALT chain(final IntermediateOperation operation,
                  final Supplier<W_ALT> wrappingStreamSupplier,
                  final PipelineOperation.OperationMetaData operationMetaData,
                  final Object... operationArguments) {
    requireNonNull(operation, "operation");
    requireNonNull(wrappingStreamSupplier, "wrappingStreamSupplier");

    W_ALT wrappingStream = wrappingStreamSupplier.get();
    if (wrappingStream != this) {
      @SuppressWarnings("UnnecessaryLocalVariable")
      final AbstractWrappedStream<W_ALT, T_ALT, S_ALT> abstractWrappedStream = wrappingStream;
      if (abstractWrappedStream.metaData != null) {
        throw new IllegalArgumentException("wrappingStream must be instantiated with (S, boolean) constructor");
      }
      abstractWrappedStream.metaData = this.metaData;
    }

    this.metaData.append(operation, operationMetaData, operationArguments);
    return (S_ALT) wrappingStream;     // unchecked
  }

  /**
   * Chains a terminal operation into this {@code WrappedStream} and invokes the post-processing
   * {@code Consumer} set by {@link WrappedStream#appendTerminalAction(Consumer) appendTerminalAction}.  The
   * {@code terminal} method must be called immediately prior to invocation of the stream's terminal operation.
   *
   * @param operation identification of the {@code Stream} operation being appended; must not be {@code null}
   * @param operationArguments the relevant operation arguments, if any; only those arguments
   *      of relevance to {@code WrappedStream} processing should be provided
   *
   * @throws NullPointerException if {@code operation} is {@code null}
   */
  protected final void terminal(final TerminalOperation operation, final Object... operationArguments) {
    terminal(operation, null, operationArguments);
  }

  /**
   * Chains a terminal operation into this {@code WrappedStream} and invokes the post-processing
   * {@code Consumer} set by {@link WrappedStream#appendTerminalAction(Consumer) appendTerminalAction}.  The
   * {@code terminal} method must be called immediately prior to invocation of the stream's terminal operation.
   *
   * @param operation identification of the {@code Stream} operation being appended; must not be {@code null}
   * @param operationMetaData an application-provided object to associate with the operation; may be {@code null}
   * @param operationArguments the relevant operation arguments, if any; only those arguments
   *      of relevance to {@code WrappedStream} processing should be provided
   *  @throws NullPointerException if {@code operation} is {@code null}
   */
  protected final void terminal(
      final TerminalOperation operation,
      final PipelineOperation.OperationMetaData operationMetaData,
      final Object... operationArguments) {
    requireNonNull(operation, "operation");

    final PipelineMetaData metaData = this.getMetaData();
    metaData.append(operation, operationMetaData, operationArguments);
    final Consumer<PipelineMetaData> pipelineConsumer = metaData.getPipelineConsumer();
    if (pipelineConsumer != null) {
      pipelineConsumer.accept(metaData);
    }
  }

  @Override
  public final S getNativeStream() {
    return this.nativeStream;
  }

  @Override
  public final PipelineMetaData getMetaData() {
    final PipelineMetaData metaData = this.metaData;
    if (metaData == null) {
      throw new IllegalStateException("metaData not set by chain");
    }
    return metaData;
  }

  @SuppressWarnings("unchecked")
  @Override
  public final S selfClose(final boolean close) {
    final PipelineMetaData metaData = this.getMetaData();
    metaData.setSelfClosing(close);
    metaData.append(IntermediateOperation.SELF_CLOSE, null);
    return (S) this;    // unchecked
  }

  /**
   * Indicates whether or not this stream pipeline is self-closing, i.e. closes upon
   * completion of the terminal operation.
   *
   * @return {@code true} if this {@code Stream} self closes; {@code false} otherwise
   */
  protected final boolean isSelfClosing() {
    return this.getMetaData().isSelfClosing();
  }

  @Override
  public void close() {
    this.closed = true;
    nativeStream.close();
  }

  @Override
  public boolean isParallel() {
    return nativeStream.isParallel();
  }

  @Override
  public S sequential() {
    return chain(IntermediateOperation.SEQUENTIAL, () -> wrapIfNotThis(nativeStream.sequential()));
  }

  @Override
  public S parallel() {
    return chain(IntermediateOperation.PARALLEL, () -> wrapIfNotThis(nativeStream.parallel()));
  }

  @Override
  public S unordered() {
    return chain(IntermediateOperation.UNORDERED, () -> wrapIfNotThis(nativeStream.unordered()));
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation delegates to the wrapped {@code Stream} without chaining an
   * entry in {@link PipelineMetaData} for this stream pipeline.
   *
   * <h3>Behavior Statement</h3>
   * Java 9 introduced a behavior change from Java 1.8 -- {@link BaseStream#onClose(Runnable)} under Java 1.8
   * did not check for stream termination/closure; under Java 9, stream termination/closure state is checked.
   * The Java 9 behavior is arguably more "proper" - {@code onClose} is documented as being an
   * <i>intermediate</i> operation most of which actually check for stream termination/closure.
   * Sovereign  is adopting the Java 9 closure behavior even in a Java 1.8 environment.
   *
   * @param closeHandler {@inheritDoc}
   * @return {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public S onClose(final Runnable closeHandler) {
    checkClosed();
    this.getMetaData().addCloser(requireNonNull(closeHandler, "closeHandler"));
    return (S)this;   // unchecked
  }

  /**
   * Associates an {@link AutoCloseable} object to be closed in conjunction with this {@code Stream}.
   * Unlike through {@link #onClose(Runnable)}, objects may be associated through this method until the
   * stream is actually closed.
   *
   * @param closeable an {@code AutoCloseable} instance to be closed when this stream is closed
   */
  public final void associateCloseable(AutoCloseable closeable) {
    checkClosed();
    requireNonNull(closeable, "closeable");
    this.getMetaData().addCloser(() -> {
      try {
        closeable.close();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void checkClosed() {
    if (closed) {
      // Same text used in Java streams (java.util.stream.AbstractPipeline.MSG_STREAM_LINKED); unfortunately not public
      throw new IllegalStateException("stream has already been operated upon or closed");
    }
  }

  @Override
  public final void setClosureGuard() {
    /*
     * A closure guard is unnecessary for an already closed stream
     */
    if (closed) {
      return;
    }

    if (!this.metaData.hasClosureGuard()) {

      final Throwable closureStackTrace;
      if (CAPTURE_CLOSURE_GUARD_ALLOCATION_TRACE) {
        closureStackTrace = new Throwable("ClosureGuard allocation");
      } else {
        closureStackTrace = null;
      }

      /*
       * Set closure guard for this AbstractWrappedStream instance and a close observer for the wrapped stream.
       */
      final CloseTracker closeTracker = new CloseTracker();
      this.metaData.addCloser(closeTracker::close);
      String className = this.getClass().getName() + "@" + Integer.toHexString(hashCode());
      this.metaData.setClosureGuard(
          new Object() {
            @SuppressWarnings("deprecation")
            @Override
            protected void finalize() throws Throwable {
              if (!closeTracker.closed) {
                LOGGER.error("{} instance was not explicitly closed; " +
                        "failure to close Stream instances obtained from a SovereignDataset may result in " +
                        "failures and/or unexpected behavior caused by resource exhaustion.",
                    className, closureStackTrace);
              }
              try {
                AbstractWrappedStream.this.close();
              } catch (Throwable e) {
                LOGGER.warn("Error closing {} instance: {}", className, e.toString(), e);
                // Ignored
              } finally {
                super.finalize();
              }
            }
          }
      );
    }
  }

  /**
   * Used to track closure of a {@code Stream} wrapped by an {@code AbstractWrappedStream}
   * instance for its {@link #setClosureGuard() closure guard}.
   *
   * @implNote This class must be static to avoid a referential tie from the wrapped
   *    stream and the {@code AbstractWrappedStream} instance.
   */
  private static class CloseTracker {
    volatile boolean closed = false;

    void close() {
      this.closed = true;
    }
  }

  /**
   * Wraps the {@code Stream} supplied unless it is the same stream as {@link #nativeStream}.
   *
   * @param stream the {@code Stream} to wrap
   *
   * @return {@code this} if {@code stream} is {@link #nativeStream}; otherwise a new {@link WrappedStream}
   *      holding {@code stream}
   */
  @SuppressWarnings("unchecked")
  private W wrapIfNotThis(final S stream) {
    if (stream != this.nativeStream) {
      return wrap(stream);
    } else {
      return (W) this;      // unchecked
    }
  }

  /**
   * Terminates this {@code Stream} using the supplied function with closing conditional on {@link #isSelfClosing()}.
   *
   * @param terminal stream termination function
   * @param <R> stream result type
   * @return the result of the terminated stream
   */
  protected <R> R selfClose(Function<S, R> terminal) {
    if (this.isSelfClosing()) {
      try (final S wrappedStream = this.nativeStream) {
        return terminal.apply(wrappedStream);
      }
    } else {
      return terminal.apply(nativeStream);
    }
  }
}
