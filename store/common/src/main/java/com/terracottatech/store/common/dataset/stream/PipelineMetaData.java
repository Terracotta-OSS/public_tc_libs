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

import com.terracottatech.store.util.Exceptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.BaseStream;

/**
 * Holds the meta data and options for a stream pipeline under construction.
 * An instance of this class is created for the first {@code Stream} in the pipeline
 * and passed along to each subsequent {@code Stream} appended to the chain.
 *
 * @author Clifford W. Johnson
 */
public final class PipelineMetaData {

  /**
   * A {@code Consumer} that does nothing.  This {@code Consumer} <i>drops out</i> when
   * {@link Consumer#andThen(Consumer) andThen} is used by simply returning the {@code after}
   * consumer.
   */
  private static final Consumer<PipelineMetaData> NOP_CONSUMER = new Consumer<PipelineMetaData>() {
    @Override
    public void accept(final PipelineMetaData l) {
      // Empty consumer
    }

    @SuppressWarnings("unchecked")
    @Override
    public Consumer<PipelineMetaData> andThen(final Consumer<? super PipelineMetaData> after) {
      return (Consumer<PipelineMetaData>)after;
    }
  };

  /**
   * The first {@code Stream} in the pipeline described by this {@code PipelineMetaData}.
   */
  private final BaseStream<?, ?> headStream;

  /**
   * The anchor from which closable objects associated with this stream are hung.  Pre-allocating this and
   * connecting it to {@link BaseStream#onClose(Runnable) headStream.onClose} permits proper closure of
   * late-allocated objects that need to be closed in conjunction with this stream.
   */
  private final AtomicReference<Runnable> closers = new AtomicReference<>(() -> {});

  /**
   * Indicates if this the pipeline described by this meta data should self-close
   * when the terminal operation is complete.
   */
  private boolean selfClose = WrappedStream.SELF_CLOSE_DEFAULT;

  /**
   * Set by the terminal operation iff {@link #selfClose} is {@code true}.  The
   * object set here has a {@code finalizer} intended to close the stream if not
   * explicitly closed.
   *
   * @see AbstractWrappedStream#setClosureGuard()
   */
  private Object closureGuard = null;

  /**
   * A list of descriptors of the {@code Stream} operations chained in the pipeline
   * described by this meta data.
   */
  private final List<PipelineOperation> operationChain = new ArrayList<>();

  /**
   * The {@code Consumer} to provide {@code Stream} post-processing before
   * the terminal operation is invoked.
   */
  private Consumer<PipelineMetaData> pipelineConsumer = NOP_CONSUMER;

  /**
   * Creates an instance of {@code PipelineMetaData} anchored at the {@code Stream}
   * provided.
   *
   * @param headStream the first {@code Stream} in the pipeline
   */
  public PipelineMetaData(final BaseStream<?, ?> headStream) {
    Objects.requireNonNull(headStream, "headStream");
    this.headStream = headStream;
    this.headStream.onClose(this::close);
  }

  /**
   * Gets the <i>head</i>, or first, {@code Stream} in the stream pipeline
   * represented by this {@code PipelineMetaData}.
   *
   * @return the head {@code Stream} of this pipeline
   */
  public final BaseStream<?, ?> getHeadStream() {
    return this.headStream;
  }

  /**
   * Gets the indication of whether or not the {@code Stream} described by this
   * {@code PipelineMetaData} should close itself after completion of the terminal operation.
   *
   * @return {@code true} if the {@code Stream} will attempt to close itself; {@code false}
   *    otherwise
   */
  public final boolean isSelfClosing() {
    return this.selfClose;
  }

  /**
   * Sets whether or not the {@code Stream} described by this {@code PipelineMetaData}
   * should close itself after completion of the terminal operation.
   *
   * @param selfClose if {@code true}, the {@code Stream} will attempt a close on completion
   *                  of the terminal operation; if {@code false}, the {@code Stream} will
   *                  not attempt a close
   */
  public final void setSelfClosing(final boolean selfClose) {
    this.selfClose = selfClose;
  }

  /**
   * Indicates whether or not a closure guard is set for the stream pipeline described by this {@code PipelineMetaData}.
   * @return {@code true} if a closure guard is set
   */
  final boolean hasClosureGuard() {
    return this.closureGuard != null;
  }

  /**
   * Records the closure guard for the stream pipeline described by this {@code PipelineMetadata}.
   *
   * @param closureGuard the closure guard object
   */
  final void setClosureGuard(Object closureGuard) {
    this.closureGuard = closureGuard;
  }

  /**
   * Appends a {@code Stream} operation to this pipeline descriptor.
   *
   * @param operation the {@code Stream} {@code Operation}; must not be {@code null}
   * @param operationMetaData an application-provided object to associate with the operation; may be {@code null}
   * @param arguments the {@code operation} arguments relevant to pipeline post-processing;
   *                  must not be {@code null} but may be empty
   *  @return this {@code PipelineMetaData} instance with the operation appended
   */
  public final PipelineMetaData append(
      final PipelineOperation.Operation operation,
      final PipelineOperation.OperationMetaData operationMetaData,
      final Object... arguments) {
    Objects.requireNonNull(operation, "operation");
    this.operationChain.add(operation.newInstance(operationMetaData, arguments));
    return this;
  }

  /**
   * Gets the list of operations in the pipeline described by this {@code PipelineMetaData} in
   * chaining order.
   *
   * @return an unmodifiable list of pipeline operations
   */
  public final List<PipelineOperation> getPipeline() {
    return Collections.unmodifiableList(this.operationChain);
  }

  /**
   * Gets the {@code Consumer} used for post-processing the stream pipeline
   * described by this {@code PipelineMetaData}.
   *
   * @return the {@code Consumer} to use for stream post-processing
   */
  public final Consumer<PipelineMetaData> getPipelineConsumer() {
    return this.pipelineConsumer;
  }

  /**
   * Appends the specified pipeline {@code Consumer} to the existing pipeline {@code Consumer}.
   *
   * @param pipelineConsumer the pipeline {@code Consumer} to append
   *
   * @return the head of the new pipeline {@code Consumer} chain
   */
  public final Consumer<PipelineMetaData> appendPipelineConsumer(final Consumer<PipelineMetaData> pipelineConsumer) {
    Objects.requireNonNull(pipelineConsumer, "pipelineConsumer");
    this.pipelineConsumer = this.pipelineConsumer.andThen(pipelineConsumer);
    return this.pipelineConsumer;
  }

  /**
   * Sets the post-processing {@code Consumer} for the stream pipeline described
   * by this {@code PipelineMetaData}.  Any previous value is replaced.
   *
   * @param pipelineConsumer the {@code Consumer} called to post-process this
   *                         {@code Stream} for this pipeline; if {@code null},
   *                         an internal no-op {@code Consumer} is used.
   */
  final void setPipelineConsumer(final Consumer<PipelineMetaData> pipelineConsumer) {
    this.pipelineConsumer = pipelineConsumer == null ? NOP_CONSUMER : pipelineConsumer;
  }

  /**
   * Adds a close action to the sequence of close actions to perform when the stream
   * associated with this {@code PipelineMetaData} is closed.  Close actions may be added
   * to this sequence even <i>after</i> the stream is terminated.
   * @param closeAction a {@code Runnable} to append to the close action sequence
   */
  void addCloser(Runnable closeAction) {
    closers.accumulateAndGet(closeAction, Exceptions::composeRunnables);
  }

  /**
   * Invokes the close handler sequence constructed through {@link #addCloser(Runnable)} calls.
   */
  private void close() {
    closers.getAndSet(() -> {}).run();
  }
}
