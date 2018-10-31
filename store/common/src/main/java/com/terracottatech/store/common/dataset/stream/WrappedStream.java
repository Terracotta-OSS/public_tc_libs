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

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Extends the definition of {@link BaseStream} to support delegation to a
 * wrapped {@code Stream}.
 *
 * @param <T> the stream element type
 * @param <S> the {@link BaseStream} subtype of the native {@code Stream} wrapped by this {@code WrappedStream}
 *
 * @author Clifford W. Johnson
 */
public interface WrappedStream<T, S extends BaseStream<T, S>> extends BaseStream<T, S> {

  /**
   * The default value for the {@link #selfClose(boolean) selfClose} operation.
   */
  boolean SELF_CLOSE_DEFAULT = true;

  /**
   * Gets the native {@link Stream} wrapped by this {@code WrappedStream}.
   *
   * @return the wrapped {@code Stream}
   */
  S getNativeStream();

  /**
   * Gets the stream pipeline meta data for this {@code WrappedStream}.
   *
   * @return the {@code PipelineMetaData} instance describing the stream pipeline
   *      containing this {@code WrappedStream}
   */
  PipelineMetaData getMetaData();

  /**
   * Gets the list of {@code PipelineOperation} instances representing the
   * chain of {@code Stream} operations for this stream pipeline.
   *
   * @return an unmodifiable list of {@code PipelineOperation} instances in the same
   *      sequence as the operations in this {@code Stream}
   */
  default List<PipelineOperation> getPipeline() {
    return this.getMetaData().getPipeline();
  }

  /**
   * Gets the head of the current {@code Consumer} chain specified for post-processing this
   * {@code WrappedStream} pipeline before the stream's terminal operation is started.
   *
   * @return the current post-processing {@code Consumer}
   */
  default Consumer<PipelineMetaData> getTerminalAction() {
    return this.getMetaData().getPipelineConsumer();
  }

  /**
   * Appends the {@code Consumer} provided to the existing chain of consumers used to post-process
   * this {@code WrappedStream} pipeline before the stream's terminal operation is started.
   * If there is no existing consumer, the {@code Consumer} provided is set as the
   * post-processing consumer.  The consumers are called in the order appended.
   *
   * @param pipelineConsumer the {@code Consumer} to which the {@link PipelineMetaData}
   *                         is presented for post-processing the pipeline containing this
   *                         {@code WrappedStream}; must not be {@code null}
   * @return the pipeline with the new consumer appended
   */
  default Consumer<PipelineMetaData> appendTerminalAction(final Consumer<PipelineMetaData> pipelineConsumer) {
    Objects.requireNonNull(pipelineConsumer, "pipelineConsumer");
    return this.getMetaData().appendPipelineConsumer(pipelineConsumer);
  }

  /**
   * The {@code WrappedStream} operation used to indicate whether or not this {@code Stream}
   * should close itself on completion of the terminal operation.  The default value for this
   * operation is <code>{@value #SELF_CLOSE_DEFAULT}</code>.
   *
   * <p>This is a <i>stateful, intermediate</i> operation.
   *
   * @param close {@code true} if this {@code Stream} is to close at termination; {@code false}
   *      if this {@code Stream} is not to self-close
   *
   * @return this {@code Stream}
   */
  S selfClose(boolean close);

  /**
   * Sets a finalizer guardian object in this stream in an attempt to close this stream
   * when this stream is eligible for finalization (no strong references remain).
   * <p>
   * The guardian set by this method is used to encourage closure of streams from which a
   * {@link BaseStream#iterator()} or {@link BaseStream#spliterator()}.
   */
  void setClosureGuard();
}
