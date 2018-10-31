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
package com.terracottatech.sovereign;

import com.terracottatech.sovereign.plan.StreamPlan;
import com.terracottatech.store.Record;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Extend the stream layer with additional functionality that is only provided
 * by Sovereign.
 * <p>
 * {@code RecordStream} extends the operations available to {@link Stream}
 * by adding a {@link #selfClose(boolean)} operation.  JDK-provided {@code Stream}
 * instances, with some exceptions, do <b>not</b> close on termination -- even when
 * the stream is exhausted.  With the exception of the {@link #iterator()} and
 * {@link #spliterator()} methods, a {@code RecordStream} has the capability of
 * self-closing after completion of terminal operations.  Whether or not
 * the stream self-closes is determined by the last use of the {@code selfClose}
 * operation in the stream.
 *
 * If self-close is not enabled, streams based on {@code RecordStream} must be
 * explicitly closed to ensure proper resource disposal.  This may be done either by
 * calling {@link Stream#close()} directly or using try-with-resources.
 * The following is a recommended usage pattern:
 * <pre>{@code
 * SovereignDataset dataset = ...
 * List<Animals.Animal> mammals;
 * try (Stream<Record<String>> base = dataset.records()) {
 *   mammals = base
 *       .filter(compare(Animals.Schema.TAXONOMIC_CLASS).is("mammal"))
 *       .map(Animals.Animal::new)
 *       .collect(toList());
 * }
 * }</pre>
 * Closure of any {@code Stream} in the stream pipeline closes the full pipeline so the
 * following alternative also works:
 * <pre>{@code
 * SovereignDataset dataset = ...
 * List<Animals.Animal> mammals;
 * try (Stream<Animals.Animal> mapStream = dataset.records()
 *          .filter(compare(Animals.Schema.TAXONOMIC_CLASS).is("mammal"))
 *          .map(Animals.Animal::new)) {
 *   mammals = mapStream.collect(toList());
 * }
 * }</pre>
 *
 * @author Clifford W. Johnson
 * @author RKAV re-factored to add explain functionality and expose as interface
 */
public interface RecordStream<K extends Comparable<K>> extends Stream<Record<K>> {
  /**
   * Observes the stream pipeline and provides the pre-execution analysis information for this stream pipeline.
   * <p>
   * A typical sample usage is given below.
   * <pre>{@code
   * try (RecordStream<String> testStream = dataset.records()) {
   *   long count = testStream.explain(System.out::println).filter(...).count();
   * }
   * }</pre>
   *
   * @param consumer {@code Consumer} that is passed the {@code StreamPlan}
   * @return this {@code RecordStream<K>}
   */
  RecordStream<K> explain(Consumer<? super StreamPlan> consumer);

  /**
   * The {@code RecordStream} operation used to indicate whether or not this {@code Stream}
   * should close itself on completion of the terminal operation.
   * <p>
   * This is a <i>stateful, intermediate</i> operation.
   *
   * @param close {@code true} if this {@code Stream} is to close at termination; {@code false}
   *      if this {@code Stream} is not to self-close
   *
   * @return this {@code Stream}
   */
  Stream<Record<K>> selfClose(boolean close);
}
