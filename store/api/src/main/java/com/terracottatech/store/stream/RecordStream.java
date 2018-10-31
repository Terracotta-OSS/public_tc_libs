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
package com.terracottatech.store.stream;

import com.terracottatech.store.Record;
import com.terracottatech.store.internal.function.Functions;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 * A {@link Stream} of {@link Record} instances.
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
   * <p>
   * Implementations may not call the consumer until after the stream has closed.  Care must therefore be taken to avoid
   * deadlocking stream processing by blocking the stream on waiting for the consumer to be called.
   *
   * @param consumer {@code Consumer} that is passed an explanation of the stream execution plan
   * @return this {@code RecordStream<K>}
   */
  RecordStream<K> explain(Consumer<Object> consumer);

  /**
   * Returns an equivalent stream that uses the given batch size hint.
   * <p>
   * Stream executions will attempt (when possible) to optimize execution by transferring multiple elements over the
   * network at one time.  The size hint provided here will be used as a hint to the batch sizes to use when
   * transferring elements.
   *
   * @return this {@code RecordStream<K>}
   */
  RecordStream<K> batch(int sizeHint);

  /**
   * Returns an equivalent stream where any client side operations are performed inline with the server side.
   * <p>
   * Inline execution runs the client side portion of a pipeline synchronously during the server side processing.  This
   * means that until the client side portion of the stream pipeline has finished executing the server stream will not
   * advance to process the next element.
   * <p>
   * Unlike {@link #batch(int)} this operation is an edict and not a hint.  This means marking a stream as inline
   * overrides any previous or future batching instruction.
   *
   * @return this {@code RecordStream<K>}
   * @see #batch(int)
   */
  RecordStream<K> inline();

  @Override
  RecordStream<K> filter(Predicate<? super Record<K>> predicate);

  /**
   * {@inheritDoc}
   * <p>
   * <em>Record streams are distinct by definition since they represent the records found within a dataset.  Implementations
   * may therefore choose to elide distinct operations from the pipeline.</em>
   *
   * @return a distinct stream (possibly the same stream)
   */
  @Override
  RecordStream<K> distinct();

  /**
   * This method will throw a {@code java.lang.UnsupportedOperationException} since {@link Record records}
   * are not {@code Comparable}.
   *
   * @return a new {@code RecordStream<K>}
   * @see #sorted(Comparator)
   */
  @Override
  RecordStream<K> sorted();

  /**
   * Returns a {@code RecordStream<K>} consisting of the elements of this stream, sorted
   * according to the provided {@code Comparator}.
   *
   * <p>
   * Following are the ways to get a {@code RecordStream<K>} with {@link Record records}
   * sorted by the key or sorted by the value of a cell.
   * <pre>{@code
   * RecordStream<String> keySortedRecordStream = recordStream.sorted(Record.<K>keyFunction().asComparator());
   * RecordStream<String> cellSortedRecordStream = recordStream.sorted(NAME.valueOr("").asComparator());
   * }</pre>
   * <p>
   *
   * @param comparator used to compare {@code Record}s in the stream
   * @return a new {@code RecordStream<K>}
   */
  @Override
  RecordStream<K> sorted(Comparator<? super Record<K>> comparator);

  @Override
  RecordStream<K> peek(Consumer<? super Record<K>> action);

  @Override
  RecordStream<K> limit(long maxSize);

  @Override
  RecordStream<K> skip(long n);

  @Override
  RecordStream<K> sequential();

  @Override
  RecordStream<K> parallel();

  @Override
  RecordStream<K> unordered();

  @Override
  RecordStream<K> onClose(Runnable closeHandler);

  @SafeVarargs @SuppressWarnings("varargs")
  static <T> Consumer<T> log(String message, Function<? super T, ?> ... mappers) {
    return Functions.<T>log(message, asList(mappers));    // Java 10.0 needs explicit type declaration
  }
}
