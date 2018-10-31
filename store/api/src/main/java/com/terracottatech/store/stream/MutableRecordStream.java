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
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link Stream} of {@link Record} instances through which the {@code Record}s may be mutated.
 */
public interface MutableRecordStream<K extends Comparable<K>> extends RecordStream<K> {

  /**
   * Performs an update transformation against the {@link Record}s in the stream.
   * <p>
   * This is a <i>terminal</i> operation.
   * @param transform the transformation to perform
   */
  void mutate(UpdateOperation<? super K> transform);

  /**
   * Performs an update transformation against the {@link Record}s in the stream.
   * <p>
   * This is an <i>intermediate</i> operation.
   * @param transform the transformation to perform
   * @return a {@code Stream} of new {@code Tuple}s holding before and after {@code Record} instances
   */
  Stream<Tuple<Record<K>, Record<K>>> mutateThen(UpdateOperation<? super K> transform);

  /**
   * Deletes every {@code Record} in the stream.
   * <p>
   * This is a <i>terminal</i> operation.
   */
  void delete();

  /**
   * Deletes every {@link Record} in the stream.
   * <p>
   * This is an <i>intermediate</i> operation.
   * @return a {@code Stream} of the deleted {@code Record}s
   */
  Stream<Record<K>> deleteThen();

  @Override
  MutableRecordStream<K> filter(Predicate<? super Record<K>> predicate);

  @Override
  MutableRecordStream<K> distinct();

  /**
   * {@inheritDoc}
   * <p>
   * Sorting a {@code MutableRecordStream} returns a non-mutable {@link RecordStream}.
   * @return a new, non-mutable {@code RecordStream}
   */
  @Override
  RecordStream<K> sorted();

  /**
   * {@inheritDoc}
   * <p>
   * Sorting a {@code MutableRecordStream} returns a non-mutable {@link RecordStream}.
   * @return a new, non-mutable {@code RecordStream}
   */
  @Override
  RecordStream<K> sorted(Comparator<? super Record<K>> comparator);

  @Override
  MutableRecordStream<K> peek(Consumer<? super Record<K>> action);

  @Override
  MutableRecordStream<K> limit(long maxSize);

  @Override
  MutableRecordStream<K> skip(long n);

  @Override
  MutableRecordStream<K> sequential();

  @Override
  MutableRecordStream<K> parallel();

  @Override
  MutableRecordStream<K> unordered();

  @Override
  MutableRecordStream<K> onClose(Runnable closeHandler);

  @Override
  MutableRecordStream<K> explain(Consumer<Object> consumer);

  @Override
  MutableRecordStream<K> batch(int sizeHint);

  @Override
  MutableRecordStream<K> inline();
}
