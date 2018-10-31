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
package com.terracottatech.store.async;

import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Asynchronous equivalent to {@link MutableRecordStream}.
 */
public interface AsyncMutableRecordStream<K extends Comparable<K>> extends AsyncRecordStream<K> {

  /**
   * Performs an update transformation against the {@link Record}s in the stream.
   * <p>
   * This is a <i>terminal</i> operation.
   * @param transform the transformation to perform
   * @return the asynchronous mutation
   */
  Operation<Void> mutate(UpdateOperation<? super K> transform);

  /**
   * Performs an update transformation against the {@link Record}s in the stream.
   * <p>
   * This is an <i>intermediate</i> operation.
   * @param transform the transformation to perform
   * @return a {@code Stream} of new {@code Tuple}s holding before and after {@code Record} instances
   */
  AsyncStream<Tuple<Record<K>, Record<K>>> mutateThen(UpdateOperation<? super K> transform);

  /**
   * Deletes every {@code Record} in the stream.
   * <p>
   * This is a <i>terminal</i> operation.
   *
   * @return the asynchronous deletion
   */
  Operation<Void> delete();

  /**
   * Deletes every {@link Record} in the stream.
   * <p>
   * This is an <i>intermediate</i> operation.
   * @return a {@code Stream} of the deleted {@code Record}s
   */
  AsyncStream<Record<K>> deleteThen();

  @Override
  AsyncMutableRecordStream<K> explain(Consumer<Object> consumer);

  @Override
  AsyncMutableRecordStream<K> filter(Predicate<? super Record<K>> predicate);

  @Override
  AsyncMutableRecordStream<K> distinct();

  @Override
  AsyncRecordStream<K> sorted();

  @Override
  AsyncRecordStream<K> sorted(Comparator<? super Record<K>> comparator);

  @Override
  AsyncMutableRecordStream<K> peek(Consumer<? super Record<K>> action);

  @Override
  AsyncMutableRecordStream<K> limit(long maxSize);

  @Override
  AsyncMutableRecordStream<K> skip(long n);

  @Override
  AsyncMutableRecordStream<K> sequential();

  @Override
  AsyncMutableRecordStream<K> parallel();

  @Override
  AsyncMutableRecordStream<K> unordered();

  @Override
  AsyncMutableRecordStream<K> onClose(Runnable closeHandler);
}
