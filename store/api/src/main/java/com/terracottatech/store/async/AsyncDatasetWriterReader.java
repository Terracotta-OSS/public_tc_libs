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

import com.terracottatech.store.Cell;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.UpdateOperation;

import static java.util.Arrays.asList;

/**
 * An asynchronous equivalent to {@link DatasetWriterReader}.
 */
public interface AsyncDatasetWriterReader<K extends Comparable<K>> extends AsyncDatasetReader<K> {
  /**
   * An asynchronous equivalent to {@link DatasetWriterReader#add(Comparable, Cell...)}.
   *
   * @param key key for the record
   * @param cells cells which form the record
   * @return an operation representing the add execution
   */
  default Operation<Boolean> add(K key, Cell<?> ... cells) {
    return add(key, asList(cells));
  }

  /**
   * An asynchronous equivalent to {@link DatasetWriterReader#add(Comparable, Iterable)}.
   *
   * @param key key for the record
   * @param cells a non-{@code null} {@code Iterable} supplying cells which form the record
   * @return an operation representing the add execution
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  Operation<Boolean> add(K key, Iterable<Cell<?>> cells);

  /**
   * An asynchronous equivalent to {@link DatasetWriterReader#update(Comparable, UpdateOperation)}.
   *
   * @param key key of the record to mutate
   * @param transform the mutating transformation to apply to the record
   * @return an operation representing the update execution
   */
  Operation<Boolean> update(K key, UpdateOperation<? super K> transform);

  /**
   * An asynchronous equivalent to {@link DatasetWriterReader#delete(Comparable)}.
   *
   * @param key key of the record to remove
   * @return an operation representing the delete execution
   */
  Operation<Boolean> delete(K key);

  /**
   * Equivalent to {@link DatasetWriterReader#on(Comparable)}, but returns an AsyncReadWriteRecordAccessor
   * rather than a ReadWriteRecordAccessor.
   *
   * @param key key of the record
   * @return an AsyncReadWriteRecordAccessor
   */
  @Override
  AsyncReadWriteRecordAccessor<K> on(K key);

  @Override
  AsyncMutableRecordStream<K> records();
}
