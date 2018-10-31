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

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.Record;

import java.util.Optional;

/**
 * An asynchronous equivalent to {@link DatasetReader}.
 */
public interface AsyncDatasetReader<K extends Comparable<K>> {

  /**
   * An asynchronous equivalent to {@link DatasetReader#get(Comparable)}.
   *
   * @param key key for the record
   * @return an operation representing the get execution
   */
  Operation<Optional<Record<K>>> get(K key);

  /**
   * Equivalent to {@link DatasetReader#on(Comparable)}, but returns an AsyncReadRecordAccessor
   * rather than a ReadRecordAccessor.
   *
   * @param key key of the record
   * @return an AsyncReadRecordAccessor
   */
  AsyncReadRecordAccessor<K> on(K key);

  /**
   * Returns an {@link AsyncStream} of the records in this dataset.
   *
   * @return an asynchronous stream of records
   */
  AsyncRecordStream<K> records();

}
