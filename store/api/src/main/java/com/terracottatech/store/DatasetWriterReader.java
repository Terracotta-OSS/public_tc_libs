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
package com.terracottatech.store;

import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.stream.MutableRecordStream;

import static java.util.Arrays.asList;

/**
 * Accessor that provides read and write access on to a dataset.
 * <p>
 * Operations on a dataset fall in to two classes:
 * <ul>
 * <li>Single Key CRUD operations:<ul>
 * <li>Create - {@link DatasetWriterReader#add(Comparable, Cell[])}</li>
 * <li>Read - {@link DatasetReader#get(Comparable)}</li>
 * <li>Update - {@link DatasetWriterReader#update(Comparable, UpdateOperation)}</li>
 * <li>Delete - {@link DatasetWriterReader#delete(Comparable)}</li>
 * </ul></li>
 * <li>Computations - {@link DatasetReader#records()}</li>
 * </ul>
 * Computations are read-only operations but may perform mutations by using a
 * suitable mutative terminal stream operation.  For example a bulk deletion based on
 * a predicate can be performed so:
 * <pre>  datasetWriterReader.records()
 *           .filter(predicate)
 *           .forEach(datasetWriterReader.delete());</pre>
 * <p>
 * Functional types passed to dataset methods can be expressed as either opaque
 * or transparent types.  Lambda expressions and those utilizing user types are
 * opaque to the dataset.  Functions expressed using the
 * {@link com.terracottatech.store.function functional DSL} are however
 * transparent.  This means their behavior is well understood, and therefore a
 * much larger set of optimizations can be applied to the operation concerned.
 *
 * @param <K> key type
 *
 * @author cdennis
 */
public interface DatasetWriterReader<K extends Comparable<K>> extends DatasetReader<K> {
  /**
   * Creates a record for the specified key.
   *
   * @param key key for the record
   * @param cells cells which form the record
   * @return true if this dataset did not already hold a record against this key and so a record was created,
   * false if the dataset already held a record against this key and no record was created.
   */
  default boolean add(K key, Cell<?> ... cells) {
    return add(key, asList(cells));
  }

  /**
   * Creates a record for the specified key.
   *
   * @param key key for the record
   * @param cells a non-{@code null} {@code Iterable} supplying the cells which form the record
   * @return true if this dataset did not already hold a record against this key and so a record was created,
   * false if the dataset already held a record against this key and no record was created.
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  boolean add(K key, Iterable<Cell<?>> cells);

  /**
   * Updates a record for the specified key using the specified UpdateOperation.
   *
   * @param key key of the record to mutate
   * @param transform the mutating transformation to apply to the record
   * @return true if this dataset held a record against this key and so the record was updated, false
   * if the dataset did not hold a record against this key and so no record was updated.
   */
  boolean update(K key, UpdateOperation<? super K> transform);

  /**
   * Deletes a record for the specified key.
   *
   * @param key key of the record to remove
   * @return true if this dataset held a record against this key and so the record was deleted, false
   * if the dataset did not hold a record against this key and so no record was deleted.
   */
  boolean delete(K key);

  /**
   * Returns a ReadWriteRecordAccessor which can be used to for fine-control of
   * read and write operations on the record held against the specified key.
   *
   * @param key key for the record
   * @return a ReadWriteRecordAccessor tied to the supplied key.
   */
  @Override
  ReadWriteRecordAccessor<K> on(K key);

  /**
   * Returns a {@link java.util.stream.Stream} of the records in this dataset.
   *
   * @return a stream of records
   */
  @Override
  MutableRecordStream<K> records();

  /**
   * Returns an asynchronous version of this writer-reader.
   *
   * @return an asynchronous writer-reader
   */
  @Override
  AsyncDatasetWriterReader<K> async();
}
