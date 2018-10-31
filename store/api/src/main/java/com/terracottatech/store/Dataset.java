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

import com.terracottatech.store.indexing.Indexing;

/**
 * Main access point in to a Terracotta Store dataset. A dataset consists of a
 * collection of uniquely keyed records. Records are composed of a required key,
 * and a set of uniquely named cells. Each cell holds a single value of that
 * cells type.
 * <p>
 * Operations against a {@code Dataset}, and its subordinate objects, using a clustered
 * configuration may throw a {@link StoreOperationAbandonedException} if a server reconnection
 * occurs during the operation.  It is up to the application to determine the proper recourse
 * for retying or recovering the operation.
 *
 * @param <K> key type
 *
 * @see StoreOperationAbandonedException
 *
 * @author Chris Dennis
 */
public interface Dataset<K extends Comparable<K>> extends AutoCloseable {

  /**
   * Provides read-only access on to this dataset.
   *
   * @return a dataset reader
   */
  DatasetReader<K> reader();

  /**
   * Provides read-write access on to this dataset.
   *
   * @return a dataset writer reader
   */
  DatasetWriterReader<K> writerReader();

  /**
   * Exposes the {@link com.terracottatech.store.indexing.Indexing indexing} instance used to create, alter and delete
   * indexes for this dataset.
   *
   * @return the {@link com.terracottatech.store.indexing.Indexing indexing} instance used to manipulate indexes
   */
  Indexing getIndexing();

  /**
   * Closes the Dataset. Calls to {@code Dataset} accessor methods
   * following a call to this method are likely to be aborted with a
   * {@link StoreRuntimeException}.
   * Note: the possibility of operations on associated streams after
   * calling this method is implementation dependent.
   */
  @Override
  void close();
}
