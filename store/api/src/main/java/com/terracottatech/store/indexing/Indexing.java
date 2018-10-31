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
package com.terracottatech.store.indexing;

import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.async.Operation;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * Allows creation, destruction, and querying of the indexes available to a
 * {@code Dataset}.
 *
 * @author Chris Dennis
 */
public interface Indexing {

  /**
   * Trigger creation of a new index using the given settings over cells of the
   * given definition.
   * <p>
   * The returned {@code Operation} completes normally when the index becomes live,
   * and is available for use by the system.  Exceptional completion is observed
   * through the {@link Operation#get} methods -- {@code get} throws an
   * {@link java.util.concurrent.ExecutionException} if the index creation failed.
   * The {@link ExecutionException#getCause() cause} from this exception indicates
   * the reason for the failure.  If the arguments to {@code createIndex} prevent
   * the index from being created, the cause is an {@link IllegalArgumentException};
   * if the dataset state prevents the index from being created, the cause is an
   * {@link IllegalStateException}.
   *
   * @param <T> JDK type of the cells to be indexed
   * @param cellDefinition definition of the cells to index
   * @param settings index type definition
   * @return an {@code Operation} representing the creation of the index
   */
  <T extends Comparable<T>> Operation<Index<T>> createIndex(CellDefinition<T> cellDefinition, IndexSettings settings);

  //possible future magic
  //void adviseIndex(CellDefinition<?> cellDefinition, Setting ... settings);

  //possible future magic
  //<T> Index index(FunctionIdentifier identifier, Function<Record<?>, T> on, Type<T> type, IndexSettings definition);

  /**
   * Destroy an index.
   *
   * @param index index to destroy
   *
   * @throws StoreIndexNotFoundException if the index does not exist
   */
  void destroyIndex(Index<?> index) throws StoreIndexNotFoundException;

  /**
   * Return the current set of live indexes.
   * <p>
   * Indexes that have been created, but are not yet live will not be returned.
   *
   * @return the set of live indexes
   */
  Collection<Index<?>> getLiveIndexes();

  /**
   * Return the current set of indexes.
   *
   * @return the set of all indexes
   */
  Collection<Index<?>> getAllIndexes();
}
