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
package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.store.definition.CellDefinition;

/**
 * Provides access to secondary indexes.
 *
 * @param <K> the value type of the primary key for the {@code SovereignDataset} from which
 *           this {@code Catalog} is derived
 *
 * @author mscott
 * @author Clifford W. Johnson
 */
// This interface permits access to SovereignDatasetImpl methods that, because of access
// to SPI package from API, prevent the methods from appearing in SovereignDataset.
public interface Catalog<K extends Comparable<K>> {

  boolean DISABLE_SECONDARY_INDEXES = false;

  /**
   * If secondary (non-primary) indexing is enabled and live, returns the
   * {@link com.terracottatech.sovereign.indexing.SovereignIndexSettings#btree() btree} index defined
   * over the {@code CellDefinition} provided.
   *
   * @param def the {@code CellDefinition} for which the index is sought
   * @param <T> the value type for the {@code CellDefinition} and index
   *
   * @return the {@code SortedIndexMap} corresponding to {@code def}
   *
   * @throws java.lang.AssertionError if secondary indexing is disabled
   */
  <T extends Comparable<T>> SovereignSortedIndexMap<T, K> getSortedIndexFor(CellDefinition<T> def);

  /**
   * Indicates whether or not a secondary index exists for the {@code CellDefinition} specified.
   *
   * @param def the {@code CellDefinition} for which the index is sought
   * @param <T> the value type for the {@code CellDefinition} and index
   *
   * @return {@code true} if secondary indexing is enabled and live and a
   *    {@link com.terracottatech.sovereign.indexing.SovereignIndexSettings#btree() btree} index
   *    exists for {@code def}; {@code false} otherwise
   */
  <T extends Comparable<T>> boolean hasSortedIndex(CellDefinition<T> def);

  SovereignContainer<K> getContainer();

  /**
   * Current Revision.
   * @return
   */
  long getCurrentMSN();

  /**
   * Indicates if this the {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset} underlying
   * this {@code Catalog} is undergoing disposal or is presently disposed and no longer usable.
   *
   * @return {@code true} if the dataset underlying this catalog is disposed and no longer usable;
   *      {@code false} otherwise
   */
  boolean isDisposed();
}
