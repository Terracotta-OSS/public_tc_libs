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
package com.terracottatech.sovereign.spi;

import com.terracottatech.sovereign.spi.store.Context;
import com.terracottatech.sovereign.spi.store.IndexMap;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.sovereign.spi.store.SortedIndexMap;

/**
 *
 * @author mscott
 */
public interface Space<C extends Context, L extends Locator> {

  /**
   * Get the runtime for this Space instance.
   * @return runtime
   */
  SpaceRuntime runtime();

  /**
   * Total maximum capacity of the Space
   * @return maximum capacity in bytes
   */
  long getCapacity();
  /**
   * Total used space
   * @return space used in bytes
   */
  long getUsed();
  /**
   * Total reserved space
   * @return space reserved in bytes
   */
  long getReserved();
  /**
   * read-only
   * @return true if data is read-only
   */
  boolean isReadOnly();

  /**
   * free this space and all associated structures
   */
  void drop();

  /**
   * Indicates whether or not this {@code Space} is dropped.
   *
   * @return {@code true} if dropped; {@code false} otherwise
   */
  boolean isDropped();

  /**
   * Secondary map structure to associate keys with locators in a given container.
   *
   * @param config the configuration for the new {@code IndexMap}
   * @param <K> the type of the key used in the returned {@code IndexMap}
   *
   * @return a new {@code IndexMap}; if {@link IndexMapConfig#isSortedMap()} returns {@code true},
   *      the returned map is a {@link SortedIndexMap}
   */
  <K extends Comparable<K>> IndexMap<?, ?, ?, ?> createMap(String purpose, IndexMapConfig<K> config);

  /**
   * Removes the indicated {@code IndexMap} from this {@code Space}.  The
   * {@link com.terracottatech.sovereign.spi.store.IndexMap#drop() IndexMap.drop} method is
   * <b>not</b> invoked; the {@code drop} method must be called independently.
   *
   * @param indexMap the {@code IndexMap} to remove and drop
   * @param <K> the type of the key used in {@code indexMap}
   */
  <K extends Comparable<K>, RK extends Comparable<RK>> void removeMap(IndexMap<K, RK, C, L> indexMap);
}
