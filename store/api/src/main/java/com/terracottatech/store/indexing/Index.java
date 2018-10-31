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

import com.terracottatech.store.definition.CellDefinition;

/**
 * Representation of an index.
 *
 * @param <T> data type being indexed
 *
 * @author Chris Dennis
 */
public interface Index<T extends Comparable<T>> {

  /**
   * {@link CellDefinition} for the cells that are being indexed by
   * this index.
   *
   * @return indexed cell definition
   */
  CellDefinition<T> on();

  /**
   * {@link IndexSettings} for this index.
   *
   * @return index settings
   */
  IndexSettings definition();

  /**
   * {@link Status} for this index.
   *
   * @return index status
   */
  Status status();

  /**
   * Status of an index.
   */
  public enum Status {
    /**
     * In a multi-stripe configuration, at least one (but not all) stripes do not have the index defined.
     */
    BROKEN,

    /**
     * The state of a no-longer-extant index.
     */
    DEAD,

    /**
     * The state of a defined but not yet created index.
     */
    POTENTIAL,

    /**
     * The state of an created, but not yet fully populated index.
     */
    INITALIZING,

    /**
     * The state of live and query-able index.
     */
    LIVE;
  }
}
