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
package com.terracottatech.sovereign.indexing;

import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.store.definition.CellDefinition;

/**
 * Created by cschanck
 */
public interface SovereignIndex<T extends Comparable<T>> extends Comparable<SovereignIndex<?>> {

  static enum State { UNKNOWN, CREATED, LOADING, LIVE }

  /**
   * The {@link CellDefinition} this index is on.
   * @return
   */
  CellDefinition<T> on();

  // TODO this is misnamed, since we have a cell definition concept.

  /**
   * Index settings.
   * @return
   */
  SovereignIndexSettings definition();

  /**
   * {@link State} of this index.
   * @return
   */
  State getState();

  /**
   * Is it live, ok for use.
   * @return
   */
  boolean isLive();

  /**
   * Get the descpription of this index
   * @return
   */
  SovereignIndexDescription<T> getDescription();

  /**
   * Get the statistics object for this index.
   * @return
   */
  SovereignIndexStatistics getStatistics();

}
