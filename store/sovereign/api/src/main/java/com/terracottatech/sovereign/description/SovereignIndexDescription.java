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
package com.terracottatech.sovereign.description;

import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

import java.io.Serializable;

/**
 * Opaque {@code Serializable} description of an index in a sovereign dataset.
 *
 * @author cschanck
 */
public interface SovereignIndexDescription<T extends Comparable<T>> extends Serializable {

  /**
   * name of the cell the index is on.
   *
   * @return
   */
  String getCellName();

  /**
   * type of the cell the index is on.
   *
   * @return
   */
  Type<T> getCellType();

  /**
   * Index settings.
   *
   * @return
   */
  SovereignIndexSettings getIndexSettings();

  /**
   * Retrieve the cell definition. Formed from cell name and type.
   *
   * @return
   */
  CellDefinition<T> getCellDefinition();

  /**
   * State of this index at the time of the description creation.
   *
   * @return
   */
  SovereignIndex.State getState();

}
