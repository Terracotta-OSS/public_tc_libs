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
package com.terracottatech.sovereign.impl.indexing;

import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

/**
 * @author cschanck
 */
public class SimpleIndexDescription<T extends Comparable<T>> implements SovereignIndexDescription<T> {

  private static final long serialVersionUID = 2899141782308321688L;

  private final String cellName;
  private final Class<T> cellClass;
  private final SovereignIndexSettings indexSettings;
  private final SovereignIndex.State state;

  public SimpleIndexDescription(SimpleIndex.State state, String cellName, Type<T> cellType,
                                SovereignIndexSettings indexSettings) {
    this.state = state;
    this.cellName = cellName;
    this.cellClass = cellType.getJDKType();
    this.indexSettings = indexSettings;
  }

  public String getCellName() {
    return this.cellName;
  }

  public Type<T> getCellType() {
    return Type.forJdkType(cellClass);
  }

  public SovereignIndexSettings getIndexSettings() {
    return indexSettings;
  }

  public CellDefinition<T> getCellDefinition() {
    return CellDefinition.define(getCellName(), getCellType());
  }

  @Override
  public SovereignIndex.State getState() {
    return state;
  }

  @Override
  public String toString() {
    return "Index: " + state.name() + " " + cellName + '\'' + getCellType() + " :: Sorted: " + indexSettings.isSorted() + "Unique:" + indexSettings.isUnique();
  }
}
