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
package com.terracottatech.sovereign.impl.dataset.metadata;

import com.terracottatech.store.definition.CellDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * @author cschanck
 **/
public class DatasetSchemaThreadLocal extends AbstractSchema {

  private final DatasetSchemaBackend parent;

  public DatasetSchemaThreadLocal(DatasetSchemaBackend parent) {
    this.parent = parent;
    parent.register(this);
  }

  @Override
  public Map<CellDefinition<?>, SchemaCellDefinition<?>> makeDefToSchemaMap() {
    return new HashMap<>();
  }

  @Override
  public Map<Integer, CellDefinition<?>> makeIdToSchemaMap() {
    return new HashMap<>();
  }

  @Override
  public SchemaCellDefinition<?> idFor(CellDefinition<?> def) {
    // fast local path
    SchemaCellDefinition<?> probe = getDefToSchemaMap().get(def);
    if (probe != null) {
      return probe;
    }

    SchemaCellDefinition<?> newDef = parent.idFor(def);
    if (newDef != null) {
      getDefToSchemaMap().put(def, newDef);
      getIdToSchemaMap().put(newDef.id(), def);
    }
    return newDef;
  }

  @Override
  public CellDefinition<?> definitionFor(int id) {
    // fast local path
    CellDefinition<?> def = getIdToSchemaMap().get(id);
    if (def != null) {
      return def;
    }
    def = parent.definitionFor(id);
    if (def != null) {
      getIdToSchemaMap().put(id, def);
      return def;
    }
    throw new IllegalStateException("unknown schema id: " + id);
  }

  @Override
  public boolean isOverflowed() {
    return parent.isOverflowed();
  }

  public void dispose() {
    getDefToSchemaMap().clear();
    getIdToSchemaMap().clear();
  }
}
