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

import com.terracottatech.sovereign.DatasetSchema;
import com.terracottatech.store.definition.CellDefinition;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * @author cschanck
 **/
public abstract class AbstractSchema implements DatasetSchema {

  private final Map<CellDefinition<?>, SchemaCellDefinition<?>> defToSchemaMap;
  private final Map<Integer, CellDefinition<?>> idToSchemaMap;

  public abstract Map<CellDefinition<?>, SchemaCellDefinition<?>> makeDefToSchemaMap();

  public abstract Map<Integer, CellDefinition<?>> makeIdToSchemaMap();

  static Comparator<CellDefinition<?>> cellDefComp = new Comparator<CellDefinition<?>>() {
    @Override
    public int compare(CellDefinition<?> o1, CellDefinition<?> o2) {
    int ret = o1.name().compareTo(o2.name());
    if (ret == 0) {
      ret = o1.type().getJDKType().getCanonicalName().compareTo(o2.type().getJDKType().getCanonicalName());
    }
    return ret;
    }
  };

  public AbstractSchema() {
    this.defToSchemaMap = makeDefToSchemaMap();
    this.idToSchemaMap = makeIdToSchemaMap();
  }

  Map<CellDefinition<?>, SchemaCellDefinition<?>> getDefToSchemaMap() {
    return defToSchemaMap;
  }

  Map<Integer, CellDefinition<?>> getIdToSchemaMap() {
    return idToSchemaMap;
  }

  public abstract SchemaCellDefinition<?> idFor(CellDefinition<?> def);

  public CellDefinition<?> definitionFor(int id) {
    return idToSchemaMap.get(id);
  }

  public TreeMap<CellDefinition<?>, SchemaCellDefinition<?>> orderedDefinitions() {
    TreeMap<CellDefinition<?>, SchemaCellDefinition<?>> tm = new TreeMap<>(cellDefComp);
    tm.putAll(defToSchemaMap);
    return tm;
  }

  @Override
  public Stream<CellDefinition<?>> definitions() {
    return orderedDefinitions().keySet().stream();
  }

}
