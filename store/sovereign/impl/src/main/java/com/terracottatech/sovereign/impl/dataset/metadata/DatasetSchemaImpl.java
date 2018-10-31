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

import java.util.stream.Stream;

/**
 * @author cschanck
 **/
public class DatasetSchemaImpl implements DatasetSchema {

  private final DatasetSchemaBackend backend;

  private ThreadLocal<DatasetSchemaThreadLocal> localSchema = new ThreadLocal<DatasetSchemaThreadLocal>() {
    @Override
    protected DatasetSchemaThreadLocal initialValue() {
      return makeLocal();
    }
  };

  public DatasetSchemaImpl() {
    this.backend = new DatasetSchemaBackend();
  }

  public void initialize(PersistableSchemaList seed) {
    backend.setTo(seed);
  }

  public void reload(PersistableSchemaList seed) {
    backend.reload(seed);
  }

  private DatasetSchemaThreadLocal makeLocal() {
    return new DatasetSchemaThreadLocal(backend);
  }

  private DatasetSchemaThreadLocal local() {
    return localSchema.get();
  }

  @Override
  public Stream<CellDefinition<?>> definitions() {
    return backend.definitions();
  }

  @Override
  public boolean isOverflowed() {
    return backend.isOverflowed();
  }

  public SchemaCellDefinition<?> idFor(CellDefinition<?> def) {
    return local().idFor(def);
  }

  public void mark(CellDefinition<?> def) {
    local().idFor(def);
  }

  public DatasetSchemaBackend getBackend() {
    return backend;
  }

  public CellDefinition<?> definitionFor(int sid) {
    return local().definitionFor(sid);
  }
}
