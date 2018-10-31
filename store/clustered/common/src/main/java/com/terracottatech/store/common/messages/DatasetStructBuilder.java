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

package com.terracottatech.store.common.messages;

import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;

import static com.terracottatech.store.common.messages.StoreStructures.CELL_DEFINITION_STRUCT;
import static com.terracottatech.store.common.messages.StoreStructures.CELL_STRUCT;
import static com.terracottatech.store.common.messages.StoreStructures.KEY_STRUCT;

public class DatasetStructBuilder {

  private static final Struct RECORD_DATA_STRUCT = StructBuilder.newStructBuilder()
          .int64("msn", 10)
          .struct("key", 20, KEY_STRUCT)
          .structs("cells", 30, CELL_STRUCT)
          .build();

  private static final Struct UUID_STRUCT = StructBuilder.newStructBuilder()
          .int64("msb", 10)
          .int64("lsb", 20)
          .build();

  private final Struct intrinsicStruct;
  private final Struct throwableStruct;
  private final StructBuilder underlying = StructBuilder.newStructBuilder();

  public DatasetStructBuilder(Struct intrinsicStruct, Struct throwableStruct) {
    this.intrinsicStruct = intrinsicStruct;
    this.throwableStruct = throwableStruct;
  }

  public DatasetStructBuilder key(String name, int index) {
    underlying.struct(name, index, KEY_STRUCT);
    return this;
  }

  public DatasetStructBuilder intrinsic(String name, int index) {
    underlying.struct(name, index, intrinsicStruct);
    return this;
  }

  public DatasetStructBuilder bool(String name, int index) {
    underlying.bool(name, index);
    return this;
  }

  public Struct build() {
    return underlying.build();
  }

  public DatasetStructBuilder cells(String name, int index) {
    underlying.structs(name, index, CELL_STRUCT);
    return this;
  }

  public DatasetStructBuilder cellDefinition(String name, int index) {
    underlying.struct(name, index, CELL_DEFINITION_STRUCT);
    return this;
  }

  public DatasetStructBuilder newBuilder() {
    return new DatasetStructBuilder(intrinsicStruct, throwableStruct);
  }

  public DatasetStructBuilder intrinsics(String name, int index) {
    underlying.structs(name, index, intrinsicStruct);
    return this;
  }

  public DatasetStructBuilder enm(String name, int index, EnumMapping<?> mapping) {
    underlying.enm(name, index, mapping);
    return this;
  }

  public DatasetStructBuilder uuid(String name, int index) {
    underlying.struct(name, index, UUID_STRUCT);
    return this;
  }

  public DatasetStructBuilder uuids(String name, int index) {
    underlying.structs(name, index, UUID_STRUCT);
    return this;
  }

  public DatasetStructBuilder throwable(String name, int index) {
    underlying.struct(name, index, throwableStruct);
    return this;
  }

  public DatasetStructBuilder record(String name, int index) {
    underlying.struct(name, index, RECORD_DATA_STRUCT);
    return this;
  }
  public StructBuilder getUnderlying() {
    return underlying;
  }

  public DatasetStructBuilder fp64(String name, int index) {
    underlying.fp64(name, index);
    return this;
  }

  public DatasetStructBuilder int32(String name, int index) {
    underlying.int32(name, index);
    return this;
  }

  public DatasetStructBuilder int64(String name, int index) {
    underlying.int64(name, index);
    return this;
  }

  public DatasetStructBuilder byteBuffer(String name, int index) {
    underlying.byteBuffer(name, index);
    return this;
  }

  public DatasetStructBuilder struct(String name, int index, Struct struct) {
    underlying.struct(name, index, struct);
    return this;
  }

  public DatasetStructBuilder string(String name, int index) {
    underlying.string(name, index);
    return this;
  }
}
