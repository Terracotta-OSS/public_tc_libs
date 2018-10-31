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
package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.definition.CellDefinition;

import java.util.Optional;
import java.util.function.Function;

import static com.terracottatech.store.intrinsics.IntrinsicType.UPDATE_OPERATION_REMOVE;

public class RemoveOperation<K extends Comparable<K>, T> extends LeafIntrinsicUpdateOperation<K> implements UpdateOperation.CellUpdateOperation<K, T> {

  private final CellDefinition<T> cellDefinition;

  public RemoveOperation(CellDefinition<T> definition) {
    super(UPDATE_OPERATION_REMOVE);
    this.cellDefinition = definition;
  }

  @Override
  public CellDefinition<T> definition() {
    return cellDefinition;
  }

  @Override
  public Function<Record<?>, Optional<Cell<T>>> cell() {
    return r -> Optional.empty();
  }

  @Override
  public String toString() {
    return "remove(" + cellDefinition + ")";
  }
}
