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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.terracottatech.store.intrinsics.IntrinsicType.UPDATE_OPERATION_ALL_OF;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;

public class AllOfOperation<K extends Comparable<K>> extends LeafIntrinsicUpdateOperation<K> {

  private final List<CellUpdateOperation<?, ?>> operations;

  public AllOfOperation(List<CellUpdateOperation<?,?>> operations) {
    super(UPDATE_OPERATION_ALL_OF);
    this.operations = operations;
  }

  public List<CellUpdateOperation<?, ?>> getOperations() {
    return operations;
  }

  @Override
  public Iterable<Cell<?>> apply(Record<K> r) {
    Collection<CellDefinition<?>> remove = operations.stream().map(UpdateOperation.CellUpdateOperation::definition).collect(toList());
    return concat(
        stream(r.spliterator(), false).filter(c -> !remove.contains(c.definition())),
        operations.stream().map(UpdateOperation.CellUpdateOperation::cell).map(f -> f.apply(r)).filter(Optional::isPresent).map(Optional::get)
    ).collect(toList());
  }

  @Override
  public String toString() {
    return "allOf" + operations.toString();
  }
}
