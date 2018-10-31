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
package com.terracottatech.store.intrinsics;

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.impl.LeafIntrinsicPredicate;

import java.util.Objects;

public class CellDefinitionExists extends LeafIntrinsicPredicate<Record<?>> {

  private final CellDefinition<?> cellDefinition;

  public CellDefinitionExists(CellDefinition<?> cellDefinition) {
    super(IntrinsicType.PREDICATE_CELL_DEFINITION_EXISTS);
    this.cellDefinition = cellDefinition;
  }

  public CellDefinition<?> getCellDefinition() {
    return cellDefinition;
  }

  @Override
  public boolean test(Record<?> record) {
    return record.get(cellDefinition).isPresent();
  }

  @Override
  public String toString() {
    return cellDefinition + ".exists()";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CellDefinitionExists that = (CellDefinitionExists) o;
    return Objects.equals(cellDefinition, that.cellDefinition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cellDefinition);
  }
}
