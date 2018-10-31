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

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;
import com.terracottatech.store.function.BuildableOptionalFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableStringOptionalFunction;
import com.terracottatech.store.intrinsics.IntrinsicFunction;

import java.util.Objects;
import java.util.Optional;

import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_CELL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN_OR_EQUAL;

public abstract class CellExtractor<T> extends LeafIntrinsic {

  private final CellDefinition<T> cellDefinition;

  private CellExtractor(CellDefinition<T> cellDefinition) {
    super(FUNCTION_CELL);
    this.cellDefinition = cellDefinition;
  }

  public CellDefinition<T> extracts() {
    return cellDefinition;
  }

  public String toString() {
    return cellDefinition.name();
  }

  public static <T> BuildableOptionalFunction<Record<?>, T> extractPojo(CellDefinition<T> cellDefinition) {
    return new PojoCellExtractor<>(cellDefinition);
  }

  public static <T extends Comparable<T>> BuildableComparableOptionalFunction<Record<?>, T> extractComparable(CellDefinition<T> cellDefinition) {
    return new ComparableCellExtractor<>(cellDefinition);
  }

  public static BuildableStringOptionalFunction<Record<?>> extractString(CellDefinition<String> cellDefinition) {
    return new StringCellExtractor(cellDefinition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CellExtractor<?> that = (CellExtractor<?>) o;
    return Objects.equals(cellDefinition, that.cellDefinition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cellDefinition);
  }

  static class PojoCellExtractor<T> extends CellExtractor<T> implements IntrinsicFunction<Record<?>, Optional<T>>, BuildableOptionalFunction<Record<?>, T> {

    PojoCellExtractor(CellDefinition<T> cellDefinition) {
      super(cellDefinition);
    }

    @Override
    public Optional<T> apply(Record<?> t) {
      return t.get(extracts());
    }

    @Override
    public BuildablePredicate<Record<?>> is(T test) {
      return new GatedComparison.Equals<>(this, new Constant<>(test));
    }
  }

  static class ComparableCellExtractor<T extends Comparable<T>> extends PojoCellExtractor<T> implements BuildableComparableOptionalFunction<Record<?>, T> {

    ComparableCellExtractor(CellDefinition<T> cellDefinition) {
      super(cellDefinition);
    }

    @Override
    public BuildablePredicate<Record<?>> isGreaterThan(T test) {
      return new GatedComparison.Contrast<>(this, GREATER_THAN, new Constant<>(test));
    }

    @Override
    public BuildablePredicate<Record<?>> isGreaterThanOrEqualTo(T test) {
      return new GatedComparison.Contrast<>(this, GREATER_THAN_OR_EQUAL, new Constant<>(test));
    }

    @Override
    public BuildablePredicate<Record<?>> isLessThan(T test) {
      return new GatedComparison.Contrast<>(this, LESS_THAN, new Constant<>(test));
    }

    @Override
    public BuildablePredicate<Record<?>> isLessThanOrEqualTo(T test) {
      return new GatedComparison.Contrast<>(this, LESS_THAN_OR_EQUAL, new Constant<>(test));
    }
  }
}
