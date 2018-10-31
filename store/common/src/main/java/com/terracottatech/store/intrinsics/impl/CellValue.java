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
import com.terracottatech.store.definition.ComparableCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildableFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.intrinsics.IntrinsicBuildableComparableFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableStringFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

public class CellValue<T> extends LeafIntrinsic implements BuildableFunction<Record<?>, T>, IntrinsicFunction<Record<?>, T> {

  private final CellDefinition<T> cellDefinition;
  private final T defaultValue;

  public CellValue(CellDefinition<T> cellDefinition, T defaultValue) {
    super(IntrinsicType.FUNCTION_CELL_VALUE);
    this.cellDefinition = cellDefinition;
    this.defaultValue = defaultValue;
  }

  public CellDefinition<T> getCellDefinition() {
    return cellDefinition;
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  @Override
  public T apply(Record<?> cells) {
    Optional<T> opt = cells.get(cellDefinition);
    if (opt.isPresent() || defaultValue == null) {
      return opt.get();
    } else {
      return defaultValue;
    }
  }

  @Override
  public BuildablePredicate<Record<?>> is(T test) {
    return new NonGatedComparison.Equals<>(this, new Constant<>(test));
  }

  @Override
  public String toString() {
    if (defaultValue == null) {
      return cellDefinition.name() + ".valueOrFail()";
    } else {
      return cellDefinition.name() + ".valueOr(" + defaultValue + ")";
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CellValue<?> cellValue = (CellValue<?>) o;
    return Objects.equals(cellDefinition, cellValue.cellDefinition) &&
            Objects.equals(defaultValue, cellValue.defaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cellDefinition, defaultValue);
  }

  public static class ComparableCellValue<T extends Comparable<T>> extends CellValue<T>
      implements IntrinsicBuildableComparableFunction<Record<?>, T> {

    public ComparableCellValue(ComparableCellDefinition<T> cellDefinition, T defaultValue) {
      super(cellDefinition, defaultValue);
    }
  }

  public static class IntCellValue extends CellValue<Integer> implements IntrinsicBuildableToIntFunction<Record<?>> {

    public IntCellValue(IntCellDefinition cellDefinition, Integer defaultValue) {
      super(cellDefinition, defaultValue);
    }

    @Override
    public int applyAsInt(Record<?> value) {
      return apply(value);
    }

    @Override
    public Comparator<Record<?>> asComparator() {
      return new ComparableComparator<>(this);
    }

    @Override
    public String toString() {
      if (getDefaultValue() == null) {
        return getCellDefinition().name() + ".intValueOrFail()";
      } else {
        return getCellDefinition().name() + ".intValueOr(" + getDefaultValue() + ")";
      }
    }
  }

  public static class LongCellValue extends CellValue<Long> implements IntrinsicBuildableToLongFunction<Record<?>> {

    public LongCellValue(LongCellDefinition cellDefinition, Long defaultValue) {
      super(cellDefinition, defaultValue);
    }

    @Override
    public long applyAsLong(Record<?> value) {
      return apply(value);
    }

    @Override
    public Comparator<Record<?>> asComparator() {
      return new ComparableComparator<>(this);
    }

    @Override
    public String toString() {
      if (getDefaultValue() == null) {
        return getCellDefinition().name() + ".longValueOrFail()";
      } else {
        return getCellDefinition().name() + ".longValueOr(" + getDefaultValue() + ")";
      }
    }
  }

  public static class DoubleCellValue extends CellValue<Double> implements IntrinsicBuildableToDoubleFunction<Record<?>> {

    public DoubleCellValue(DoubleCellDefinition cellDefinition, Double defaultValue) {
      super(cellDefinition, defaultValue);
    }

    @Override
    public double applyAsDouble(Record<?> value) {
      return apply(value);
    }

    @Override
    public Comparator<Record<?>> asComparator() {
      return new ComparableComparator<>(this);
    }

    @Override
    public String toString() {
      if (getDefaultValue() == null) {
        return getCellDefinition().name() + ".doubleValueOrFail()";
      } else {
        return getCellDefinition().name() + ".doubleValueOr(" + getDefaultValue() + ")";
      }
    }
  }

  public static class StringCellValue extends CellValue<String> implements IntrinsicBuildableStringFunction<Record<?>> {

    public StringCellValue(StringCellDefinition cellDefinition, String defaultValue) {
      super(cellDefinition, defaultValue);
    }
  }
}
