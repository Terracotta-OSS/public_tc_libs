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
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableStringOptionalFunction;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Objects;
import java.util.Optional;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN_OR_EQUAL;

public class StringCellExtractor extends CellExtractor.ComparableCellExtractor<String>
        implements BuildableStringOptionalFunction<Record<?>> {

  StringCellExtractor(CellDefinition<String> cellDefinition) {
    super(cellDefinition);
  }

  @Override
  public BuildableComparableOptionalFunction<Record<?>, Integer> length() {
    return new Length(this);
  }

  @Override
  public BuildablePredicate<Record<?>> startsWith(String prefix) {
    return new StartsWith(this, prefix);
  }

  public static class Length extends LeafIntrinsic
          implements BuildableComparableOptionalFunction<Record<?>, Integer>,
          IntrinsicFunction<Record<?>, Optional<Integer>> {

    private final StringCellExtractor function;

    private Length(StringCellExtractor function) {
      super(IntrinsicType.OPTIONAL_STRING_LENGTH);
      this.function = function;
    }

    @Override
    public Optional<Integer> apply(Record<?> cells) {
      return function.apply(cells).map(String::length);
    }

    @Override
    public BuildablePredicate<Record<?>> is(Integer test) {
      return new GatedComparison.Equals<>(this, new Constant<>(test));
    }

    @Override
    public BuildablePredicate<Record<?>> isGreaterThan(Integer test) {
      return contrast(GREATER_THAN, test);
    }

    @Override
    public BuildablePredicate<Record<?>> isLessThan(Integer test) {
      return contrast(LESS_THAN, test);
    }

    @Override
    public BuildablePredicate<Record<?>> isGreaterThanOrEqualTo(Integer test) {
      return contrast(GREATER_THAN_OR_EQUAL, test);
    }

    @Override
    public BuildablePredicate<Record<?>> isLessThanOrEqualTo(Integer test) {
      return contrast(LESS_THAN_OR_EQUAL, test);
    }

    private GatedComparison.Contrast<Record<?>, Integer> contrast(ComparisonType operator, Integer test) {
      return new GatedComparison.Contrast<>(this, operator, new Constant<>(test));
    }

    public Intrinsic getFunction() {
      return function;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Length length = (Length) o;
      return Objects.equals(function, length.function);
    }

    @Override
    public int hashCode() {
      return Objects.hash(function);
    }

    @Override
    public String toString() {
      return "length( " + function + " )";
    }
  }

  public static class StartsWith extends LeafIntrinsicPredicate<Record<?>> {

    private final StringCellExtractor function;
    private final String prefix;

    StartsWith(StringCellExtractor function, String prefix) {
      super(IntrinsicType.OPTIONAL_STRING_STARTS_WITH);
      this.function = function;
      this.prefix = prefix;
    }

    @Override
    public boolean test(Record<?> cells) {
      return function.apply(cells)
              .map(s -> s.startsWith(prefix))
              .orElse(false);
    }

    public Intrinsic getFunction() {
      return function;
    }

    public String getPrefix() {
      return prefix;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      StartsWith that = (StartsWith) o;
      return Objects.equals(function, that.function) &&
              Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), function, prefix);
    }

    @Override
    public String toString() {
      return function + " startsWith " + prefix;
    }
  }
}
