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

import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableStringFunction;
import com.terracottatech.store.function.BuildableToIntFunction;
import com.terracottatech.store.intrinsics.impl.ComparableComparator;
import com.terracottatech.store.intrinsics.impl.LeafIntrinsic;
import com.terracottatech.store.intrinsics.impl.LeafIntrinsicPredicate;

import java.util.Comparator;
import java.util.Objects;

public interface IntrinsicBuildableStringFunction<T>
        extends BuildableStringFunction<T>, IntrinsicBuildableComparableFunction<T, String> {

  @Override
  default BuildableToIntFunction<T> length() {
    return new Length<>(this);
  }

  @Override
  default BuildablePredicate<T> startsWith(String prefix) {
    return new StartsWith<>(this, prefix);
  }

  class Length<T> extends LeafIntrinsic
          implements IntrinsicBuildableToIntFunction<T>, IntrinsicFunction<T, Integer> {

    private final IntrinsicBuildableStringFunction<T> function;

    private Length(IntrinsicBuildableStringFunction<T> function) {
      super(IntrinsicType.STRING_LENGTH);
      this.function = function;
    }

    @Override
    public int applyAsInt(T value) {
      return function.apply(value).length();
    }

    @Override
    public Comparator<T> asComparator() {
      return new ComparableComparator<>(this);
    }

    @Override
    public Integer apply(T t) {
      return applyAsInt(t);
    }

    @Override
    public String toString() {
      return "length( " + function + " )";
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
      if (!super.equals(o)) {
        return false;
      }
      Length<?> length = (Length<?>) o;
      return Objects.equals(function, length.function);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), function);
    }
  }

  class StartsWith<T> extends LeafIntrinsicPredicate<T> {

    private final IntrinsicBuildableStringFunction<T> function;
    private final String prefix;

    StartsWith(IntrinsicBuildableStringFunction<T> function, String prefix) {
      super(IntrinsicType.STRING_STARTS_WITH);
      this.function = function;
      this.prefix = prefix;
    }

    @Override
    public boolean test(T value) {
      return function.apply(value).startsWith(prefix);
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
      StartsWith<?> that = (StartsWith<?>) o;
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
