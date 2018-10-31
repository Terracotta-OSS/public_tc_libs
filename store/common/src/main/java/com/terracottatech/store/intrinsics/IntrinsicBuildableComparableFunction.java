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

import com.terracottatech.store.function.BuildableComparableFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.intrinsics.impl.ComparableComparator;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.NonGatedComparison;

import java.util.Comparator;

public interface IntrinsicBuildableComparableFunction<T, R extends Comparable<R>>
    extends BuildableComparableFunction<T, R>, IntrinsicFunction<T, R> {

  @Override
  default Comparator<T> asComparator() {
    return new ComparableComparator<>(this);
  }

  @Override
  default BuildablePredicate<T> is(R test) {
    return new NonGatedComparison.Equals<>(this, new Constant<>(test));
  }

  @Override
  default BuildablePredicate<T> isGreaterThan(R test) {
    return new NonGatedComparison.Contrast<>(this, ComparisonType.GREATER_THAN, new Constant<>(test));
  }

  @Override
  default BuildablePredicate<T> isLessThan(R test) {
    return new NonGatedComparison.Contrast<>(this, ComparisonType.LESS_THAN, new Constant<>(test));
  }

  @Override
  default BuildablePredicate<T> isGreaterThanOrEqualTo(R test) {
    return new NonGatedComparison.Contrast<>(this, ComparisonType.GREATER_THAN_OR_EQUAL, new Constant<>(test));
  }

  @Override
  default BuildablePredicate<T> isLessThanOrEqualTo(R test) {
    return new NonGatedComparison.Contrast<>(this, ComparisonType.LESS_THAN_OR_EQUAL, new Constant<>(test));
  }
}
