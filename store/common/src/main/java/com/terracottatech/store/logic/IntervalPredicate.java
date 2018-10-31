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

package com.terracottatech.store.logic;

import com.terracottatech.store.intrinsics.impl.ComparisonType;

import java.util.function.Predicate;

/**
 * Predicates with a corresponding comparison operator and a string representation.
 * @param <V> type of the predicate.
 */
class IntervalPredicate<V extends Comparable<V>> implements Predicate<V> {
  private final String text;
  private final Predicate<V> delegate;
  private final ComparisonType comparisonType;

  /**
   * Predicate returning a constant value: either
   * contradiction for an empty/negative interval, or
   * a tautology for an infinite interval.
   * @param constant value returned by the predicate.
   */
  IntervalPredicate(boolean constant) {
    this(v -> constant, " is always " + String.valueOf(constant),
            constant ? ComparisonType.GREATER_THAN_OR_EQUAL : ComparisonType.EQ);
  }

  /**
   * Conjunction of two comparisons - corresponds to a finite interval, e.g:
   * (x > 1) & (x <= 1) <=> x in [1, 1)
   * @param delegate conjunction of two comparisons
   * @param interval - used in string representation
   * @param comparisonType GREATER_THAN or GREATER_THAN_OR_EQUAL
   */
  IntervalPredicate(Predicate<V> delegate, Interval<V> interval, ComparisonType comparisonType) {
    this(delegate, " in " + interval, comparisonType);
  }

  /**
   * Single comparison - corresponds to a half-finite interval.
   * @param value value to compare to.
   * @param comparisonType in this case, the comparison type also defines the predicate.
   */
  IntervalPredicate(V value, ComparisonType comparisonType) {
    this(o -> comparisonType.evaluate(o.compareTo(value)), String.valueOf(comparisonType) + value, comparisonType);
  }

  /**
   * Constructor.
   * @param delegate actual predicate - note it may be a conjunction of two comparisons for a finite interval!
   * @param text string representation to be printed in query plan.
   * @param comparisonType used to define the index range to scan.
   */
  private IntervalPredicate(Predicate<V> delegate, String text, ComparisonType comparisonType) {
    this.comparisonType = comparisonType;
    this.delegate = delegate;
    this.text = text;
  }

  @Override
  public boolean test(V v) {
    return delegate.test(v);
  }

  @Override
  public String toString() {
    return text;
  }

  ComparisonType getOperator() {
    return comparisonType;
  }
}
