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

import javax.annotation.Nonnull;
import java.util.function.Predicate;

/**
 * Convert comparisons to intervals and vice versa.
 */
class IntervalMapper {

  /**
   * Create and interval for the given comparison operator and value.
   * @param operator equality or inequality operator. Not equals operator is not allowed.
   * @param value a comparable value.
   * @param <V> type of the value
   * @return interval containing all values satisfying the predicate.
   */
  static <V extends Comparable<V>> Interval<V> toInterval(@Nonnull ComparisonType operator, V value) {
    switch (operator) {
      case EQ:
        return Interval.<V>builder().startOpen(value).endOpen(value).build();
      case GREATER_THAN:
        return Interval.<V>builder().startClosed(value).build();
      case GREATER_THAN_OR_EQUAL:
        return Interval.<V>builder().startOpen(value).build();
      case LESS_THAN:
        return Interval.<V>builder().endClosed(value).build();
      case LESS_THAN_OR_EQUAL:
        return Interval.<V>builder().endOpen(value).build();
      default:
        throw new IllegalArgumentException("Unsupported operator: "
                + operator + ": cannot be mapped to a single toInterval.");
    }
  }

  /**
   * Convert an interval to a predicate.
   *
   * @param interval in a totally ordered set.
   * @param <V> type of the interval values.
   * @return predicate testing inclusion of a value in the interval.
   */
  static <V extends Comparable<V>> IntervalPredicate<V> toPredicate(@Nonnull Interval<V> interval) {
    Start<V> start = interval.getStart();
    End<V> end = interval.getEnd();
    if (start.isFinite() && end.isFinite()) {
      int compare = end.getValue().compareTo(start.getValue());
      if (compare == 0) {
        return  (interval.isOpen())
                ? toEquality(start.getValue())
                : new IntervalPredicate<>( false);
      } else if (compare < 0) {
        return new IntervalPredicate<>(false);
      } else {
        Predicate<V> predicate = toComparison(start).and(toComparison(end));
        ComparisonType comparisonType = toFiniteIntervalOperator(interval);
        return new IntervalPredicate<>(predicate, interval, comparisonType);
      }
    } else if (end.isFinite()) {
      return toComparison(end);
    } else if (start.isFinite()) {
      return toComparison(start);
    } else {
      return new IntervalPredicate<>(true);
    }
  }

  private static <V extends Comparable<V>> ComparisonType toFiniteIntervalOperator(@Nonnull Interval<V> interval) {
    switch (interval.getStart().getInclusion()) {
      case CLOSED:
        return ComparisonType.GREATER_THAN;
      default:
        return ComparisonType.GREATER_THAN_OR_EQUAL;
    }
  }

  private static <V extends Comparable<V>> IntervalPredicate<V> toEquality(V value) {
    return new IntervalPredicate<>(value, ComparisonType.EQ);
  }

  private static <V extends Comparable<V>> IntervalPredicate<V> toComparison(Start<V> start) {
    V startValue = start.getValue();
    switch (start.getInclusion()) {
      case CLOSED:
        return new IntervalPredicate<>(startValue, ComparisonType.GREATER_THAN);
      default:
        return new IntervalPredicate<>(startValue, ComparisonType.GREATER_THAN_OR_EQUAL);
    }
  }

  private static <V extends Comparable<V>> IntervalPredicate<V> toComparison(End<V> end) {
    V endValue = end.getValue();
    switch (end.getInclusion()) {
      case CLOSED:
        return new IntervalPredicate<>(endValue, ComparisonType.LESS_THAN);
      default:
        return new IntervalPredicate<>(endValue, ComparisonType.LESS_THAN_OR_EQUAL);
    }
  }
}
