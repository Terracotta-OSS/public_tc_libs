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

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Interval on a totally ordered set.
 *
 * @param <V> type of the set elements.
 */
class Interval<V extends Comparable<V>> {

  private final Start<V> start;
  private final End<V> end;

  Interval(Start<V> start, End<V> end) {
    this.start = start;
    this.end = end;
  }

  /**
   * Get the start of the interval.
   *
   * @return start of the interval.
   */
  Start<V> getStart() {
    return start;
  }

  /**
   * Get the end of the interval.
   *
   * @return end of the interval.
   */
  End<V> getEnd() {
    return end;
  }

  /**
   * Interval contains no elements.
   * This is the case if the interval is bound, and
   * either the end is less than start, or
   * start and end are equal but at least one of them is not open.
   *
   * @return true if interval contains no elements, else false.
   */
  boolean isEmpty() {
    V startValue = start.getValue();
    V endValue = end.getValue();
    if (startValue == null || endValue == null) {
      return false;
    } else {
      int compare = endValue.compareTo(startValue);
      return compare < 0 || (compare == 0 && !isOpen());
    }
  }

  /**
   * Whether this interval is infinite.
   * @return true if both ends have null values, else false.
   */
  boolean isInfinite() {
    return start.getValue() == null && end.getValue() == null;
  }

  /**
   * Whether this interval is open.
   * @return true if both bounds are open, else false.
   */
  boolean isOpen() {
    return start.isOpen() && end.isOpen();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Interval<?> interval = (Interval<?>) o;
    return Objects.equals(start, interval.start) &&
            Objects.equals(end, interval.end);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end);
  }

  @Override
  public String toString() {
    return start + ", " + end;
  }

  /**
   * Creates a Start with a null value.
   * This behaves as an abstract value of type V which is
   * less than any other V as defined by natural ordering.
   * Used to represent intervals with unbounded start.
   * Note: this is not equal to {@code Double.NEGATIVE_INFINITY} and
   * behaves differently.
   *
   * @param <V> type of the infinity.
   * @return new Start with a null value.
   */
  static <V extends Comparable<V>> Start<V> negativeInfinity() {
    return new Start<>(null, Start.Inclusion.OPEN);
  }

  /**
   * Creates an End with a null value.
   * This behaves as an abstract value of type V which is
   * greater than any other V as defined by natural ordering.
   * Used to represent intervals with unbounded end.
   * Note: this is not equal to {@code Double.POSITIVE_INFINITY} and
   * behaves differently.
   *
   * @param <V> type of the infinity.
   * @return new End with a null value.
   */
  static <V extends Comparable<V>> End<V> positiveInfinity() {
    return new End<>(null, End.Inclusion.OPEN);
  }

  static <V extends Comparable<V>> Builder<V> builder() {
    return new Builder<>();
  }

  /**
   * Use this class to build an interval.
   * By default, start is negativeInfinity, and
   * end is negativeInfinity. The startXxx and endXxx methods
   * set the bounds for given values with corresponding inclusions.
   *
   * @param <V> type of the interval.
   */
  static class Builder<V extends Comparable<V>> {

    private Start<V> start = Interval.negativeInfinity();
    private End<V> end = Interval.positiveInfinity();

    Builder<V> startOpen(@Nonnull V value) {
      start = new Start<>(value, Start.Inclusion.OPEN);
      return this;
    }

    Builder<V> startClosed(@Nonnull V value) {
      start = new Start<>(value, Start.Inclusion.CLOSED);
      return this;
    }

    Builder<V> endClosed(@Nonnull V value) {
      end = new End<>(value, End.Inclusion.CLOSED);
      return this;
    }

    Builder<V> endOpen(@Nonnull V value) {
      end = new End<>(value, End.Inclusion.OPEN);
      return this;
    }

    Interval<V> build() {
      return new Interval<>(start, end);
    }
  }
}
