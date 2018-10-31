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

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.naturalOrder;

/**
 * Utility methods for analysis of Intervals.
 *
 */
public class Intervals {

  /**
   * Calculates the intersection of the given intervals.
   * The start of the intersection is the maximum of all starts of the given intervals.
   * The end of the intersection is the minimum of all ends of the given intervals.
   *
   * @param intervals given intervals
   *
   * @param <V>   type of the intervals.
   * @return intersection of intervals.
   */
  @SuppressWarnings("varargs")
  @SafeVarargs
  static <V extends Comparable<V>> Interval<V> intersection(Interval<V>... intervals) {
    Start<V> start = Stream.of(intervals)
            .map(Interval::getStart)
            .max(naturalOrder())
            .orElseGet(Interval::negativeInfinity);
    End<V> end = Stream.of(intervals)
            .map(Interval::getEnd)
            .min(naturalOrder())
            .orElseGet(Interval::positiveInfinity);
    return new Interval<>(start, end);
  }

  /**
   * Find a union of intervals: a collection in which each pair of
   * intervals is replaced with their union if it is a single interval.
   * @param <V> type of the intervals.
   * @param intervals stream of intervals
   * @return union of intervals.
   */
  static <V extends Comparable<V>> Stream<Interval<V>> union(Stream<Interval<V>> intervals) {
    Deque<Interval<V>> union = new ArrayDeque<>();
    intervals.sorted(Comparator.comparing(Interval::getStart))
            .forEach(current -> {
              if (union.isEmpty()) {
                union.add(current);
              } else {
                Interval<V> last = union.removeLast();
                union.addAll(union(last, current));
              }
            });
    return union.stream();
  }

  /**
   * Absorb intervals if possible, provided they are sorted by starts:
   * @param first (s1, e1)
   * @param next (s2, e2) such that s2 >= s1
   * @return if absorbable, then (s1, e2), else (s1, e1) | (s2, e2)
   */
  private static <V extends Comparable<V>> List<Interval<V>> union(Interval<V> first, Interval<V> next) {
    return areAbsorbable(first, next)
            ? singletonList(absorb(first, next))
            : asList(first, next);
  }

  /**
   * (s1, e1) and (s2, e2) are absorbable if either of the following is true:
   * <ul>
   *   <li>(s1, e1) == (s2, e2)</li>
   *   <li>e1 == inf</li>
   *   <li>s2 == -inf</li>
   *   <li>s2.value < e1.value</li>
   *   <li>s2.value == e1.value, and either at least one of them is open or (s2, e2) is empty</li>
   * </ul>
   *
   */
  private static <V extends Comparable<V>> boolean areAbsorbable(Interval<V> first, Interval<V> next) {
    if (Objects.equals(first, next)) {
      return true;
    }
    End<V> firstEnd = first.getEnd();
    V firstEndValue = firstEnd.getValue();
    if (firstEndValue == null) {
      return true;
    }
    Start<V> nextStart = next.getStart();
    V nextStartValue = nextStart.getValue();
    if (nextStartValue == null) {
      return true;
    } else {
      int compare = nextStartValue.compareTo(firstEndValue);
      return compare < 0 || (compare == 0 && (firstEnd.isOpen() || nextStart.isOpen() || next.isEmpty()));
    }
  }

  /**
   * Absorb two intervals into a single interval union.
   * @param first (s1, e1)
   * @param next (s2, e2)
   * @return (s1, e2) if e2 > e1, else (s1, e2)
   */
  private static <V extends Comparable<V>> Interval<V> absorb(Interval<V> first, Interval<V> next) {
    return next.getEnd().compareTo(first.getEnd()) > 0
            ? new Interval<>(first.getStart(), next.getEnd())
            : first;
  }
}
