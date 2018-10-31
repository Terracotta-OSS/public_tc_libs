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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.store.logic.Intervals.intersection;
import static java.util.Collections.singletonList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Test {@link Interval}
 */
public abstract class TestIntervals<V extends Comparable<V>> {

  final Interval<V> infinite = builder().build();

  Interval.Builder<V> builder() {
    return Interval.builder();
  }

  /**
   * Test interval for infinite property.
   */
  @Test
  public void testInfinite() {
    getAllIntervals()
            .forEach(interval -> assertEquals(interval == infinite, interval.isInfinite()));
  }

  /**
   * Test non empty intervals.
   */
  @Test
  public void testNonEmpty() {
    getNonEmptyIntervals()
            .map(Interval::isEmpty)
            .forEach(Assert::assertFalse);
  }

  protected abstract Stream<Interval<V>> getNonEmptyIntervals();

  /**
   * Test empty intervals.
   */
  @Test
  public void testEmpty() {
    getEmptyIntervals()
            .map(Interval::isEmpty)
            .forEach(Assert::assertTrue);
  }

  protected abstract Stream<Interval<V>> getEmptyIntervals();

  /**
   * Test that intersection of empty set of intervals is an infinite interval.
   */
  @Test
  public void testIntersectionOfNothing() {
    assertEquals(infinite, intersection());
  }

  private Stream<Interval<V>> getAllIntervals() {
    return concat(getNonEmptyIntervals(), getEmptyIntervals());
  }

  /**
   * Test that intersection is idempotent:
   * a & a = a
   */
  @Test
  public void testIntersectionIsIdempotent() {
    getAllIntervals()
            .forEach(interval -> assertEquals(interval, intersection(interval, interval)));
  }

  /**
   * Test that intersection is commutative:
   * a & b = b & a
   */
  @Test
  public void testIntersectionIsCommutative() {
    getAllIntervals()
            .forEach(a -> getAllIntervals()
                    .forEach(b -> assertEquals(intersection(a, b), intersection(b, a))));
  }


  /**
   * Test that intersection interval a with infinite interval (universal set U) is a itself:
   * a & U = a.
   */
  @Test
  public void testIntersectInfinite() {
    getAllIntervals()
            .forEach(interval -> assertEquals(interval, intersection(interval, infinite)));
  }

  /**
   * Test that intersection with an empty interval (empty set 0) is an empty interval:
   * a & 0 = 0.
   */
  @Test
  public void testIntersectEmpty() {
    getAllIntervals()
            .forEach(interval ->
                    getEmptyIntervals()
                            .map(empty -> intersection(interval, empty))
                            .map(Interval::isEmpty)
                            .forEach(Assert::assertTrue)
            );
  }

  /**
   * Test that intersection is associative:
   * a & (b & c) = (a & b) & c = &(a, b, c)
   */
  @Test
  public void testIntersectionIsAssociative() {
    List<Interval<V>> intervals = getAllIntervals().collect(Collectors.toList());
    for (int i = 0; i < 10; i++) {
      Collections.shuffle(intervals);
      for (int j = 0; j < intervals.size() - 2; j++) {
        Interval<V> a = intervals.get(j);
        Interval<V> b = intervals.get(j + 1);
        Interval<V> c = intervals.get(j + 2);
        Interval<V> a_bc = intersection(a, intersection(b, c));
        Interval<V> ab_c = intersection(intersection(a, b), c);
        Interval<V> abc = intersection(a, b, c);
        assertEquals(a_bc, ab_c);
        assertEquals(ab_c, abc);
      }
    }
  }

  /**
   * Test relationships between
   * closed, half-closed and open intervals
   * between same values.
   * <ul>
   * <li>[a, b) & (a, b) = [a, b)</li>
   * <li>(a, b] & (a, b) = (a, b]</li>
   * <li>[a, b] & (a, b) = [a, b]</li>
   * <li>[a, b) & (a, b] = [a, b]</li>
   * <li>[a, b] & [a, b) = [a, b]</li>
   * <li>[a, b] & (a, b] = [a, b]</li>
   * </ul>
   */
  @Test
  public void testInclusions() {
    for (V a : getEndpointValues()) {
      for (V b : getEndpointValues()) {
        Interval<V> open = builder().startOpen(a).endOpen(b).build();
        Interval<V> endClosed = builder().startOpen(a).endClosed(b).build();
        Interval<V> startClosed = builder().startClosed(a).endOpen(b).build();
        Interval<V> closed = builder().startClosed(a).endClosed(b).build();

        assertEquals(startClosed, intersection(startClosed, open));
        assertEquals(endClosed, intersection(endClosed, open));
        assertEquals(closed, intersection(closed, open));
        assertEquals(closed, intersection(startClosed, endClosed));
        assertEquals(closed, intersection(startClosed, closed));
        assertEquals(closed, intersection(endClosed, closed));
      }
    }
  }

  abstract V[] getEndpointValues();

  /**
   * Test that the intersection of adjacent intervals is
   * degenerate, if adjacent endpoints are both inclusive,
   * else empty:
   * <ul>
   *   <li>(a, b) & (b, c) = (b, b)</li>
   *   <li>(a, b] & (b, c) = 0</li>
   *   <li>(a, b) & [b, c) = 0</li>
   *   <li>(a, b] & [b, c) = 0</li>
   * </ul>
   */
  @Test
  public void testAdjacent() {
    V[] values = getEndpointValues();
    assertTrue(values.length >= 3);
    Arrays.sort(values);
    V a = values[0];
    V b = values[1];
    V c = values[2];

    Interval<V> abOpen = builder().startOpen(a).endOpen(b).build();
    Interval<V> bcOpen = builder().startOpen(b).endOpen(c).build();
    Interval<V> degenerate = builder().startOpen(b).endOpen(b).build();
    assertEquals(degenerate, intersection(abOpen, bcOpen));

    Interval<V> abEndClosed = builder().startOpen(a).endClosed(b).build();
    assertTrue(intersection(abEndClosed, bcOpen).isEmpty());

    Interval<V> bcStartClosed = builder().startClosed(b).endOpen(c).build();
    assertTrue(intersection(abOpen, bcStartClosed).isEmpty());

    assertTrue(intersection(abEndClosed, bcStartClosed).isEmpty());
  }

  /**
   * Test that intersection of overlapping intervals is
   * not empty (per definition):
   * <p>
   * (-inf, b) & (a, inf) = (a, b) which is not empty if a < b.
   */
  @Test
  public void testOverlapping() {
    V[] values = getEndpointValues();
    assertTrue(values.length >= 2);
    Arrays.sort(values);
    V a = values[0];
    V b = values[1];

    Interval<V> _b = builder().endOpen(b).build();
    Interval<V> a_ = builder().startOpen(a).build();
    Interval<V> intersection = intersection(_b, a_);
    assertFalse(intersection.isEmpty());

    Interval<V> expected = builder().startOpen(a).endOpen(b).build();
    assertEquals(expected, intersection);
  }

  /**
   * Test that intersection of disjoint intervals is
   * not empty (per definition):
   * <p>
   * (-inf, a) & (b, inf) = (b, a) which is negative and hence empty if a < b.
   */
  @Test
  public void testDisjoint() {
    V[] values = getEndpointValues();
    assertTrue(values.length >= 2);
    Arrays.sort(values);
    V a = values[0];
    V b = values[1];

    Interval<V> _a = builder().endOpen(a).build();
    Interval<V> b_ = builder().startOpen(b).build();
    Interval<V> intersection = intersection(_a, b_);
    assertTrue(intersection.isEmpty());

    Interval<V> expected = builder().startOpen(b).endOpen(a).build();
    assertEquals(expected, intersection);
  }

  @Test
  public void testUnionOfEmptyInput() {
    long count = union().count();
    assertEquals(0, count);
  }

  /**
   * Test that union is idempotent:
   * (a | a) = {a}
   */
  @Test
  public void testUnionIsIdempotent() {
    getAllIntervals()
            .forEach(interval -> assertEquals(
                    unionList(interval, interval),
                    singletonList(interval)
            ));
  }

  /**
   * Test that union with infinite interval (universal set U) is infinite:
   * a | U = U.
   */
  @Test
  public void testUnionWithInfinite() {
    getAllIntervals()
            .forEach(interval -> assertEquals(
                    unionList(interval, infinite),
                    singletonList(infinite)
            ));
  }

  /**
   * Test that union is commutative:
   * a | b = b | a
   */
  @Test
  public void testUnionIsCommutative() {
    getAllIntervals()
            .forEach(a -> getAllIntervals()
                    .forEach(b -> assertEquals(
                            unionList(a, b),
                            unionList(b, a)
                    )));
  }

  /**
   * Test that union is associative:
   * a | (b | c) = (a | b) | c = &(a, b, c)
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testUnionIsAssociative() {
    List<Interval<V>> intervals = getAllIntervals().collect(Collectors.toList());
    for (int i = 0; i < 10; i++) {
      Collections.shuffle(intervals);
      for (int j = 0; j < intervals.size() - 2; j++) {
        Interval<V> a = intervals.get(j);
        Interval<V> b = intervals.get(j + 1);
        Interval<V> c = intervals.get(j + 2);

        List<Interval<V>> a_bc = union(concat(
                of(a),
                union(b, c)
        ));

        List<Interval<V>> ab_c = union(concat(
                union(a, b),
                of(c)
        ));

        List<Interval<V>> abc = unionList(a, b, c);
        assertEquals(a_bc, ab_c);
        assertEquals(ab_c, abc);
      }
    }
  }

  private List<Interval<V>> union(Stream<Interval<V>> intervals) {
    return Intervals.union(intervals).collect(Collectors.toList());
  }

  @SuppressWarnings("varargs")
  @SafeVarargs
  private final Stream<Interval<V>> union(Interval<V>... intervals) {
    return Intervals.union(Stream.of(intervals));
  }

  @SuppressWarnings("varargs")
  @SafeVarargs
  final List<Interval<V>> unionList(Interval<V>... intervals) {
    return Intervals.union(Stream.of(intervals)).collect(Collectors.toList());
  }
}
