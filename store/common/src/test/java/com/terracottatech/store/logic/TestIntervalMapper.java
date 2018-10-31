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

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.ComparableCellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.GatedComparison;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.EQ;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.NEQ;
import static com.terracottatech.store.logic.Intervals.intersection;
import static com.terracottatech.store.logic.IntervalMapper.toPredicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Test interval to predicate 2-way conversions.
 * @param <V> type of intervals and predicates.
 */
public abstract class TestIntervalMapper<V extends Comparable<V>> {

  private final ComparableCellDefinition<V> definition;

  @Mock
  private V value;

  @Mock
  private V less;

  @Mock
  private V more;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(value.compareTo(less)).thenReturn(1);
    when(value.compareTo(more)).thenReturn(-1);

    when(less.compareTo(value)).thenReturn(-1);
    when(less.compareTo(more)).thenReturn(-1);

    when(more.compareTo(value)).thenReturn(1);
    when(more.compareTo(less)).thenReturn(1);
  }

  TestIntervalMapper(ComparableCellDefinition<V> definition) {
    this.definition = definition;
  }

  /**
   * x = v <=> x in (v, v)
   */
  @Test
  public void testEq() {
    Interval<V> interval = IntervalMapper.toInterval(EQ, value);
    assertEquals(new Start<>(value, Start.Inclusion.OPEN), interval.getStart());
    assertEquals(new End<>(value, End.Inclusion.OPEN), interval.getEnd());

    Predicate<Record<?>> comparison = definition.value().is(value);
    assertEquals(interval, toRecordInterval(comparison));

    IntervalPredicate<V> predicate = toPredicate(interval);
    assertTrue(predicate.test(value));
    assertFalse(predicate.test(less));
    assertFalse(predicate.test(more));
    assertEquals(EQ, predicate.getOperator());
  }

  /**
   * x < v <=> x in (-inf, v]
   */
  @Test
  public void testLessThan() {
    Interval<V> interval = IntervalMapper.toInterval(LESS_THAN, value);
    assertEquals(Interval.negativeInfinity(), interval.getStart());
    assertEquals(new End<>(value, End.Inclusion.CLOSED), interval.getEnd());

    Predicate<Record<?>> comparison = definition.value().isLessThan(value);
    assertEquals(interval, toRecordInterval(comparison));

    IntervalPredicate<V> predicate = toPredicate(interval);
    assertFalse(predicate.test(value));
    assertTrue(predicate.test(less));
    assertFalse(predicate.test(more));
    assertEquals(LESS_THAN, predicate.getOperator());
  }


  /**
   * x <= v <=> x in (-inf, v)
   */
  @Test
  public void testLessThanOrEqual() {
    Interval<V> interval = IntervalMapper.toInterval(LESS_THAN_OR_EQUAL, value);
    assertEquals(Interval.negativeInfinity(), interval.getStart());
    assertEquals(new End<>(value, End.Inclusion.OPEN), interval.getEnd());

    Predicate<Record<?>> comparison = definition.value().isLessThanOrEqualTo(value);
    assertEquals(interval, toRecordInterval(comparison));

    IntervalPredicate<V> predicate = toPredicate(interval);
    assertTrue(predicate.test(value));
    assertTrue(predicate.test(less));
    assertFalse(predicate.test(more));
    assertEquals(LESS_THAN_OR_EQUAL, predicate.getOperator());
  }


  /**
   * x > v <=> x in [v, inf)
   */
  @Test
  public void testGreaterThan() {
    Interval<V> interval = IntervalMapper.toInterval(GREATER_THAN, value);
    assertEquals(Interval.positiveInfinity(), interval.getEnd());
    assertEquals(new Start<>(value, Start.Inclusion.CLOSED), interval.getStart());

    Predicate<Record<?>> comparison = definition.value().isGreaterThan(value);
    assertEquals(interval, toRecordInterval(comparison));

    IntervalPredicate<V> predicate = toPredicate(interval);
    assertFalse(predicate.test(value));
    assertFalse(predicate.test(less));
    assertTrue(predicate.test(more));
    assertEquals(GREATER_THAN, predicate.getOperator());
  }


  /**
   * x >= v <=> x in (v, inf)
   */
  @Test
  public void testGreaterThanOrEqual() {
    Interval<V> interval = IntervalMapper.toInterval(GREATER_THAN_OR_EQUAL, value);
    assertEquals(Interval.positiveInfinity(), interval.getEnd());
    assertEquals(new Start<>(value, Start.Inclusion.OPEN), interval.getStart());

    Predicate<Record<?>> comparison = definition.value().isGreaterThanOrEqualTo(value);
    assertEquals(interval, toRecordInterval(comparison));

    IntervalPredicate<V> predicate = toPredicate(interval);
    assertTrue(predicate.test(value));
    assertFalse(predicate.test(less));
    assertTrue(predicate.test(more));
    assertEquals(GREATER_THAN_OR_EQUAL, predicate.getOperator());
  }


  /**
   * x != v does not map to a single toRecordInterval!
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNeq() {
    IntervalMapper.toInterval(NEQ, value);
  }

  /**
   * x < v & x > v = false
   */
  @Test
  public void testEmpty() {
    Interval<V> lt = IntervalMapper.toInterval(LESS_THAN, value);
    Interval<V> gt = IntervalMapper.toInterval(GREATER_THAN, value);
    Interval<V> empty = intersection(lt, gt);

    Predicate<Record<?>> ltp = definition.value().isLessThan(value);
    Predicate<Record<?>> gtp = definition.value().isGreaterThan(value);
    assertEquals(empty, toInterval(ltp, gtp));

    IntervalPredicate<V> predicate = toPredicate(empty);
    assertFalse(predicate.test(value));
    assertFalse(predicate.test(less));
    assertFalse(predicate.test(more));
    assertEquals(EQ, predicate.getOperator());
  }

  /**
   * x < less < v & x > more > v = false
   */
  @Test
  public void testNegative() {
    Interval<V> lt = IntervalMapper.toInterval(LESS_THAN, less);
    Interval<V> gt = IntervalMapper.toInterval(GREATER_THAN, more);
    Interval<V> negative = intersection(lt, gt);

    Predicate<Record<?>> ltp = definition.value().isLessThan(less);
    Predicate<Record<?>> gtp = definition.value().isGreaterThan(more);
    assertEquals(negative, toInterval(ltp, gtp));

    IntervalPredicate<V> predicate = toPredicate(negative);
    assertFalse(predicate.test(value));
    assertFalse(predicate.test(less));
    assertFalse(predicate.test(more));
    assertEquals(EQ, predicate.getOperator());
  }

  /**
   * x > less & x < more <=>  x in [less, more]
   */
  @Test
  public void testClosedRange() {
    Interval<V> gt = IntervalMapper.toInterval(GREATER_THAN, less);
    Interval<V> lt = IntervalMapper.toInterval(LESS_THAN, more);
    Interval<V> closed = intersection(lt, gt);

    Predicate<Record<?>> gtp = definition.value().isGreaterThan(less);
    Predicate<Record<?>> ltp = definition.value().isLessThan(more);
    assertEquals(closed, toInterval(ltp, gtp));

    IntervalPredicate<V> predicate = toPredicate(closed);
    assertTrue(predicate.test(value));
    assertFalse(predicate.test(less));
    assertFalse(predicate.test(more));
    assertEquals(GREATER_THAN, predicate.getOperator());
  }

  /**
   * x >= less & x <= more <=> x in (less, more)
   */
  @Test
  public void testOpenRange() {
    Interval<V> gte = IntervalMapper.toInterval(GREATER_THAN_OR_EQUAL, less);
    Interval<V> lte = IntervalMapper.toInterval(LESS_THAN_OR_EQUAL, more);
    Interval<V> open = intersection(lte, gte);

    Predicate<Record<?>> gtp = definition.value().isGreaterThanOrEqualTo(less);
    Predicate<Record<?>> ltp = definition.value().isLessThanOrEqualTo(more);
    assertEquals(open, toInterval(ltp, gtp));

    IntervalPredicate<V> predicate = toPredicate(open);
    assertTrue(predicate.test(value));
    assertTrue(predicate.test(less));
    assertTrue(predicate.test(more));
    assertEquals(GREATER_THAN_OR_EQUAL, predicate.getOperator());
  }

  /**
   * x >= less & x < more <=> x in (less, more]
   */
  @Test
  public void testOpenToClosed() {
    Interval<V> gte = IntervalMapper.toInterval(GREATER_THAN_OR_EQUAL, less);
    Interval<V> lt = IntervalMapper.toInterval(LESS_THAN, more);
    Interval<V> openToClosed = intersection(lt, gte);

    Predicate<Record<?>> gtp = definition.value().isGreaterThanOrEqualTo(less);
    Predicate<Record<?>> ltp = definition.value().isLessThan(more);
    assertEquals(openToClosed, toInterval(ltp, gtp));

    IntervalPredicate<V> predicate = toPredicate(openToClosed);
    assertTrue(predicate.test(value));
    assertTrue(predicate.test(less));
    assertFalse(predicate.test(more));
    assertEquals(GREATER_THAN_OR_EQUAL, predicate.getOperator());
  }

  /**
   * x > less & x <= more <=> x in [less, more)
   */
  @Test
  public void testClosedToOpen() {
    Interval<V> gt = IntervalMapper.toInterval(GREATER_THAN, less);
    Interval<V> lte = IntervalMapper.toInterval(LESS_THAN_OR_EQUAL, more);
    Interval<V> closedToOpen = intersection(lte, gt);

    Predicate<Record<?>> gtp = definition.value().isGreaterThan(less);
    Predicate<Record<?>> ltp = definition.value().isLessThanOrEqualTo(more);
    assertEquals(closedToOpen, toInterval(ltp, gtp));

    IntervalPredicate<V> predicate = toPredicate(closedToOpen);
    assertTrue(predicate.test(value));
    assertFalse(predicate.test(less));
    assertTrue(predicate.test(more));
    assertEquals(GREATER_THAN, predicate.getOperator());
  }

  /**
   * x >= -inf & x <= inf = true
   */
  @Test
  public void testInfiniteRange() {
    Interval<V> infinite = Interval.<V>builder().build();

    IntervalPredicate<V> predicate = toPredicate(infinite);
    assertTrue(predicate.test(value));
    assertTrue(predicate.test(less));
    assertTrue(predicate.test(more));
    assertEquals(GREATER_THAN_OR_EQUAL, predicate.getOperator());
  }

  @SuppressWarnings("unchecked")
  private static <V extends Comparable<V>> Interval<V> toRecordInterval(Predicate<Record<?>> predicate) {
    if (predicate instanceof GatedComparison) {
      return toInterval((GatedComparison<?, V>) predicate);
    } else {
      throw new IllegalArgumentException("Cannot map to toRecordInterval: " + predicate);
    }
  }

  @SuppressWarnings({"unchecked", "varargs"})
  @SafeVarargs
  private static <V extends Comparable<V>> Interval<V> toInterval(Predicate<Record<?>>... predicates) {
    return Stream.of(predicates)
            .filter(p -> p instanceof GatedComparison)
            .map(p -> (GatedComparison<?, V>) p)
            .map(TestIntervalMapper::toInterval)
            .reduce(Intervals::intersection)
            .orElseGet(() -> Interval.<V>builder().build());
  }

  private static <V extends Comparable<V>> Interval<V> toInterval(GatedComparison<?, V> comparison) {
    IntrinsicFunction<?, V> right = comparison.getRight();
    if (right instanceof Constant) {
      @SuppressWarnings("unchecked")
      Constant<?, V> constant = (Constant<?, V>) right;
      V value = constant.getValue();
      ComparisonType comparisonType = comparison.getComparisonType();
      return IntervalMapper.toInterval(comparisonType, value);
    } else {
      throw new IllegalArgumentException("Cannot map to toInterval: " + comparison);
    }
  }
}
