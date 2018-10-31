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
package com.terracottatech.store.function;

import org.junit.Test;

import java.util.Comparator;
import java.util.function.Predicate;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BuildableComparableFunctionTest {

  @Test
  public void testIsGreaterThanConstant() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Predicate<Integer> predicate = function.isGreaterThan(1);

    assertFalse(predicate.test(0));
    assertFalse(predicate.test(1));
    assertTrue(predicate.test(2));
  }

  @Test
  public void testIsLessThanConstant() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Predicate<Integer> predicate = function.isLessThan(1);

    assertTrue(predicate.test(0));
    assertFalse(predicate.test(1));
    assertFalse(predicate.test(2));
  }

  @Test
  public void testIsGreaterThanOrEqualToConstant() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Predicate<Integer> predicate = function.isGreaterThanOrEqualTo(1);

    assertFalse(predicate.test(0));
    assertTrue(predicate.test(1));
    assertTrue(predicate.test(2));
  }

  @Test
  public void testIsLessThanOrEqualToConstant() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Predicate<Integer> predicate = function.isLessThanOrEqualTo(1);

    assertTrue(predicate.test(0));
    assertTrue(predicate.test(1));
    assertFalse(predicate.test(2));
  }

  @Test
  public void testAndConjuction() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Predicate<Integer> predicate = function.isGreaterThan(10).and(function.isLessThan(20));

    assertFalse(predicate.test(4));
    assertTrue(predicate.test(15));
    assertFalse(predicate.test(31));
  }

  @Test
  public void testAsComparator() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Comparator<Integer> comparator = function.asComparator();

    assertThat(comparator.compare(1, 2), lessThan(0));
    assertThat(comparator.compare(1, 1), is(0));
    assertThat(comparator.compare(2, 1), greaterThan(0));
  }

  @Test
  public void testAsComparatorReverse() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Comparator<Integer> comparator = function.asComparator().reversed();

    assertThat(comparator.compare(1, 2), greaterThan(0));
    assertThat(comparator.compare(1, 1), is(0));
    assertThat(comparator.compare(2, 1), lessThan(0));
  }

  @Test
  public void testAsComparatorReverseReverse() {
    BuildableComparableFunction<Integer, Integer> function = t -> t;

    Comparator<Integer> comparator = function.asComparator().reversed().reversed();

    assertThat(comparator.compare(1, 2), lessThan(0));
    assertThat(comparator.compare(1, 1), is(0));
    assertThat(comparator.compare(2, 1), greaterThan(0));
  }
}
