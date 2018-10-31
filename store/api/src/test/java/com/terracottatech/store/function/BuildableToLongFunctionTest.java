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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.store.function;

import org.junit.Test;

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BuildableToLongFunctionTest {

  @Test
  public void testAddConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    ToLongFunction<Long> add = function.add(2L);

    assertEquals(add.applyAsLong(1L), 3);
  }

  @Test
  public void testSubtractConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    ToLongFunction<Long> subtract = function.subtract(2L);

    assertEquals(subtract.applyAsLong(1L), -1L);
  }

  @Test
  public void testIncrement() {
    BuildableToLongFunction<Long> function = t -> t;

    ToLongFunction<Long> increment = function.increment();

    assertEquals(increment.applyAsLong(1L), 2L);
  }

  @Test
  public void testDecrement() {
    BuildableToLongFunction<Long> function = t -> t;

    ToLongFunction<Long> decrement = function.decrement();

    assertEquals(decrement.applyAsLong(1L), 0L);
  }

  @Test
  public void testMultiplyConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    ToLongFunction<Long> multiply = function.multiply(2L);

    assertEquals(multiply.applyAsLong(3L), 6L);
  }

  @Test
  public void testDivideConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    ToLongFunction<Long> divide = function.divide(2L);

    assertEquals(divide.applyAsLong(6L), 3L);
  }

  @Test
  public void testIsConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    Predicate<Long> predicate = function.is(1L);

    assertFalse(predicate.test(2L));
    assertTrue(predicate.test(1L));
  }

  @Test
  public void testIsGreaterThanConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    Predicate<Long> predicate = function.isGreaterThan(1L);

    assertFalse(predicate.test(0L));
    assertFalse(predicate.test(1L));
    assertTrue(predicate.test(2L));
  }

  @Test
  public void testIsLessThanConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    Predicate<Long> predicate = function.isLessThan(1L);

    assertTrue(predicate.test(0L));
    assertFalse(predicate.test(1L));
    assertFalse(predicate.test(2L));
  }

  @Test
  public void testIsGreaterThanOrEqualToConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    Predicate<Long> predicate = function.isGreaterThanOrEqualTo(1L);

    assertFalse(predicate.test(0L));
    assertTrue(predicate.test(1L));
    assertTrue(predicate.test(2L));
  }

  @Test
  public void testIsLessThanOrEqualToConstant() {
    BuildableToLongFunction<Long> function = t -> t;

    Predicate<Long> predicate = function.isLessThanOrEqualTo(1L);

    assertTrue(predicate.test(0L));
    assertTrue(predicate.test(1L));
    assertFalse(predicate.test(2L));
  }

  @Test
  public void testAsComparator() {
    BuildableToLongFunction<Long> function = t -> t;

    Comparator<Long> comparator = function.asComparator();

    assertThat(comparator.compare(1L, 2L), lessThan(0));
    assertThat(comparator.compare(1L, 1L), is(0));
    assertThat(comparator.compare(2L, 1L), greaterThan(0));
  }

  @Test
  public void testAsComparatorReverse() {
    BuildableToLongFunction<Long> function = t -> t;

    Comparator<Long> comparator = function.asComparator().reversed();

    assertThat(comparator.compare(1L, 2L), greaterThan(0));
    assertThat(comparator.compare(1L, 1L), is(0));
    assertThat(comparator.compare(2L, 1L), lessThan(0));
  }

  @Test
  public void testAsComparatorReverseReverse() {
    BuildableToLongFunction<Long> function = t -> t;

    Comparator<Long> comparator = function.asComparator().reversed().reversed();

    assertThat(comparator.compare(1L, 2L), lessThan(0));
    assertThat(comparator.compare(1L, 1L), is(0));
    assertThat(comparator.compare(2L, 1L), greaterThan(0));
  }
}
