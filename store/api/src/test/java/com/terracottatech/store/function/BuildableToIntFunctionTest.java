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
import java.util.function.ToIntFunction;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author cdennis
 */
public class BuildableToIntFunctionTest {

  @Test
  public void testAddConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    ToIntFunction<Integer> add = function.add(2);

    assertEquals(add.applyAsInt(1), 3);
  }

  @Test
  public void testSubtractConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    ToIntFunction<Integer> subtract = function.subtract(2);

    assertEquals(subtract.applyAsInt(1), -1);
  }

  @Test
  public void testIncrement() {
    BuildableToIntFunction<Integer> function = t -> t;

    ToIntFunction<Integer> increment = function.increment();

    assertEquals(increment.applyAsInt(1), 2);
  }

  @Test
  public void testDecrement() {
    BuildableToIntFunction<Integer> function = t -> t;

    ToIntFunction<Integer> decrement = function.decrement();

    assertEquals(decrement.applyAsInt(1), 0);
  }

  @Test
  public void testMultiplyConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    ToIntFunction<Integer> multiply = function.multiply(2);

    assertEquals(multiply.applyAsInt(3), 6);
  }

  @Test
  public void testDivideConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    ToIntFunction<Integer> divide = function.divide(2);

    assertEquals(divide.applyAsInt(6), 3);
  }

  @Test
  public void testIsConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    Predicate<Integer> predicate = function.is(1);

    assertFalse(predicate.test(2));
    assertTrue(predicate.test(1));
  }

  @Test
  public void testIsGreaterThanConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    Predicate<Integer> predicate = function.isGreaterThan(1);

    assertFalse(predicate.test(0));
    assertFalse(predicate.test(1));
    assertTrue(predicate.test(2));
  }

  @Test
  public void testIsLessThanConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    Predicate<Integer> predicate = function.isLessThan(1);

    assertTrue(predicate.test(0));
    assertFalse(predicate.test(1));
    assertFalse(predicate.test(2));
  }

  @Test
  public void testIsGreaterThanOrEqualToConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    Predicate<Integer> predicate = function.isGreaterThanOrEqualTo(1);

    assertFalse(predicate.test(0));
    assertTrue(predicate.test(1));
    assertTrue(predicate.test(2));
  }

  @Test
  public void testIsLessThanOrEqualToConstant() {
    BuildableToIntFunction<Integer> function = t -> t;

    Predicate<Integer> predicate = function.isLessThanOrEqualTo(1);

    assertTrue(predicate.test(0));
    assertTrue(predicate.test(1));
    assertFalse(predicate.test(2));
  }

  @Test
  public void testAsComparator() {
    BuildableToIntFunction<Integer> function = t -> t;

    Comparator<Integer> comparator = function.asComparator();

    assertThat(comparator.compare(1, 2), lessThan(0));
    assertThat(comparator.compare(1, 1), is(0));
    assertThat(comparator.compare(2, 1), greaterThan(0));
  }

  @Test
  public void testAsComparatorReverse() {
    BuildableToIntFunction<Integer> function = t -> t;

    Comparator<Integer> comparator = function.asComparator().reversed();

    assertThat(comparator.compare(1, 2), greaterThan(0));
    assertThat(comparator.compare(1, 1), is(0));
    assertThat(comparator.compare(2, 1), lessThan(0));
  }

  @Test
  public void testAsComparatorReverseReverse() {
    BuildableToIntFunction<Integer> function = t -> t;

    Comparator<Integer> comparator = function.asComparator().reversed().reversed();

    assertThat(comparator.compare(1, 2), lessThan(0));
    assertThat(comparator.compare(1, 1), is(0));
    assertThat(comparator.compare(2, 1), greaterThan(0));
  }
}
