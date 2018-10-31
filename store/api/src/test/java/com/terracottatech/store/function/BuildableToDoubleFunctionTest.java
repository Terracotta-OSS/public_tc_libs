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
import java.util.function.ToDoubleFunction;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BuildableToDoubleFunctionTest {

  @Test
  public void testAddConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    ToDoubleFunction<Double> add = function.add(2.0d);

    assertEquals(add.applyAsDouble(1.0d), 3.0d, 0.0d);
  }

  @Test
  public void testSubtractConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    ToDoubleFunction<Double> subtract = function.subtract(2.0d);

    assertEquals(subtract.applyAsDouble(1.0d), -1.0d, 0.0d);
  }

  @Test
  public void testIncrement() {
    BuildableToDoubleFunction<Double> function = t -> t;

    ToDoubleFunction<Double> increment = function.increment();

    assertEquals(increment.applyAsDouble(1.0d), 2.0d, 0.0d);
  }

  @Test
  public void testDecrement() {
    BuildableToDoubleFunction<Double> function = t -> t;

    ToDoubleFunction<Double> decrement = function.decrement();

    assertEquals(decrement.applyAsDouble(1.0d), 0.0d, 0.0d);
  }

  @Test
  public void testMultiplyConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    ToDoubleFunction<Double> multiply = function.multiply(2.0d);

    assertEquals(multiply.applyAsDouble(2.0d), 4.0d, 0.0d);
  }

  @Test
  public void testDivideConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    ToDoubleFunction<Double> divide = function.divide(2.0d);

    assertEquals(divide.applyAsDouble(1.0d), 0.5d, 0.0d);
  }

  @Test
  public void testIsConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Predicate<Double> predicate = function.is(1.0d);

    assertFalse(predicate.test(1.1d));
    assertTrue(predicate.test(1.0d));
  }

  @Test
  public void testIsNaN() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Predicate<Double> predicate = function.is(Double.NaN);

    assertFalse(predicate.test(1.1d));
    assertTrue(predicate.test(Double.NaN));
    assertTrue(predicate.test(Double.longBitsToDouble((0x7ffL << 52)| 0xAAAAAAAAAAAAAL)));
  }

  @Test
  public void testIsGreaterThanConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Predicate<Double> predicate = function.isGreaterThan(1.0d);

    assertFalse(predicate.test(0.0d));
    assertFalse(predicate.test(1.0d));
    assertTrue(predicate.test(2.0d));
  }

  @Test
  public void testIsLessThanConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Predicate<Double> predicate = function.isLessThan(1.0d);

    assertTrue(predicate.test(0.0d));
    assertFalse(predicate.test(1.0d));
    assertFalse(predicate.test(2.0d));
  }

  @Test
  public void testIsGreaterThanOrEqualToConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Predicate<Double> predicate = function.isGreaterThanOrEqualTo(1.0d);

    assertFalse(predicate.test(0.0d));
    assertTrue(predicate.test(1.0d));
    assertTrue(predicate.test(2.0d));
  }

  @Test
  public void testIsLessThanOrEqualToConstant() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Predicate<Double> predicate = function.isLessThanOrEqualTo(1.0d);

    assertTrue(predicate.test(0.0d));
    assertTrue(predicate.test(1.0d));
    assertFalse(predicate.test(2.0d));
  }

  @Test
  public void testAsComparator() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Comparator<Double> comparator = function.asComparator();

    assertThat(comparator.compare(1.0d, 2.0d), lessThan(0));
    assertThat(comparator.compare(1.0d, 1.0d), is(0));
    assertThat(comparator.compare(2.0d, 1.0d), greaterThan(0));
  }

  @Test
  public void testAsComparatorReverse() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Comparator<Double> comparator = function.asComparator().reversed();

    assertThat(comparator.compare(1.0d, 2.0d), greaterThan(0));
    assertThat(comparator.compare(1.0d, 1.0d), is(0));
    assertThat(comparator.compare(2.0d, 1.0d), lessThan(0));
  }

  @Test
  public void testAsComparatorReverseReverse() {
    BuildableToDoubleFunction<Double> function = t -> t;

    Comparator<Double> comparator = function.asComparator().reversed().reversed();

    assertThat(comparator.compare(1.0d, 2.0d), lessThan(0));
    assertThat(comparator.compare(1.0d, 1.0d), is(0));
    assertThat(comparator.compare(2.0d, 1.0d), greaterThan(0));
  }

}
