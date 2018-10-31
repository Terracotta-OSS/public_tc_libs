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
package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Record;
import com.terracottatech.store.function.BuildableToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToDoubleFunction;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests for {@link ArithmeticOperation.Double} functions.
 */
public abstract class ArithmeticOperationDoubleTest extends DoubleCellValueTest {

  /**
   * Test that for the degenerate operand (not changing the input value),
   * the operator returns the input function.
   */
  @Test
  public void testDegenerateCase() {
    IntrinsicBuildableToDoubleFunction<Record<?>> inputFunction = new CellValue.DoubleCellValue(DOUBLE_CELL, 0.0D);
    BuildableToDoubleFunction<Record<?>> apply = applyDegenerateCase(inputFunction);
    assertTrue(apply == inputFunction);
    double value = 2;
    assertTrue(apply.is(value).test(createRecord(value)));
  }

  protected abstract BuildableToDoubleFunction<Record<?>> applyDegenerateCase(IntrinsicBuildableToDoubleFunction<Record<?>> inputFunction);

  protected Record<String> createRecord(double value) {
    return new TestRecord<>("record", Collections.singleton(DOUBLE_CELL.newCell(value)));
  }

  /**
   * Test that {@link BuildableToDoubleFunction#add(double)} method called on the given operator
   * returns a portable function which produces a correct result.
   */
  @Test
  public void testAdd() {
    BuildableToDoubleFunction<Record<?>> add = getTestObject().add(2);
    assertThat(add, instanceOf(Add.Double.class));
    assertTrue(add.is(4).test(createRecord(2)));
  }

  /**
   * Test that {@link BuildableToDoubleFunction#multiply(double)} method called on the given operator
   * returns a portable function which produces a correct result.
   */
  @Test
  public void testMultiply() {
    BuildableToDoubleFunction<Record<?>> multiply = getTestObject().multiply(2);
    assertThat(multiply, instanceOf(Multiply.Double.class));
    assertTrue(multiply.is(4).test(createRecord(2)));
  }

  /**
   * Test that {@link BuildableToDoubleFunction#divide(double)} method called on the given operator
   * returns a portable function which produces a correct result.
   */
  @Test
  public void testDivide() {
    BuildableToDoubleFunction<Record<?>> divide = getTestObject().divide(2);
    assertThat(divide, instanceOf(Divide.Double.class));
    assertTrue(divide.is(1).test(createRecord(2)));
  }

  protected abstract BuildableToDoubleFunction<Record<?>> getTestObject();

  /**
   * Test {@link ArithmeticOperation#apply(java.lang.Object)}.
   */
  @Test
  public abstract void testApply();

  /**
   * Test chain of operators.
   */
  @Test
  public abstract void testChain();

  protected void assertResult(double result, double expected, String legend) {
    String message = legend + " = " + result + " != " + expected;
    assertEquals(message, expected, result, 0);
  }

  @Test
  public void testAsComparator() {
    Comparator<Record<?>> cmp = getTestObject().asComparator();
    assertThat(cmp, instanceOf(ComparableComparator.class));
  }
}
