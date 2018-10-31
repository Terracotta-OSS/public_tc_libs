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
import com.terracottatech.store.function.BuildableToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToIntFunction;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests for {@link ArithmeticOperation.Int} functions.
 */
public abstract class ArithmeticOperationIntTest extends IntCellValueTest {

  /**
   * Test that for the degenerate operand (not changing the input value),
   * the operator returns the input function.
   */
  @Test
  public void testDegenerateCase() {
    IntrinsicBuildableToIntFunction<Record<?>> inputFunction = new CellValue.IntCellValue(INT_CELL, 0);
    BuildableToIntFunction<Record<?>> apply = applyDegenerateCase(inputFunction);
    assertSame(apply, inputFunction);
    int value = 2;
    assertTrue(apply.is(value).test(createRecord(value)));
  }

  protected abstract BuildableToIntFunction<Record<?>> applyDegenerateCase(IntrinsicBuildableToIntFunction<Record<?>> inputFunction);

  protected Record<String> createRecord(int value) {
    return new TestRecord<>("record", Collections.singleton(INT_CELL.newCell(value)));
  }

  /**
   * Test that {@link BuildableToIntFunction#add(int)} method called on the given operator
   * returns a portable function which produces a correct result.
   */
  @Test
  public void testAdd() {
    BuildableToIntFunction<Record<?>> add = getTestObject().add(2);
    assertThat(add, instanceOf(Add.Int.class));
    assertTrue(add.is(4).test(createRecord(2)));
  }

  /**
   * Test that {@link BuildableToIntFunction#multiply(int)} method called on the given operator
   * returns a portable function which produces a correct result.
   */
  @Test
  public void testMultiply() {
    BuildableToIntFunction<Record<?>> multiply = getTestObject().multiply(2);
    assertThat(multiply, instanceOf(Multiply.Int.class));
    assertTrue(multiply.is(4).test(createRecord(2)));
  }

  /**
   * Test that {@link BuildableToIntFunction#divide(int)} method called on the given operator
   * returns a portable function which produces a correct result.
   */
  @Test
  public void testDivide() {
    BuildableToIntFunction<Record<?>> divide = getTestObject().divide(2);
    assertThat(divide, instanceOf(Divide.Int.class));
    assertTrue(divide.is(1).test(createRecord(2)));
  }

  protected abstract BuildableToIntFunction<Record<?>> getTestObject();

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

  protected void assertResult(int result, int expected, String legend) {
    String message = legend + " = " + result + " != " + expected;
    assertEquals(message, expected, result);
  }

  @Test
  public void testAsComparator() {
    Comparator<Record<?>> cmp = getTestObject().asComparator();
    assertThat(cmp, instanceOf(ComparableComparator.class));
  }
}
