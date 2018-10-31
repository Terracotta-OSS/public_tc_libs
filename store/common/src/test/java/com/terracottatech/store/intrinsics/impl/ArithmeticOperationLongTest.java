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
import com.terracottatech.store.function.BuildableToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToLongFunction;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests for {@link ArithmeticOperation.Long} functions.
 */
public abstract class ArithmeticOperationLongTest extends LongCellValueTest {

  /**
   * Test that for the degenerate operand (not changing the input value),
   * the operator returns the input function.
   */
  @Test
  public void testDegenerateCase() {
    IntrinsicBuildableToLongFunction<Record<?>> inputFunction = new CellValue.LongCellValue(LONG_CELL, 0L);
    BuildableToLongFunction<Record<?>> apply = applyDegenerateCase(inputFunction);
    assertTrue(apply == inputFunction);
    long value = 2;
    assertTrue(apply.is(value).test(createRecord(value)));
  }

  protected abstract BuildableToLongFunction<Record<?>> applyDegenerateCase(IntrinsicBuildableToLongFunction<Record<?>> inputFunction);

  protected TestRecord<String> createRecord(long value) {
    return new TestRecord<>("record", Collections.singleton(LONG_CELL.newCell(value)));
  }

  /**
   * Test that {@link BuildableToLongFunction#add(long)} method called on the given operator
   * returns a portable function.
   */
  @Test
  public void testAdd() {
    BuildableToLongFunction<Record<?>> add = getTestObject().add(2);
    assertThat(add, instanceOf(Add.Long.class));
    assertTrue(add.is(4).test(createRecord(2)));
  }

  /**
   * Test that {@link BuildableToLongFunction#multiply(long)} method called on the given operator
   * returns a portable function.
   */
  @Test
  public void testMultiply() {
    BuildableToLongFunction<Record<?>> multiply = getTestObject().multiply(2);
    assertThat(multiply, instanceOf(Multiply.Long.class));
    assertTrue(multiply.is(4).test(createRecord(2)));
  }

  /**
   * Test that {@link BuildableToLongFunction#divide(long)} method called on the given operator
   * returns a portable function which produces a correct result.
   */
  @Test
  public void testDivide() {
    BuildableToLongFunction<Record<?>> divide = getTestObject().divide(2);
    assertThat(divide, instanceOf(Divide.Long.class));
    assertTrue(divide.is(1).test(createRecord(2)));
  }

  protected abstract BuildableToLongFunction<Record<?>> getTestObject();

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

  protected void assertResult(long result, long expected, String legend) {
    String message = legend + " = " + result + " != " + expected;
    assertEquals(message, expected, result);
  }

  @Test
  public void testAsComparator() {
    Comparator<Record<?>> cmp = getTestObject().asComparator();
    assertThat(cmp, instanceOf(ComparableComparator.class));
  }
}
