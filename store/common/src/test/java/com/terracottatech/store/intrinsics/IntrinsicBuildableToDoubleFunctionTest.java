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
package com.terracottatech.store.intrinsics;

import org.junit.Test;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link IntrinsicBuildableToDoubleFunction} methods.
 */
public class IntrinsicBuildableToDoubleFunctionTest<T> {

  protected IntrinsicBuildableToDoubleFunction<T> getFunction() {
    return (TestFunction<T>) t -> 0;
  }

  protected T valueSource() {
    return null;
  }

  @Test
  public void testIntrinsic() throws Exception {
    assertThat(getFunction(), is(instanceOf(Intrinsic.class)));
  }

  @Test
  public void testIs() throws Exception {
    IntrinsicBuildableToDoubleFunction<T> toDoubleFunction = getFunction();
    assertThat(toDoubleFunction.is(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toDoubleFunction.is(0).test(valueSource()));
    assertFalse(toDoubleFunction.is(1).test(valueSource()));
  }

  @Test
  public void testIsGreaterThan() throws Exception {
    IntrinsicBuildableToDoubleFunction<T> toDoubleFunction = getFunction();
    assertThat(toDoubleFunction.isGreaterThan(0), is(instanceOf(Intrinsic.class)));
    assertFalse(toDoubleFunction.isGreaterThan(0).test(valueSource()));
    assertTrue(toDoubleFunction.isGreaterThan(-1).test(valueSource()));
    assertFalse(toDoubleFunction.isGreaterThan(1).test(valueSource()));
  }

  @Test
  public void testIsLessThan() throws Exception {
    IntrinsicBuildableToDoubleFunction<T> toDoubleFunction = getFunction();
    assertThat(toDoubleFunction.isLessThan(0), is(instanceOf(Intrinsic.class)));
    assertFalse(toDoubleFunction.isLessThan(0).test(valueSource()));
    assertFalse(toDoubleFunction.isLessThan(-1).test(valueSource()));
    assertTrue(toDoubleFunction.isLessThan(1).test(valueSource()));
  }

  @Test
  public void testIsGreaterThanOrEqualTo() throws Exception {
    IntrinsicBuildableToDoubleFunction<T> toDoubleFunction = getFunction();
    assertThat(toDoubleFunction.isGreaterThanOrEqualTo(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toDoubleFunction.isGreaterThanOrEqualTo(0).test(valueSource()));
    assertTrue(toDoubleFunction.isGreaterThanOrEqualTo(-1).test(valueSource()));
    assertFalse(toDoubleFunction.isGreaterThanOrEqualTo(1).test(valueSource()));
  }

  @Test
  public void testIsLessThanOrEqualTo() throws Exception {
    IntrinsicBuildableToDoubleFunction<T> toDoubleFunction = getFunction();
    assertThat(toDoubleFunction.isLessThanOrEqualTo(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toDoubleFunction.isLessThanOrEqualTo(0).test(valueSource()));
    assertFalse(toDoubleFunction.isLessThanOrEqualTo(-1).test(valueSource()));
    assertTrue(toDoubleFunction.isLessThanOrEqualTo(1).test(valueSource()));
  }


  private interface TestFunction<T> extends IntrinsicBuildableToDoubleFunction<T> {

    @Override
    default IntrinsicType getIntrinsicType() {
      return null;
    }

    @Override
    default List<Intrinsic> incoming() {
      return null;
    }

    @Override
    default String toString(Function<Intrinsic, String> formatter) {
      return toString();
    }
  }
}