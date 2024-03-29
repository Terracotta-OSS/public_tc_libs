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
 * Tests {@link IntrinsicBuildableToLongFunction} methods.
 */
public class IntrinsicBuildableToLongFunctionTest<T> {

  protected IntrinsicBuildableToLongFunction<T> getFunction() {
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
    IntrinsicBuildableToLongFunction<T> toLongFunction = getFunction();
    assertThat(toLongFunction.is(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toLongFunction.is(0).test(valueSource()));
    assertFalse(toLongFunction.is(1).test(valueSource()));
  }

  @Test
  public void testIsGreaterThan() throws Exception {
    IntrinsicBuildableToLongFunction<T> toLongFunction = getFunction();
    assertThat(toLongFunction.isGreaterThan(0), is(instanceOf(Intrinsic.class)));
    assertFalse(toLongFunction.isGreaterThan(0).test(valueSource()));
    assertTrue(toLongFunction.isGreaterThan(-1).test(valueSource()));
    assertFalse(toLongFunction.isGreaterThan(1).test(valueSource()));
  }

  @Test
  public void testIsLessThan() throws Exception {
    IntrinsicBuildableToLongFunction<T> toLongFunction = getFunction();
    assertThat(toLongFunction.isLessThan(0), is(instanceOf(Intrinsic.class)));
    assertFalse(toLongFunction.isLessThan(0).test(valueSource()));
    assertFalse(toLongFunction.isLessThan(-1).test(valueSource()));
    assertTrue(toLongFunction.isLessThan(1).test(valueSource()));
  }

  @Test
  public void testIsGreaterThanOrEqualTo() throws Exception {
    IntrinsicBuildableToLongFunction<T> toLongFunction = getFunction();
    assertThat(toLongFunction.isGreaterThanOrEqualTo(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toLongFunction.isGreaterThanOrEqualTo(0).test(valueSource()));
    assertTrue(toLongFunction.isGreaterThanOrEqualTo(-1).test(valueSource()));
    assertFalse(toLongFunction.isGreaterThanOrEqualTo(1).test(valueSource()));
  }

  @Test
  public void testIsLessThanOrEqualTo() throws Exception {
    IntrinsicBuildableToLongFunction<T> toLongFunction = getFunction();
    assertThat(toLongFunction.isLessThanOrEqualTo(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toLongFunction.isLessThanOrEqualTo(0).test(valueSource()));
    assertFalse(toLongFunction.isLessThanOrEqualTo(-1).test(valueSource()));
    assertTrue(toLongFunction.isLessThanOrEqualTo(1).test(valueSource()));
  }


  private interface TestFunction<T> extends IntrinsicBuildableToLongFunction<T> {

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