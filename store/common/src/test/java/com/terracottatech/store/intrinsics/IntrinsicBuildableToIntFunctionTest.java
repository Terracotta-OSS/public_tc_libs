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
import static org.junit.Assert.*;

/**
 * Tests {@link IntrinsicBuildableToIntFunction} methods.
 */
public class IntrinsicBuildableToIntFunctionTest<T> {

  protected IntrinsicBuildableToIntFunction<T> getFunction() {
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
    IntrinsicBuildableToIntFunction<T> toIntFunction = getFunction();
    assertThat(toIntFunction.is(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toIntFunction.is(0).test(valueSource()));
    assertFalse(toIntFunction.is(1).test(valueSource()));
  }

  @Test
  public void testIsGreaterThan() throws Exception {
    IntrinsicBuildableToIntFunction<T> toIntFunction = getFunction();
    assertThat(toIntFunction.isGreaterThan(0), is(instanceOf(Intrinsic.class)));
    assertFalse(toIntFunction.isGreaterThan(0).test(valueSource()));
    assertTrue(toIntFunction.isGreaterThan(-1).test(valueSource()));
    assertFalse(toIntFunction.isGreaterThan(1).test(valueSource()));
  }

  @Test
  public void testIsLessThan() throws Exception {
    IntrinsicBuildableToIntFunction<T> toIntFunction = getFunction();
    assertThat(toIntFunction.isLessThan(0), is(instanceOf(Intrinsic.class)));
    assertFalse(toIntFunction.isLessThan(0).test(valueSource()));
    assertFalse(toIntFunction.isLessThan(-1).test(valueSource()));
    assertTrue(toIntFunction.isLessThan(1).test(valueSource()));
  }

  @Test
  public void testIsGreaterThanOrEqualTo() throws Exception {
    IntrinsicBuildableToIntFunction<T> toIntFunction = getFunction();
    assertThat(toIntFunction.isGreaterThanOrEqualTo(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toIntFunction.isGreaterThanOrEqualTo(0).test(valueSource()));
    assertTrue(toIntFunction.isGreaterThanOrEqualTo(-1).test(valueSource()));
    assertFalse(toIntFunction.isGreaterThanOrEqualTo(1).test(valueSource()));
  }

  @Test
  public void testIsLessThanOrEqualTo() throws Exception {
    IntrinsicBuildableToIntFunction<T> toIntFunction = getFunction();
    assertThat(toIntFunction.isLessThanOrEqualTo(0), is(instanceOf(Intrinsic.class)));
    assertTrue(toIntFunction.isLessThanOrEqualTo(0).test(valueSource()));
    assertFalse(toIntFunction.isLessThanOrEqualTo(-1).test(valueSource()));
    assertTrue(toIntFunction.isLessThanOrEqualTo(1).test(valueSource()));
  }


  private interface TestFunction<T> extends IntrinsicBuildableToIntFunction<T> {

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