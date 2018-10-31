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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * Basic tests for {@link Divide.Long}.
 */
public class DivideLongTest extends ArithmeticOperationLongTest {

  @Override
  protected IntrinsicBuildableToLongFunction<Record<?>> getFunction() {
    return super.getFunction().divide(1);
  }

  @Override
  protected Record<String> valueSource() {
    return createRecord(0L);
  }

  @Override
  protected BuildableToLongFunction<Record<?>> applyDegenerateCase(IntrinsicBuildableToLongFunction<Record<?>> inputFunction) {
    return Divide.Long.divide(inputFunction, 1);
  }

  @Override
  protected BuildableToLongFunction<Record<?>> getTestObject() {
    return new Divide.Long<>(getFunction(), 1);
  }

  @Override
  public void testApply() {
    long result = new Divide.Long<>(getFunction(), 2)
            .apply(valueSource());
    assertEquals(0, result);
  }

  @Override
  public void testChain() {
    BuildableToLongFunction<Record<?>> chain = new Divide.Long<>(getFunction(), 3).add(1);
    assertThat(chain, instanceOf(Add.Long.class));
    assertResult(chain.applyAsLong(createRecord(6)), 3, "6 / 1 / 3 + 1");
  }

  @Test(expected = ArithmeticException.class)
  public void testDivideByZero() {
    Divide.Long<Record<?>> zeroDivision = new Divide.Long<>(getFunction(), 0);
    zeroDivision.applyAsLong(createRecord(1));
  }

  @Test
  public void testObjectMethods() {
    Object add = new Divide.Long<>(getFunction(), 0);
    assertEquals(add, add);
    assertEquals(add.hashCode(), add.hashCode());

    Object same = new Divide.Long<>(getFunction(), 0);
    assertEquals(add, same);
    assertEquals(add.hashCode(), same.hashCode());

    Object otherValue = new Divide.Long<>(getFunction(), 1);
    assertNotEquals(add, otherValue);

    Object otherFunction = new Divide.Long<>(getFunction().divide(2), 0);
    assertNotEquals(add, otherFunction);
  }
}
