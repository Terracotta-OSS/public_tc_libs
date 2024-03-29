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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * Basic tests for {@link Divide.Int}.
 */
public class DivideIntTest extends ArithmeticOperationIntTest {

  @Override
  protected IntrinsicBuildableToIntFunction<Record<?>> getFunction() {
    return super.getFunction().divide(1);
  }

  @Override
  protected Record<String> valueSource() {
    return createRecord(0);
  }

  @Override
  protected BuildableToIntFunction<Record<?>> applyDegenerateCase(IntrinsicBuildableToIntFunction<Record<?>> inputFunction) {
    return Divide.Int.divide(inputFunction,1);
  }

  @Override
  protected BuildableToIntFunction<Record<?>> getTestObject() {
    return new Divide.Int<>(getFunction(), 1);
  }

  @Override
  public void testApply() {
    int result = new Divide.Int<>(getFunction(), 2)
            .apply(valueSource());
    assertEquals(0, result);
  }

  @Override
  public void testChain() {
    BuildableToIntFunction<Record<?>> chain = new Divide.Int<>(getFunction(), 3).add(1);
    assertThat(chain, instanceOf(Add.Int.class));
    assertResult(chain.applyAsInt(createRecord(6)), 3, "6 / 1 / 3 + 1");
  }

  @Test(expected = ArithmeticException.class)
  public void testDivideByZero() {
    Divide.Int<Record<?>> zeroDivision = new Divide.Int<>(getFunction(), 0);
    zeroDivision.applyAsInt(createRecord(1));
  }

  @Test
  public void testObjectMethods() {
    Object add = new Divide.Int<>(getFunction(), 0);
    assertEquals(add, add);
    assertEquals(add.hashCode(), add.hashCode());

    Object same = new Divide.Int<>(getFunction(), 0);
    assertEquals(add, same);
    assertEquals(add.hashCode(), same.hashCode());

    Object otherValue = new Divide.Int<>(getFunction(), 1);
    assertNotEquals(add, otherValue);

    Object otherFunction = new Divide.Int<>(getFunction().divide(2), 0);
    assertNotEquals(add, otherFunction);
  }
}
