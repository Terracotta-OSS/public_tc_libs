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
 * Basic tests for {@link Multiply.Long}.
 */
public class MultiplyLongTest extends ArithmeticOperationLongTest {

  @Override
  protected IntrinsicBuildableToLongFunction<Record<?>> getFunction() {
    return super.getFunction().multiply(1);
  }

  @Override
  protected Record<String> valueSource() {
    return createRecord(0);
  }

  @Override
  protected BuildableToLongFunction<Record<?>> applyDegenerateCase(IntrinsicBuildableToLongFunction<Record<?>> inputFunction) {
    return Multiply.Long.multiply(inputFunction,1);
  }

  @Override
  protected BuildableToLongFunction<Record<?>> getTestObject() {
    return new Multiply.Long<>(getFunction(), 1);
  }

  @Override
  public void testApply() {
    long result = new Multiply.Long<>(getFunction(), 2)
            .apply(valueSource());
    assertEquals(0, result);
  }

  @Override
  public void testChain() {
    BuildableToLongFunction<Record<?>> chain = new Multiply.Long<>(getFunction(), 3).add(2);
    assertThat(chain, instanceOf(Add.Long.class));
    assertResult(chain.applyAsLong(createRecord(2)), 8L, "2 * 1 * 3 + 2");
  }

  @Test
  public void testObjectMethods() {
    Object add = new Multiply.Long<>(getFunction(), 0);
    assertEquals(add, add);
    assertEquals(add.hashCode(), add.hashCode());

    Object same = new Multiply.Long<>(getFunction(), 0);
    assertEquals(add, same);
    assertEquals(add.hashCode(), same.hashCode());

    Object otherValue = new Multiply.Long<>(getFunction(), 1);
    assertNotEquals(add, otherValue);

    Object otherFunction = new Multiply.Long<>(getFunction().multiply(0), 0);
    assertNotEquals(add, otherFunction);
  }
}
