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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * Basic tests for {@link Add.Double}.
 */
public class AddDoubleTest extends ArithmeticOperationDoubleTest {

  @Override
  protected IntrinsicBuildableToDoubleFunction<Record<?>> getFunction() {
    return super.getFunction().add(1);
  }

  @Override
  protected Record<String> valueSource() {
    return createRecord(-1.0D);
  }

  @Override
  protected BuildableToDoubleFunction<Record<?>> applyDegenerateCase(IntrinsicBuildableToDoubleFunction<Record<?>> inputFunction) {
    return Add.Double.add(inputFunction, 0.0);
  }

  protected BuildableToDoubleFunction<Record<?>> getTestObject() {
    return new Add.Double<>(getFunction(), -1);
  }

  @Override
  public void testApply() {
    double result = new Add.Double<>(getFunction(), 0)
            .apply(valueSource());
    assertEquals(0, result, 0);
  }

  @Override
  public void testChain() {
    BuildableToDoubleFunction<Record<?>> chain = new Add.Double<>(getFunction(), 3).multiply(2);
    assertThat(chain, instanceOf(Multiply.Double.class));
    assertResult(chain.applyAsDouble(createRecord(2)), 12.0, "(2 + 1 + 3) * 2");
  }

  @Test
  public void testObjectMethods() {
    Object add = new Add.Double<>(getFunction(), 0);
    assertEquals(add, add);
    assertEquals(add.hashCode(), add.hashCode());

    Object same = new Add.Double<>(getFunction(), 0);
    assertEquals(add, same);
    assertEquals(add.hashCode(), same.hashCode());

    Object otherValue = new Add.Double<>(getFunction(), 1);
    assertNotEquals(add, otherValue);

    Object otherFunction = new Add.Double<>(super.getFunction(), 0);
    assertNotEquals(add, otherFunction);

    Object nanValue = new Add.Double<>(getFunction(), Double.NaN);
    assertNotEquals(add, nanValue);

    Object anotherNanValue = new Add.Double<>(getFunction(), Double.NaN);
    assertEquals(nanValue, anotherNanValue);
    assertEquals(nanValue.hashCode(), anotherNanValue.hashCode());
  }
}
