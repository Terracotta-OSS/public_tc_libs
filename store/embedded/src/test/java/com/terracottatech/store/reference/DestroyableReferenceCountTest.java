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
package com.terracottatech.store.reference;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DestroyableReferenceCountTest {
  @Test
  public void destroyBeforeUse() {
    DestroyableReferenceCount count = new DestroyableReferenceCount();
    assertDestroy(count, 0, true, true);
    assertDestroy(count, 0, true, false);
  }

  @Test
  public void destroyAfterSingleIncrement() {
    DestroyableReferenceCount count = new DestroyableReferenceCount();
    count.increment();
    assertDestroy(count, 1, false, false);
    assertDestroy(count, 1, false, false);
  }

  @Test
  public void destroyAfterIncrementAndDecrement() {
    DestroyableReferenceCount count = new DestroyableReferenceCount();
    count.increment();
    count.decrement();
    assertDestroy(count, 0, true, true);
    assertDestroy(count, 0, true, false);
  }

  @Test(expected = AssertionError.class)
  public void decrementBelowZero() {
    DestroyableReferenceCount count = new DestroyableReferenceCount();
    count.decrement();
  }

  @Test
  public void countCorrectly() {
    DestroyableReferenceCount count = new DestroyableReferenceCount();
    count.increment();
    count.increment();
    count.decrement();
    count.increment();
    count.decrement();
    count.increment();
    count.increment();
    assertDestroy(count, 3, false, false);
    assertDestroy(count, 3, false, false);
  }

  private void assertDestroy(DestroyableReferenceCount count, int value, boolean destroyed, boolean fresh) {
    DestructionResult result = count.destroy();
    assertEquals(value, result.getCount());
    assertEquals(destroyed, result.isDestroyed());
    assertEquals(fresh, result.isFresh());
  }
}
