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

import com.terracottatech.store.StoreException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DestroyableReferenceTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private DestroyableReference<Object> reference;
  private Object value;

  @Before
  public void before() {
    reference = new DestroyableReference<>();
    value = new Object();
  }

  @Test
  public void setValue() throws Exception {
    runThreads(() -> {
      Object o = reference.acquireValue();
      assertEquals(value, o);
      reference.releaseValue();
    });

    Optional<Object> valueToDestroy1 = reference.destroy();
    assertTrue(valueToDestroy1.isPresent());
    Object foundValue = valueToDestroy1.get();
    assertEquals(value, foundValue);

    Optional<Object> valueToDestroy2 = reference.destroy();
    assertFalse(valueToDestroy2.isPresent());
  }

  @Test
  public void destroyFailsIfOutstandingAcquires() throws Exception {
    runThreads(() -> {
      Object o = reference.acquireValue();
      assertEquals(value, o);
    });

    expectedException.expect(StoreException.class);
    expectedException.expectMessage("Cannot destroy, reference count: 1");

    reference.destroy();
  }

  @Test
  public void destroyFailsBeforeValueSet() throws Exception {
    expectedException.expect(StoreException.class);
    expectedException.expectMessage("Cannot destroy, reference count: 1");

    reference.destroy();
  }

  @Test
  public void destroyFailsWithMultipleOutstandingAcquires() throws Exception {
    runThreads(() -> {
      Object o1 = reference.acquireValue();
      assertEquals(value, o1);
      Object o2 = reference.acquireValue();
      assertEquals(value, o2);
      Object o3 = reference.acquireValue();
      assertEquals(value, o3);
      reference.releaseValue();
    });

    expectedException.expect(StoreException.class);
    expectedException.expectMessage("Cannot destroy, reference count: 2");

    reference.destroy();
  }

  private void runThreads(Runnable getRunnable) throws Exception {
    Thread getThread = new Thread(getRunnable);
    Thread setThread = new Thread(() -> reference.setValue(value));

    getThread.start();
    setThread.start();

    getThread.join();
    setThread.join();
  }
}
