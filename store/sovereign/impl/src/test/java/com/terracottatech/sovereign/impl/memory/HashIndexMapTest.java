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

package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.model.SovereignValueGenerator;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.Type;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Clifford W. Johnson
 */
public class HashIndexMapTest {

  @Mock
  private SovereignValueGenerator valueGenerator;

  @Mock
  private ContextImpl context;

  private SovereignRuntime<String> runtime = new SovereignRuntime<>(
    new SovereignDataSetConfig<>(Type.STRING, SystemTimeReference.class), new CachingSequence());


  private HashIndexMap<String> hashIndexMap =
      new HashIndexMap<>(runtime, 0, String.class,
          new UnlimitedPageSource(new HeapBufferSource()));

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testDrop() throws Exception {
    final Field backingField = HashIndexMap.class.getDeclaredField("backing");
    backingField.setAccessible(true);
    final Field engineField = HashIndexMap.class.getDeclaredField("engine");
    engineField.setAccessible(true);

    this.hashIndexMap.drop();

    assertNull(backingField.get(this.hashIndexMap));
    assertNull(engineField.get(this.hashIndexMap));
  }

  @Test
  public void testDroppedDrop() throws Exception {
    this.hashIndexMap.drop();
    this.hashIndexMap.drop();   // Second drop should not fail
  }

  @Test
  public void testDroppedEstimateSize() throws Exception {
    this.hashIndexMap.drop();

    try {
      this.hashIndexMap.estimateSize();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDroppedGet() throws Exception {
    this.hashIndexMap.drop();

    try {
      this.hashIndexMap.get(this.context, "echidna");
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedGetCapacity() throws Exception {
    this.hashIndexMap.drop();

    try {
      this.hashIndexMap.getCapacity();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDroppedGetReserved() throws Exception {
    this.hashIndexMap.drop();

    try {
      this.hashIndexMap.getReserved();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDroppedGetUsed() throws Exception {
    this.hashIndexMap.drop();

    try {
      this.hashIndexMap.getUsed();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDroppedPut() throws Exception {
    this.hashIndexMap.drop();

    final PersistentMemoryLocator locator = mock(PersistentMemoryLocator.class);
    try {
      this.hashIndexMap.put(this.context, "echidna", locator);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
    verifyZeroInteractions(locator);
  }

  @Test
  public void testDroppedRemove() throws Exception {
    this.hashIndexMap.drop();

    final PersistentMemoryLocator locator = mock(PersistentMemoryLocator.class);
    try {
      this.hashIndexMap.remove(this.context, "echidna", locator);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
    verifyZeroInteractions(locator);
  }

  @Test
  public void testDroppedToString() throws Exception {
    this.hashIndexMap.drop();
    assertNotNull(this.hashIndexMap.toString());
  }
}
