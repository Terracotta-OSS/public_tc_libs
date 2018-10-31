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

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.model.SovereignValueGenerator;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.Type;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests impact of {@link BtreeIndexMap#drop()} on public {@link BtreeIndexMap} methods.
 *
 * @author Clifford W. Johnson
 */
public class BtreeIndexMapDropTest {


  @Mock
  private SovereignValueGenerator generator;

  @Mock
  private ContextImpl context;

  private BtreeIndexMap<Long, Integer> btreeMap;

  private PageSourceLocation pageSource = PageSourceLocation.offheap();
  private SovereignRuntime<?> runtime;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.runtime=new SovereignRuntime<>(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class), new
      CachingSequence());
    this.btreeMap = new BtreeIndexMap<>(runtime, "test", 0, Long.class, this.pageSource);
  }

  @Test
  public void testDrop() throws Exception {
    final Field treeField = BtreeIndexMap.class.getDeclaredField("tree");
    treeField.setAccessible(true);

    final ABPlusTree<?> tree = (ABPlusTree)treeField.get(this.btreeMap);
    final ABPlusTree<?> treeSpy = spy(tree);
    treeField.set(this.btreeMap, treeSpy);

    this.btreeMap.drop();

    verify(treeSpy).close();

    // The ValueGenerator may be shared -- no calls from drop()
    verifyZeroInteractions(this.generator);
  }

  @Test
  public void testDroppedDrop() throws Exception {
    this.btreeMap.drop();
    this.btreeMap.drop();   // Second should not fail
  }

  @Test
  public void testDroppedEstimateSize() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.estimateSize();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDroppedFirst() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.first(this.context);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedGet() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.get(this.context, 1L);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedHigher() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.higher(this.context, 1L);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedHigherEqual() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.get(this.context, 1L);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedLast() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.last(this.context);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedLower() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.lower(this.context, 1L);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedLowerEqual() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.lowerEqual(this.context, 1L);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
  }

  @Test
  public void testDroppedPut() throws Exception {
    this.btreeMap.drop();

    PersistentMemoryLocator locator = mock(PersistentMemoryLocator.class);
    try {
      this.btreeMap.put(1, this.context, 1L, locator);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
    verifyZeroInteractions(locator);
  }

  @Test
  public void testDroppedRemove() throws Exception {
    this.btreeMap.drop();

    PersistentMemoryLocator locator = mock(PersistentMemoryLocator.class);
    try {
      this.btreeMap.remove(1, this.context, 1L, locator);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.context);
    verifyZeroInteractions(locator);
  }

  @Test
  public void testDroppedSize() throws Exception {
    this.btreeMap.drop();

    try {
      this.btreeMap.estimateSize();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDroppedToString() throws Exception {
    this.btreeMap.drop();

    assertNotNull(this.btreeMap.toString());
  }

}
