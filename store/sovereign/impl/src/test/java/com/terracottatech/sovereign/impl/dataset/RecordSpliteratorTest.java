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

package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.store.Record;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Spliterator;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author mscott
 * @author Clifford W. Johnson
 */
public class RecordSpliteratorTest {

  @Mock
  protected SovereignContainer<String> container;

  @Mock
  protected PersistentMemoryLocator locator;

  @Mock
  protected VersionedRecord<String> record;

  @Mock
  protected Consumer<Record<String>> consumer;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  protected Spliterator<Record<String>> getSpliteratorUnderTest(PersistentMemoryLocator locator) {
    return new RecordSpliterator<>(null,
        locator, container, Long.MIN_VALUE);
  }

  /**
   * Test of tryAdvance method, of class RecordSpliterator.
   */
  @Test
  public void testTryAdvance() {
    PersistentMemoryLocator first = mock(PersistentMemoryLocator.class);
    PersistentMemoryLocator last = mock(PersistentMemoryLocator.class);
    when(first.next()).thenReturn(last);
    when(last.isEndpoint()).thenReturn(true);

    when(container.first(mock(ContextImpl.class))).thenReturn(first);
    when(container.get(ArgumentMatchers.eq(first))).thenReturn(record);
    Spliterator<Record<String>> instance = getSpliteratorUnderTest(first);
    instance.tryAdvance(consumer);

    verify(consumer).accept(ArgumentMatchers.eq(record));
  }

  /**
   * Test of trySplit method, of class RecordSpliterator.
   */
  @Test
  public void testTrySplit() {
    Spliterator<Record<String>> instance = getSpliteratorUnderTest(this.locator);
    Spliterator<?> result = instance.trySplit();
    assertNull(result);
  }

  /**
   * Test of characteristics method, of class RecordSpliterator.
   */
  @Test
  public void testCharacteristics() {
    Spliterator<Record<String>> instance = getSpliteratorUnderTest(this.locator);
    int expResult = 0;
    int result = instance.characteristics();
    assertEquals(expResult, result);
  }

  /**
   * Test of estimateSize method, of class RecordSpliterator.
   */
  @Test
  public void testEstimateSize() {
    Spliterator<Record<String>> instance = getSpliteratorUnderTest(this.locator);
    long expResult = Long.MAX_VALUE;
    long result = instance.estimateSize();
    assertEquals(expResult, result);
  }

  @Test
  public void testDisposedCharacteristics() throws Exception {
    final Spliterator<Record<String>> spliterator = getSpliteratorUnderTest(this.locator);

    when(this.container.isDisposed()).thenReturn(true);
    spliterator.characteristics();
  }

  @Test
  public void testDisposedTryAdvance() throws Exception {
    final Spliterator<Record<String>> spliterator = getSpliteratorUnderTest(this.locator);

    when(this.container.isDisposed()).thenReturn(true);
    try {
      spliterator.tryAdvance(r -> {});
      fail();
    } catch (Exception e) {
      // Expected
    }
  }
}
