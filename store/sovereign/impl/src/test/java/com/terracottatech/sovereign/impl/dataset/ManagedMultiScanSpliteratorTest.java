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
import com.terracottatech.store.Record;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Purpose is to test all the unit test cases that is already tested for ManagedSpliterator,
 * and record spliterator and just extend the multi scan for multi scan testing.
 *
 * See {@link PassThroughManagedSpliteratorTest} for additional test cases that wraps
 * pass through with {@link ManagedMultiScanSpliterator} spliterator.
 *
 * @author RKAV
 */
public class ManagedMultiScanSpliteratorTest extends ManagedSpliteratorTest {
  @Mock
  private VersionedRecord<String> record1;


  @Override
  @SuppressWarnings("unchecked")
  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
    super.initMocks();
  }
  @Override
  protected Spliterator<Record<String>> getSpliteratorUnderTest(PersistentMemoryLocator locator) {
    return new ManagedMultiScanSpliterator<>(null, this.container, locator, Long.MIN_VALUE, null, () -> null,
      mockAction);
  }

  @Override
  protected ManagedSpliterator<String> getManagedSpliteratorUnderTestWithManagedAction() {
    return new ManagedMultiScanSpliterator<>(null, this.container, null, Long.MIN_VALUE, null, () -> null,
      mockAction);
  }

  /**
   * Test of tryAdvance method, of class RecordSpliterator.
   */
  @Test
  public void testTryMultiScan() {
    PersistentMemoryLocator first = mock(PersistentMemoryLocator.class);
    PersistentMemoryLocator second = mock(PersistentMemoryLocator.class);
    when(first.next()).thenReturn(second);
    when(second.isEndpoint()).thenReturn(true);

    PersistentMemoryLocator third = mock(PersistentMemoryLocator.class);
    PersistentMemoryLocator fourth = mock(PersistentMemoryLocator.class);
    when(third.next()).thenReturn(fourth);
    when(fourth.isEndpoint()).thenReturn(true);

    when(container.first(mock(ContextImpl.class))).thenReturn(first);
    when(container.get(ArgumentMatchers.eq(first))).thenReturn(record);
    when(container.get(ArgumentMatchers.eq(third))).thenReturn(record1);

    List<IndexRange<String>> rangeList = new ArrayList<>();
    rangeList.add(new MockIndexRange(third));

    Spliterator<Record<String>> instance =
        new ManagedMultiScanSpliterator<>(null, this.container, first, Long.MIN_VALUE,
            null,
            () -> (rangeList.size() > 0) ? rangeList.remove(0) : null, mockAction);

    int count = 0;
    while (instance.tryAdvance(consumer)) {
      count++;
    }
    verify(consumer).accept(ArgumentMatchers.eq(record));
    verify(consumer).accept(ArgumentMatchers.eq(record1));
    Assert.assertEquals(2, count);
  }

  private static class MockIndexRange implements IndexRange<String> {
    private PersistentMemoryLocator locator;

    public MockIndexRange(PersistentMemoryLocator locator) {
      this.locator = locator;
    }

    @Override
    public PersistentMemoryLocator getLocator() {
      return locator;
    }

    @Override
    public RangePredicate<String> getEndpoint() {
      return (x) -> true;
    }
  }
}
