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
import com.terracottatech.sovereign.impl.memory.storageengines.PrimitivePortability;
import com.terracottatech.sovereign.impl.memory.storageengines.PrimitivePortabilityImpl;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.spi.IndexMapConfig;
import com.terracottatech.sovereign.spi.Space;
import com.terracottatech.sovereign.spi.store.DataContainer;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * @author mscott
 */
public abstract class BtreeIndexMapTest<K extends Comparable<K>> {

  Space<ContextImpl, ?> space;
  SovereignSortedIndexMap<K, Integer> map;
  MemoryAddressList olist = mock(MemoryAddressList.class);
  MemoryRecordContainer<?> container = mock(MemoryRecordContainer.class);

  private SovereignDataSetConfig<Integer, SystemTimeReference> config;
  private HashSet<Closeable> closables;
  protected ContextImpl context;

  public BtreeIndexMapTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  public abstract Class<K> keyType();

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {

    config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).resourceSize(
      SovereignDataSetConfig.StorageType.OFFHEAP, 128 * 1024 * 1024).versionLimitStrategy(() -> {
      return SystemTimeReference.class;
    }).freeze();

    SovereignRuntime<?> r = new SovereignRuntime<>(config, new CachingSequence());
    space = new MemorySpace(r);
    when(olist.get(ArgumentMatchers.anyInt())).then(new Answer<Integer>() {

      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        return ((Number) invocation.getArguments()[0]).intValue();
      }

    });
    when(olist.contains(ArgumentMatchers.anyInt())).thenReturn(Boolean.TRUE);
    when(container.createLocator(ArgumentMatchers.anyLong(), ArgumentMatchers.any())).then(new Answer<Locator>() {

      @Override
      public Locator answer(InvocationOnMock invocation) throws Throwable {
        return new PersistentMemoryLocator(((Long) invocation.getArguments()[0]).intValue(),
          (MemoryLocatorFactory) invocation.getArguments()[1]);
      }

    });
    when(container.mapLocator(ArgumentMatchers.any(PersistentMemoryLocator.class))).then(new Answer<Long>() {

      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return ((PersistentMemoryLocator) invocation.getArguments()[0]).index();
      }

    });

    map = (SovereignSortedIndexMap<K, Integer>) createSortedMap();
    context = Mockito.spy(new ContextImpl(null, false));

  }

  @After
  public void tearDown() {
    context.close();
    space.drop();
  }

  @SuppressWarnings("unchecked")
  public SovereignSortedIndexMap<K, ?> createSortedMap() {
    return (SovereignSortedIndexMap<K, ?>) space.createMap("test", new IndexMapConfig<K>() {
      @Override
      public boolean isSortedMap() {
        return true;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Type<K> getType() {
        return Type.forJdkType(keyType());
      }

      @Override
      public DataContainer<?, ?, ?> getContainer() {
        return container;
      }

    });
  }

  /**
   * Test of get method, of class BtreeIndexMap.
   */
  public void add(K[] list) {
    int slot = 1;
    AtomicInteger val = new AtomicInteger(0);
    for (K name : list) {
      map.put(val.incrementAndGet(), context, name, new PersistentMemoryLocator(slot++, null));
    }
    for (K name : list) {
      assertEquals(name, list[((int) (map.get(context, name).index() - 1))]);
    }
    PrimitivePortabilityImpl<K> port = PrimitivePortabilityImpl.factory(keyType());
    K[] sorted = Arrays.copyOf(list, list.length);
    Arrays.sort(sorted, port);
    K last = sorted[0];
    Locator loc = map.get(context, last);
    while (!loc.isEndpoint()) {
      K next = list[((int) (((PersistentMemoryLocator) loc).index() - 1))];
      Assert.assertTrue(port.compare(last, next) <= 0);
      last = next;
      loc = loc.next();
    }
    loc = map.get(context, last);
    while (!loc.isEndpoint()) {
      K next = list[((int) (((PersistentMemoryLocator) loc).index() - 1))];
      Assert.assertTrue(port.compare(last, next) >= 0);
      last = next;
      loc = loc.next();
    }
    last = sorted[0];
    loc = map.first(context);
    while (!loc.isEndpoint()) {
      K next = list[((int) (((PersistentMemoryLocator) loc).index() - 1))];
      Assert.assertTrue(port.compare(last, next) <= 0);
      last = next;
      loc = loc.next();
    }
  }

  /**
   * Test of get method, of class BtreeIndexMap.
   */
  public void internalTestDups(K[] list) throws Exception {
    ArrayList<K> cap = new ArrayList<>();
    AtomicInteger val = new AtomicInteger(0);
    for (K item : list) {
      cap.add(item);
      map.put(val.incrementAndGet(), context, item, new PersistentMemoryLocator(cap.size(), null));
    }
    PrimitivePortabilityImpl<K> port = PrimitivePortabilityImpl.factory(keyType());
    K[] sorted = Arrays.copyOf(list, list.length);
    Arrays.sort(sorted, port);
    K last = sorted[0];
    Locator loc = map.first(context);
    while (!loc.isEndpoint()) {
      K next = cap.get((int) (((PersistentMemoryLocator) loc).index() - 1));
      Assert.assertTrue(port.compare(next, last) >= 0);
      last = next;
      loc = loc.next();
    }
    verify(context, times(map.runtime().getConfig().getConcurrency())).addCloseable(ArgumentMatchers.any());
    System.out.println(map);
  }

  /**
   * Test of get method, of class BtreeIndexMap.
   */
  public void middlecount(K[] list, K middle) throws Exception {
    ArrayList<K> cap = new ArrayList<>();
    AtomicInteger val = new AtomicInteger(0);
    for (K item : list) {
      cap.add(item);
      map.put(val.incrementAndGet(), context, item, new PersistentMemoryLocator(cap.size(), null));
    }
    int count = 0;
    PrimitivePortability<K> port = PrimitivePortabilityImpl.factory(keyType());
//    K[] sorted = Arrays.copyOf(list, list.length);
//    Arrays.sort(sorted, port);
    Locator loc = map.get(context, middle);
    while (!loc.isEndpoint()) {
      K next = cap.get((int) (((PersistentMemoryLocator) loc).index() - 1));
      if (middle.equals(next)) {
        count++;
      } else {
        break;
      }
      loc = loc.next();
    }
    Assert.assertEquals(10, count);
    verify(context, times(map.runtime().getConfig().getConcurrency())).addCloseable(ArgumentMatchers.any());
    System.out.println(map);
  }

  public void higher(K[] list, K middle, int expected) throws Exception {
    ArrayList<K> cap = new ArrayList<>();
    for (K item : list) {
      cap.add(item);
      map.put(cap.size(), context, item, new PersistentMemoryLocator(cap.size(), null));
    }
    int count = 0;
    PrimitivePortability<K> port = PrimitivePortabilityImpl.factory(keyType());

    Locator loc = map.higher(context, middle);
    while (!loc.isEndpoint()) {
      K next = cap.get((int) (((PersistentMemoryLocator) loc).index() - 1));
      if (port.compare(next, middle) > 0) {
        count++;
      } else {
        break;
      }
      loc = loc.next();
    }
    Assert.assertEquals(expected, count);
    verify(context, atLeastOnce()).addCloseable(ArgumentMatchers.any());
    for (int x = 0; x < cap.size(); x++) {
      map.remove(x + 1, context, cap.get(x), new PersistentMemoryLocator(x + 1, null));
    }
    Assert.assertTrue(map.first(context).isEndpoint());
  }

  public void lower(K[] list, K middle, int expected) throws Exception {
    ArrayList<K> cap = new ArrayList<>();
    for (K item : list) {
      cap.add(item);
      map.put(cap.size(), context, item, new PersistentMemoryLocator(cap.size(), null));
    }
    int count = 0;
    PrimitivePortability<K> port = PrimitivePortabilityImpl.factory(keyType());

    Locator loc = map.lower(context, middle);
    while (!loc.isEndpoint()) {
      K next = cap.get((int) (((PersistentMemoryLocator) loc).index() - 1));
      if (port.compare(next, middle) < 0) {
        count++;
      } else {
        break;
      }
      loc = loc.next();
    }
    Assert.assertEquals(expected, count);
    verify(context, atLeastOnce()).addCloseable(ArgumentMatchers.any());
    for (int x = 0; x < cap.size(); x++) {
      map.remove(x + 1, context, cap.get(x), new PersistentMemoryLocator(x + 1, null));
    }
    Assert.assertTrue(map.first(context).isEndpoint());
  }

  public void testDeletion(K[] list) throws Exception {
    ArrayList<K> cap = new ArrayList<>();
    for (K item : list) {
      cap.add(item);
      map.put(cap.size(), context, item, new PersistentMemoryLocator(cap.size(), null));
    }
    PrimitivePortabilityImpl<K> port = PrimitivePortabilityImpl.factory(keyType());
    K[] sorted = Arrays.copyOf(list, list.length);
    Arrays.sort(sorted, port);
    K last = sorted[0];
    Locator loc = map.first(context);
    while (!loc.isEndpoint()) {
      K next = cap.get((int) (((PersistentMemoryLocator) loc).index() - 1));
      Assert.assertTrue(port.compare(next, last) >= 0);
      last = next;
      loc = loc.next();
    }

    int invokes = 1;
    AtomicInteger val = new AtomicInteger(0);
    for (K item : list) {
      PersistentMemoryLocator remove = map.get(context, item);
      map.remove((int) remove.index(), context, item, remove);
      invokes += 1;
    }

    Assert.assertThat(0L, is(((SovereignSortedIndexMap) map).estimateSize()));
    verify(context, times(invokes*map.runtime().getConfig().getConcurrency())).addCloseable(ArgumentMatchers.any());

  }

  void testBatchIngestion(int many, Supplier<K> supplier) throws Exception {
    SovereignSortedIndexMap.BatchHandle<K> batcher = map.batch();
    long slot = 0;
    Map<Long, K> lookup = new HashMap<>();
    for (K next = supplier.get(); slot < many; next = supplier.get()) {
      lookup.put(slot, next);
      batcher.batchAdd(next, next, new PersistentMemoryLocator(slot++, null));
    }
    batcher.process();
    batcher.close();
    // verify
    K last = null;
    int cnt = 0;
    PersistentMemoryLocator loc = map.first(context);
    while (!loc.isEndpoint()) {
      cnt++;
      K k = lookup.get(loc.index());
      //System.out.println("Last==" + last + "this=" + k + " cnt=" + cnt);
      if (last != null) {
        Assert.assertThat(last.compareTo(k), lessThanOrEqualTo(0));
      }
      last = k;
      loc=loc.next();
    }
    Assert.assertThat(lookup.size(), is(cnt));
  }
}
