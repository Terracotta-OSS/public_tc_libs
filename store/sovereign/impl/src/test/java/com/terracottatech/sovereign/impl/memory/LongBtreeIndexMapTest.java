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

import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.spi.store.Locator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Random;
import java.util.function.Supplier;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author mscott
 */
public class LongBtreeIndexMapTest extends BtreeIndexMapTest<Long> {

  public LongBtreeIndexMapTest() {
  }

  @Override
  public Class<Long> keyType() {
    return Long.class;
  }

  @Test
  public void testAdd() {
    super.add(new Long[]{30L, 1L, 25L});
  }

  @Test
  public void testdups() throws Exception {
    super.internalTestDups(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, -1L, -10L});
  }

  @Test
  public void testProperDeletion()  throws Exception {
    super.testDeletion(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, -1L, -10L});
  }

  @Test
  public void testmiddle() throws Exception {
    super.middlecount(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 44L, 44L, 44L, 44L, 44L, 44L, 44L, 44L, -1111L, -101L}, 44L);
  }


  @Test
  public void testhigher() throws Exception {
//  test in the middle of dups
    super.higher(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, 5L, 4);
//  test past the end
    super.higher(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, 47L, 0);
//  test past the beginning
    super.higher(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, Long.MIN_VALUE, 12);
//  test between keys
    super.higher(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, 6L, 4);
  }

  @Test
  public void testlower() throws Exception {
    super.lower(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, 44L, 8);
//  test past the end
    super.lower(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, Long.MIN_VALUE, 0);
//  test past the beginning
    super.lower(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, Long.MAX_VALUE, 12);
//  test between keys
    super.lower(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, 44L, 44L, 45L, 46L, -1111L, -101L}, 6L, 8);
  }

  @Test
  public void testFirst() throws Exception {
    ContextImpl context = Mockito.spy(new ContextImpl(null, false));
    Locator loc = map.first(context);
    assertTrue(loc.isEndpoint());
    super.add(new Long[]{1L, 2L, 3L, 5L, 5L, 5L, -1L, -10L});
    loc = map.first(context);
    Assert.assertFalse(loc.isEndpoint());
    Assert.assertEquals(8, ((PersistentMemoryLocator)loc).index());
    int count = 0;
    while (!loc.isEndpoint()) {
      count++;
      loc = loc.next();
    }
    Assert.assertThat(count, is(8));
  }

  private String stats(SovereignSortedIndexMap<?, ?> map) {
    return "Size=" + map.estimateSize() + " Allocated: " + MiscUtils.bytesAsNiceString(map.getAllocatedStorageSize()) + " Occupied: " + MiscUtils
      .bytesAsNiceString(map.getOccupiedStorageSize());
  }

  @Test
  @Ignore("Does not complete, eyeball test for now")
  public void testAppendDeleteFIFO() throws Exception {
    final int MAXSIZE=100000;
    final int STEP=10;
    long val = 0;
    System.out.println("Loading");
    System.out.println(stats(map));
    while (map.estimateSize() < MAXSIZE) {
//      map.put(context, val, new PersistentMemoryLocator(val, null));
      if(val%10000 ==0) {
        System.out.println(val + ": " + stats(map));
      }
      val++;
    }
    System.out.println(stats(map));
    for (; ; ) {
      int deletes = 0;
      for(int i=0;i<STEP;i++) {
//        map.put(context, val, new PersistentMemoryLocator(val, null));
        val++;
      }
      while (map.estimateSize() >= MAXSIZE) {
        ContextImpl c = new ContextImpl(null, false);
        BtreeIndexMap.BtreePersistentMemoryLocator<?> got = (BtreeIndexMap.BtreePersistentMemoryLocator<?>) map.first(c);
        Object k = got.getKey();
        c.close();
        c = new ContextImpl(null, false);
//        map.remove(c, (Long) k, got);
        c.close();
        deletes++;
      }
      if (val % 100 == 0) {
        System.out.println("Deletes: " + deletes + " stats " + stats(map));
      }
    }
  }

  @Test
  public void testBatch() throws Exception {

    Supplier<Long> sup=new Supplier<Long>() {
      Random r=new Random();
      @Override
      public Long get() {
        return Long.valueOf(r.nextInt());
      }
    };
    testBatchIngestion(100000, sup);
  }

}
