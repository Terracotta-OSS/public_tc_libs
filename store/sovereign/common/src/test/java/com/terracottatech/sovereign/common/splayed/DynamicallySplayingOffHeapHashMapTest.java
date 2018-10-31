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

package com.terracottatech.sovereign.common.splayed;

import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.common.utils.SequenceGenerator;
import com.terracottatech.tool.RateTimer;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.Factory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by cschanck on 3/1/2016.
 */
public class DynamicallySplayingOffHeapHashMapTest {

  @Test
  public void testManualSplaying() throws Exception {
    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
    SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(),
      new IntegerStorageEngine());

    DynamicallySplayingOffHeapHashMap<Integer, Integer> set = new DynamicallySplayingOffHeapHashMap<>((i) -> {
      return MiscUtils.hash32shiftmult(i);
    }, source, engine, 1024 * 1024);

    for (int i = 0; i < 10000; i = i + 100) {
      set.splayed.shardAt(0).put(i, i);
    }
    System.out.println(set.splayed);
    for (HalvableOffHeapHashMap<Integer, Integer> hs : set.splayed.shards()) {
      System.out.println(hs);
    }
    set.splayed.halveAndPossiblySpread(0);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(1);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(3);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(set.splayed.set.length - 1);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(7);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(7);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(3);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(1);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    set.splayed.halveAndPossiblySpread(0);
    System.out.println(set.splayed);
    verify(set.splayed, 0, 10000, 100);

    // none of these should null pointer out.
    set.getOccupiedOverheadMemory();
    set.getOccupiedDataSize();
    set.getReservedDataSize();
    set.getReservedOverheadMemory();
    set.getSize();
    set.isEmpty();
    set.clear();
    set.destroy();

  }

  private void verify(DynamicSplayableSet<HalvableOffHeapHashMap<Integer, Integer>> set, int from, int to, int skip) {
    for (int i = from; i < to; i = i + skip) {
      assertTrue(set.shardAt(set.shardIndexFor(i)).containsKey(i));
    }
  }

  @Test
  public void testAutoSplaying() throws Exception {
    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
    SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(),
      new IntegerStorageEngine());

    DynamicallySplayingOffHeapHashMap<Integer, Integer> set = new DynamicallySplayingOffHeapHashMap<>((i) -> {
      return MiscUtils.hash32shiftmult(i);
    }, source, engine, 1024);

    for (int i = 0; i < 1000000; i = i + 100) {
      set.put(i, i);
    }

    for (int i = 0; i < 1000000; i = i + 100) {
      assertThat(set.get(i).intValue(), is(i));
    }

    System.out.print(set.splayed.shards().size());
  }

  @Ignore
  @Test
  public void testLarge() {
    // this test exists to test to exhaustion. Useful only for testing hypothesis etc.
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(),
      new IntegerStorageEngine());
    DynamicallySplayingOffHeapHashMap<Integer, Integer> set = new DynamicallySplayingOffHeapHashMap<>((i) -> {
      return MiscUtils.hash32shiftmult(i);
    }, source, engine);
    SequenceGenerator gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    long cnt = 0;
    RateTimer rate = new RateTimer().start();
    try {
      while (true) {
        int ii = gen.next();
        rate.event(() -> {
          set.put(ii, ii);
        });
        if ((rate.getEventCount() % 1000000) == 0) {
          System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
        }
        cnt++;
      }
    } catch (Exception e) {
      System.out.println("Splayed count: " + set.splayed.shards().size());
      System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));
      System.out.println("Reached: " + cnt);
      e.printStackTrace();
    }
  }

  @Ignore
  @Test
  public void testSingleHashMapLarge() {
    // this test exists to test to exhaustion. Useful only for testing hypothesis etc.
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(),
      new IntegerStorageEngine());
    OffHeapHashMap<Integer, Integer> set = new OffHeapHashMap<>(source, engine);
    SequenceGenerator gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    long cnt = 0;
    RateTimer rate = new RateTimer().start();
    try {
      while (true) {
        int ii = gen.next();
        rate.event(() -> {
          set.put(ii, ii);
        });
        if ((rate.getEventCount() % 1000000) == 0) {
          System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
        }
        cnt++;
      }
    } catch (Exception e) {
      System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));
      System.out.println("Reached: " + cnt);
      e.printStackTrace();
    }
  }


  @Ignore
  @Test
  public void testConcurrentOffheapHashMapLarge() {
    // this test exists to test to exhaustion. Useful only for testing hypothesis etc.
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    Factory<StorageEngine<Integer, Integer>> factory = new Factory<StorageEngine<Integer, Integer>>() {
      @Override
      public StorageEngine<Integer, Integer> newInstance() {
        SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(
          new IntegerStorageEngine(), new IntegerStorageEngine());
        return engine;
      }
    };
    ConcurrentOffHeapHashMap<Integer, Integer> set = new ConcurrentOffHeapHashMap<Integer, Integer>(source, factory,
      128 * 128, 128);
    SequenceGenerator gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    long cnt = 0;
    RateTimer rate = new RateTimer().start();
    try {
      while (true) {
        int ii = gen.next();
        rate.event(() -> {
          set.put(ii, ii);
        });
        if ((rate.getEventCount() % 1000000) == 0) {
          System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
        }
        cnt++;
      }
    } catch (Exception e) {
      System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));
      System.out.println("Reached: " + cnt);
      e.printStackTrace();
    }
  }

  @Ignore
  @Test
  public void testGetLarge() {
    // this test exists to test to exhaustion. Useful only for testing hypothesis etc.
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(),
      new IntegerStorageEngine());
    DynamicallySplayingOffHeapHashMap<Integer, Integer> set = new DynamicallySplayingOffHeapHashMap<>((i) -> {
      return MiscUtils.hash32shiftmult(i);
    }, source, engine);
    SequenceGenerator gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    long cnt = 0;
    RateTimer rate = new RateTimer().start();

    final int LOAD_MAX = 300000000;
    while (cnt < LOAD_MAX) {
      final int finalCnt = (int) cnt;
      rate.event(() -> {
        set.put(finalCnt, finalCnt);
      });
      if ((rate.getEventCount() % 1000000) == 0) {
        System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
      }
      cnt++;
    }
    System.out.println("Loaded: "+cnt);
    System.out.println("Splayed count: " + set.splayed.shards().size());
    System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));

    cnt = 0;
    gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    Random r = new Random(0);
    rate.reset();
    while (cnt < 2 * LOAD_MAX) {
      int ii = r.nextInt(2 * LOAD_MAX);
      Integer got = rate.event(() -> {
        return set.get(ii);
      });
      if ((rate.getEventCount() % 1000000) == 0) {
        System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
      }
      cnt++;
    }
    System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));
  }

  @Ignore
  @Test
  public void testGetSingle() {
    // this test exists to test to exhaustion. Useful only for testing hypothesis etc.
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(),
      new IntegerStorageEngine());
    OffHeapHashMap<Integer, Integer> set = new OffHeapHashMap<>(source, engine);
    SequenceGenerator gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    long cnt = 0;
    RateTimer rate = new RateTimer().start();

    final int LOAD_MAX = 30000000;
    while (cnt < LOAD_MAX) {
      final int finalCnt = (int) cnt;
      rate.event(() -> {
        set.put(finalCnt, finalCnt);
      });
      if ((rate.getEventCount() % 1000000) == 0) {
        System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
      }
      cnt++;
    }
    System.out.println("Loaded: "+cnt);
    System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));

    cnt = 0;
    gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    Random r = new Random(0);
    rate.reset();
    while (cnt < 2 * LOAD_MAX) {
      int ii = r.nextInt(2 * LOAD_MAX);
      Integer got = rate.event(() -> set.get(ii));
      if ((rate.getEventCount() % 1000000) == 0) {
        System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
      }
      cnt++;
    }
    System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));
  }


  @Ignore
  @Test
  public void testGetConcurrentOHHM() {
    // this test exists to test to exhaustion. Useful only for testing hypothesis etc.
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    Factory<StorageEngine<Integer, Integer>> factory = new Factory<StorageEngine<Integer, Integer>>() {
      @Override
      public StorageEngine<Integer, Integer> newInstance() {
        SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(
          new IntegerStorageEngine(), new IntegerStorageEngine());
        return engine;
      }
    };
    ConcurrentOffHeapHashMap<Integer, Integer> set = new ConcurrentOffHeapHashMap<Integer, Integer>(source, factory,
      128 * 128, 128);
    SequenceGenerator gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    long cnt = 0;
    RateTimer rate = new RateTimer().start();

    final int LOAD_MAX = 250000000;
    while (cnt < LOAD_MAX) {
      final int finalCnt = (int) cnt;
      rate.event(() -> {
        set.put(finalCnt, finalCnt);
      });
      if ((rate.getEventCount() % 1000000) == 0) {
        System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
      }
      cnt++;
    }
    System.out.println("Loaded: "+cnt);
    System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));

    cnt = 0;
    gen = new SequenceGenerator(Integer.MAX_VALUE, true);
    Random r = new Random(0);
    rate.reset();
    while (cnt < 2 * LOAD_MAX) {
      int ii = r.nextInt(2 * LOAD_MAX);
      Integer got = rate.event(() -> set.get(ii));
      if ((rate.getEventCount() % 1000000) == 0) {
        System.out.println(rate.opsRateString(TimeUnit.MILLISECONDS));
      }
      cnt++;
    }
    System.out.println(rate.rateReport(TimeUnit.MILLISECONDS));
  }
}
