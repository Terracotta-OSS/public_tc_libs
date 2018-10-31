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
package com.terracottatech.ehcache.disk;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.ehcache.disk.config.builders.EnterpriseDiskResourcePoolBuilder;
import com.terracottatech.ehcache.common.frs.FrsCodecUtils;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test concurrency of standalone FRS based caches.
 *
 * @author RKAV
 */
public class FastRestartStoreConcurrencyTest {
  private static final int TOTAL_ITERATIONS = 3;
  private static final int NUM_CACHES = 4;
  private static final int CONCURRENCY_LEVEL = 16;
  private static final int MAX_ELEMENTS_IN_CACHE = 5000;
  private static final int MAX_CACHE_SIZE_MB = 10;
  private static final int MAX_TEST_TIME_IN_MINUTES = 2;

  private static final Class<?>[] classArr = {BigObject.class, BigObject1.class,
      BigObject2.class, BigObject3.class};

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();
  private File currentFolder;

  @Before
  public void setup() throws Exception {
    currentFolder = tmpFolder.newFolder();
  }

  @Ignore
  @Test
  public void testConcurrency() throws Exception {
    {
      final PersistentCacheManager cacheManagerUnderTest = buildCacheManager();
      @SuppressWarnings("unchecked")
      final Cache<Long, BigObject>[] caches =
          (Cache<Long, BigObject>[]) Array.newInstance(Cache.class, NUM_CACHES);

      for (int i = 0; i < NUM_CACHES; i++) {
        @SuppressWarnings("unchecked")
        final Cache<Long, BigObject> thisCache = (Cache<Long, BigObject>)
            cacheManagerUnderTest.getCache("test-cache-" + i, Long.class, classArr[i % 4]);
        caches[i] = thisCache;
      }
      ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENCY_LEVEL);
      for (int i = 0; i < TOTAL_ITERATIONS; i++) {
        for (int k = 0; k < NUM_CACHES; k++) {
          @SuppressWarnings("unchecked")
          PutsGets<? super BigObject> putsGetsTask = new PutsGets<>(caches[k], k);
          executorService.execute(putsGetsTask);
        }
      }
      executorService.shutdown();
      final boolean notTimedOut = executorService.awaitTermination(MAX_TEST_TIME_IN_MINUTES, TimeUnit.MINUTES);
      cacheManagerUnderTest.close();
      // do not continue if the test had timedout
      assertTrue(notTimedOut);
    }
    {
      final PersistentCacheManager cacheManagerUnderTest = buildCacheManager();
      final Cache<Long, BigObject> cache0 = cacheManagerUnderTest.getCache("test-cache-0", Long.class, BigObject.class);
      final Cache<Long, BigObject2> cache2 = cacheManagerUnderTest.getCache("test-cache-2", Long.class, BigObject2.class);
      assertThat(cache2.get(MAX_ELEMENTS_IN_CACHE-1L).aDouble, is(FrsCodecUtils.DOUBLE_SIZE * 1.88181d));
      assertThat(cache0.get(MAX_ELEMENTS_IN_CACHE-1L).getIndex(), is(MAX_ELEMENTS_IN_CACHE-1));
      cacheManagerUnderTest.close();
    }
  }

  private PersistentCacheManager buildCacheManager() {
    CacheManagerBuilder<PersistentCacheManager> frsCacheManagerBuilder = newCacheManagerBuilder().
        with(CacheManagerBuilder.persistence(currentFolder.getAbsolutePath()));
    for (int i = 0; i < NUM_CACHES; i++) {
      final ResourcePoolsBuilder resourcePoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(EnterpriseDiskResourcePoolBuilder.diskRestartable(MAX_CACHE_SIZE_MB, MemoryUnit.MB));
      frsCacheManagerBuilder = frsCacheManagerBuilder.withCache("test-cache-" + i,
          newCacheConfigurationBuilder(Long.class, classArr[i % 4], resourcePoolsBuilder));
    }
    return frsCacheManagerBuilder.build(true);
  }

  private static final class PutsGets<V extends BigObject> implements Runnable {
    private final Cache<Long, V> cacheUnderTest;
    private final int cacheNo;

    private PutsGets(Cache<Long, V> testCache, int cacheNo) {
      this.cacheUnderTest = testCache;
      this.cacheNo = cacheNo;
    }

    @Override
    public void run() {
      doPuts(cacheUnderTest);
      doGets(cacheUnderTest);
      doGets(cacheUnderTest);
      doGets(cacheUnderTest);
    }

    private void doPuts(Cache<Long, V> cache) {
      for (int i = 0; i < MAX_ELEMENTS_IN_CACHE; i++) {
        V v = generateValue(i, cache);
        long key = v.getIndex();
        cache.put(key, v);
      }
    }

    @SuppressWarnings("unchecked")
    private V generateValue(int idx, Cache<Long, V> cache) {
      BigObject dependent = cache.get((long)new Random().nextInt(MAX_ELEMENTS_IN_CACHE/4));
      switch (cacheNo) {
        case 0:
          return (V)new BigObject(idx, dependent);
        case 1:
          return (V)new BigObject1(idx, dependent);
        case 2:
          return (V)new BigObject2(idx, dependent);
        case 3:
        default:
          return (V)new BigObject3(idx, dependent);
      }
    }

    private void doGets(Cache<Long, V> cache) {
      for (int i = 0; i < MAX_ELEMENTS_IN_CACHE; i++) {
        cache.get(i+1L);
      }
    }
  }

  private static class BigObject implements Serializable {
    private static final long serialVersionUID = 4235373809825448938L;
    private static final AtomicInteger intCounter = new AtomicInteger(0);
    private static final AtomicLong longCounter = new AtomicLong(0L);
    private final int idx;
    private final int test1;
    private final Integer test2;
    private final Long test3;
    private final Double test4;
    private final String bigString;
    private final BigObject another;
    private final int[] largeIntArray;

    BigObject(int idx, BigObject another) {
      this.idx = idx;
      test1 = intCounter.addAndGet(222);
      test2 = intCounter.addAndGet(22);
      test3 = longCounter.addAndGet(1L);
      test4 = longCounter.doubleValue();
      char[] ch = new char[1024];
      for (int i = 0; i < 1024; i++) {
        ch[i] = (char)('a' + i % 25);
      }
      bigString = new String(ch);
      this.another = another;
      largeIntArray = new int[1024];
      for (int i = 0; i < 1024; i++) {
        largeIntArray[i] = intCounter.incrementAndGet();
      }
    }

    int getIndex() {
      return idx;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BigObject bigObject = (BigObject)o;

      if (idx != bigObject.idx) return false;
      if (test1 != bigObject.test1) return false;
      if (!test2.equals(bigObject.test2)) return false;
      if (!test3.equals(bigObject.test3)) return false;
      if (!test4.equals(bigObject.test4)) return false;
      if (!bigString.equals(bigObject.bigString)) return false;
      if (another != null ? !another.equals(bigObject.another) : bigObject.another != null) return false;
      return Arrays.equals(largeIntArray, bigObject.largeIntArray);
    }

    @Override
    public int hashCode() {
      int result = idx;
      result = 31 * result + test1;
      result = 31 * result + test2.hashCode();
      result = 31 * result + test3.hashCode();
      result = 31 * result + test4.hashCode();
      result = 31 * result + bigString.hashCode();
      result = 31 * result + (another != null ? another.hashCode() : 0);
      result = 31 * result + Arrays.hashCode(largeIntArray);
      return result;
    }
  }

  private static final class BigObject1 extends BigObject {
    private static final long serialVersionUID = 5409726323968170459L;
    private final float aFloat;

    BigObject1(int idx, BigObject another) {
      super(idx, another);
      // do some random stuff
      aFloat = FrsCodecUtils.FLOAT_SIZE * 1.0f;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      BigObject1 that = (BigObject1)o;

      return Float.compare(that.aFloat, aFloat) == 0;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (aFloat != +0.0f ? Float.floatToIntBits(aFloat) : 0);
      return result;
    }
  }

  private static final class BigObject2 extends BigObject {
    private static final long serialVersionUID = -4312685773317235913L;
    private final double aDouble;

    BigObject2(int idx, BigObject another) {
      super(idx, another);
      // do some random stuff
      aDouble = FrsCodecUtils.DOUBLE_SIZE * 1.88181d;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      BigObject2 that = (BigObject2)o;
      return Double.compare(that.aDouble, aDouble) == 0;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      long temp;
      temp = Double.doubleToLongBits(aDouble);
      result = 31 * result + (int)(temp ^ (temp >>> 32));
      return result;
    }
  }

  private static final class BigObject3 extends BigObject {
    private static final long serialVersionUID = -1029083498234491064L;
    private final String hello;
    BigObject3(int idx, BigObject another) {
      super(idx, another);
      hello = "hello";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      BigObject3 that = (BigObject3)o;
      return hello.equals(that.hello);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + hello.hashCode();
      return result;
    }
  }
}