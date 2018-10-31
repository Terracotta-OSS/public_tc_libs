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
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.Status;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.ehcache.disk.config.builders.EnterpriseDiskResourcePoolBuilder;
import com.terracottatech.ehcache.common.frs.FrsCodecUtils;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Test real serializable objects in cache. Also tests the state repository.
 *
 * @author RKAV
 */
public class LargeAndSmallObjectTest {
  private static final int MAX_ELEMENTS_IN_CACHE = 2;

  private PersistentCacheManager cacheManagerUnderTest;

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();
  private File currentFolder;

  @Before
  public void setup() throws Exception {
    currentFolder = tmpFolder.newFolder();
  }

  @After
  public void tearDown() throws CachePersistenceException {
    if (cacheManagerUnderTest != null) {
      if (cacheManagerUnderTest.getStatus().equals(Status.AVAILABLE)) {
        cacheManagerUnderTest.close();
      }
      cacheManagerUnderTest.destroy();
    }
  }

  @Test
  public void testSimplePutBig() throws Exception {
    final Cache<Long, BigObject> cache = getCache(BigObject.class);
    TestAssertHandler<BigObject> th = new BigObject.BigObjectHandler();
    doPuts(cache, th);
    doGets(cache, th.getConsumer());
  }

  @Test
  public void testSimplePutSmall() throws Exception {
    final Cache<Long, SmallObject> cache = getCache(SmallObject.class);
    TestAssertHandler<SmallObject> th = new SmallObject.SmallObjectHandler();
    doPuts(cache, th);
    doGets(cache, th.getConsumer());
  }

  @Test
  public void testPersistentPutBig() throws Exception {
    TestAssertHandler<BigObject> th = new BigObject.BigObjectHandler();
    {
      final Cache<Long, BigObject> cache = getCache(BigObject.class);
      doPuts(cache, th);
      cacheManagerUnderTest.close();
    }
    {
      final Cache<Long, BigObject> cache = getCache(BigObject.class);
      doGets(cache, th.getConsumer());
    }
  }

  @Test
  public void testPersistentPutSmall() throws Exception {
    TestAssertHandler<SmallObject> th = new SmallObject.SmallObjectHandler();
    {
      final Cache<Long, SmallObject> cache = getCache(SmallObject.class);
      doPuts(cache, th);
      cacheManagerUnderTest.close();
    }
    {
      final Cache<Long, SmallObject> cache = getCache(SmallObject.class);
      doGets(cache, th.getConsumer());
    }
  }

  private <V extends IndexGetter> void doPuts(Cache<Long, V> cache, TestAssertHandler<V> handler) {
    Consumer<V> valueConsumer = handler.getConsumer();
    Supplier<V> valueSupplier = handler.getSupplier();
    for (int i = 0; i < MAX_ELEMENTS_IN_CACHE; i++) {
      V v = valueSupplier.get();
      long key = v.getIndex();
      cache.put(key, v);
      valueConsumer.accept(v);
    }
  }

  private <V extends IndexGetter> void doGets(Cache<Long, V> cache, Consumer<V> valueConsumer) {
    for (int i = 0; i < MAX_ELEMENTS_IN_CACHE; i++) {
      valueConsumer.accept(cache.get(i+1L));
    }
    assertNull(cache.get(Long.MAX_VALUE));
    assertNull(cache.get(Long.MIN_VALUE));
  }

  @SuppressWarnings("unchecked")
  private <V extends IndexGetter> Cache<Long, V> getCache(Class<V> type) {
    boolean big = type.equals(BigObject.class);
    CacheManagerBuilder<PersistentCacheManager> frsCacheManagerBuilder = newCacheManagerBuilder().
        with(CacheManagerBuilder.persistence(currentFolder.getAbsolutePath()));
    ResourcePoolsBuilder resourcePoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(EnterpriseDiskResourcePoolBuilder.diskRestartable((big) ? 10 : 1, MemoryUnit.MB));
    frsCacheManagerBuilder = frsCacheManagerBuilder.withCache("test-cache",
        newCacheConfigurationBuilder(Long.class, (Class<V>) (big ? BigObject.class : SmallObject.class), resourcePoolsBuilder));
    cacheManagerUnderTest = frsCacheManagerBuilder.build(true);
    return cacheManagerUnderTest.getCache("test-cache", Long.class, (Class<V>) (big ? BigObject.class : SmallObject.class));
  }

  private static class BigObject implements Serializable, IndexGetter {
    private static final long serialVersionUID = 17999928982756610L;
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
      largeIntArray = new int[1024*10];
      for (int i = 0; i < 1024*10; i++) {
        largeIntArray[i] = intCounter.incrementAndGet();
      }
    }

    public int getIndex() {
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

    private static class BigObjectHandler implements TestAssertHandler<BigObject> {
      private final Map<Integer, BigObject> createdObjects = new HashMap<>();
      private int counter = 1;

      public Consumer<BigObject> getConsumer() {
        return bigObject -> {
          BigObject created = createdObjects.get(bigObject.idx);
          assertNotNull(created);
          assertThat(bigObject, is(created));
        };
      }

      public Supplier<BigObject> getSupplier() {
        return () -> {
          BigObject big;
          if (createdObjects.isEmpty()) {
            big = new BigObject(counter, null);
            createdObjects.put(counter, big);
          } else {
            switch (counter % 5) {
              case 0:
                big = new BigObject(counter, createdObjects.get(1));
                break;
              case 1:
                big = new BigObject1(counter, createdObjects.get(2));
                break;
              case 2:
                big = new BigObject2(counter, createdObjects.get(3));
                break;
              case 3:
              default:
                big = new BigObject3(counter, createdObjects.get(4));
                break;
            }
            createdObjects.put(counter, big);
          }
          counter++;
          return big;
        };
      }
    }
  }

  private static final class BigObject1 extends BigObject {
    private static final long serialVersionUID = -155257300108855782L;
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
    private static final long serialVersionUID = -1426101492904795822L;
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
    private static final long serialVersionUID = 4676907723988624263L;
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

  private static class SmallObject implements Serializable, IndexGetter {
    private static final long serialVersionUID = 6942346100610449515L;
    private final Integer i1;

    private SmallObject(int i1) {
      this.i1 = i1;
    }

    public int getIndex() {
      return i1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SmallObject that = (SmallObject)o;

      return i1.equals(that.i1);

    }

    @Override
    public int hashCode() {
      return i1.hashCode();
    }

    private static class SmallObjectHandler implements TestAssertHandler<SmallObject> {
      private final Map<Integer, SmallObject> createdObjects = new HashMap<>();
      private int counter = 1;

      public Consumer<SmallObject> getConsumer() {
        return smallObject -> {
          SmallObject created = createdObjects.get(smallObject.i1);
          assertNotNull(created);
          assertThat(smallObject, is(created));
        };
      }

      public Supplier<SmallObject> getSupplier() {
        return () -> {
          SmallObject small = (counter % 2 == 0) ? new SmallObject(counter) : new SmallObject1(counter);
          createdObjects.put(counter, small);
          counter++;
          return small;
        };
      }
    }
  }

  private static class SmallObject1 extends SmallObject {
    private static final long serialVersionUID = 2892849981278424499L;
    private final int i2;

    private SmallObject1(int i1) {
      super(i1);
      this.i2 = i1 + 1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      SmallObject1 that = (SmallObject1)o;
      return i2 == that.i2;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + i2;
      return result;
    }
  }

  private interface TestAssertHandler<T> {
    Supplier<T> getSupplier();
    Consumer<T> getConsumer();
  }

  private interface IndexGetter {
    int getIndex();
  }
}
