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

import java.io.File;
import java.io.Serializable;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test lifecycle of FRS based caches.
 *
 * @author RKAV
 */
public class LifecycleTest {
  // NUM_CACHE should be > 1
  private static final int NUM_CACHES = 2;
  // TEST ITERATION SHOULD BE AT LEAST > 1
  private static final int TEST_ITERATIONS = 2;
  private static final String TEST_CACHE_PREFIX = "test-cache-";

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
  public void testCreateAndCloseAll() throws Exception {
    for (int i = 0; i < NUM_CACHES * TEST_ITERATIONS; i++) {
      int cacheNo = i%NUM_CACHES;
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(cacheNo, true);
      writeDataToCache(cache, cacheNo, i >= NUM_CACHES);
      assertDataInCache(cache, cacheNo);
      cacheManagerUnderTest.close();
    }
  }

  @Test
  public void testRemoveCacheWhenOthersArePersistentAndOpen() throws Exception {
    for (int i = 0; i < NUM_CACHES * TEST_ITERATIONS; i++) {
      int cacheNo = i%NUM_CACHES;
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(cacheNo, true);
      writeDataToCache(cache, cacheNo, i >= NUM_CACHES);
      cacheManagerUnderTest.removeCache(getCacheAlias(cacheNo));
      // make sure this cache is not available
      try {
        cache.get(1L);
        fail("Unexpected state for removed cache " + getCacheAlias(cacheNo));
      } catch (IllegalStateException ise) {
        assertThat(ise.getMessage(), containsString("UNINITIALIZED"));
      }
      if (i >= NUM_CACHES) {
        // open other caches and ensure values are as expected
        int otherCacheNo = (i - 1) % NUM_CACHES;
        final Cache<Long, TestClass> otherCache = cacheManagerUnderTest.getCache(getCacheAlias(otherCacheNo),
            Long.class, TestClass.class);
        assertDataInCache(otherCache, otherCacheNo);
      }
      // now create the removed cache again
      final Cache<Long, TestClass> cache1 = createCache(cacheNo);
      assertDataInCache(cache1, cacheNo);
      cacheManagerUnderTest.removeCache(getCacheAlias(cacheNo));
      cacheManagerUnderTest.close();
    }
  }

  @Test
  public void testRemoveCacheWhenOthersArePersistentAndClosed() throws Exception {
    for (int i = 0; i < NUM_CACHES * TEST_ITERATIONS; i++) {
      int cacheNo = i%NUM_CACHES;
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(cacheNo, false);
      writeDataToCache(cache, cacheNo, i >= NUM_CACHES);
      cacheManagerUnderTest.removeCache(getCacheAlias(cacheNo));
      // make sure this cache is not available
      try {
        cache.get(1L);
        fail("Unexpected state for removed cache " + getCacheAlias(cacheNo));
      } catch (IllegalStateException ise) {
        assertThat(ise.getMessage(), containsString("UNINITIALIZED"));
      }
      if (i >= NUM_CACHES) {
        // open other caches and ensure values are as expected
        int otherCacheNo = (cacheNo + 1) % NUM_CACHES;
        final Cache<Long, TestClass> otherCache = createCache(otherCacheNo);
        assertDataInCache(otherCache, otherCacheNo);
        cacheManagerUnderTest.removeCache(getCacheAlias(otherCacheNo));
      }
      // now create the removed cache again
      final Cache<Long, TestClass> cache1 = createCache(cacheNo);
      assertDataInCache(cache1, cacheNo);
      cacheManagerUnderTest.removeCache(getCacheAlias(cacheNo));
      cacheManagerUnderTest.close();
    }
  }

  @Test
  public void testRemoveCachePreservesData() throws Exception {
    Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, false);
    writeDataToCache(cache, 0, false);
    cacheManagerUnderTest.removeCache(getCacheAlias(0));
    cacheManagerUnderTest.close();

    cache = createCacheManagerAndGetCache(0, false);
    assertDataInCache(cache, 0);
  }

  @Test
  public void testDestroyAllWipesData() throws Exception {
    Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, false);
    writeDataToCache(cache, 0, false);
    writeDataToCache(createCache(1), 1, false);
    cacheManagerUnderTest.close();
    cacheManagerUnderTest.destroy();

    cache = createCacheManagerAndGetCache(0, false);
    writeDataToCache(cache, 0, false);
    writeDataToCache(createCache(1), 1, false);
  }

  @Test
  public void testDestroyCacheWipesData() throws Exception {
    Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, false);
    writeDataToCache(cache, 0, false);

    cacheManagerUnderTest.destroyCache(getCacheAlias(0));
    cacheManagerUnderTest.close();

    cache = createCacheManagerAndGetCache(0, false);
    writeDataToCache(cache, 0, false);
  }

  @Test
  public void testDestroyCacheDoesNotTouchOtherCaches() throws Exception {
    // first create two caches and put some data
    final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, true);
    Cache<Long, TestClass> otherCache = cacheManagerUnderTest.getCache(getCacheAlias(1), Long.class, TestClass.class);
    writeDataToCache(cache, 0, false);
    writeDataToCache(otherCache, 1, false);
    cacheManagerUnderTest.destroyCache(getCacheAlias(0));
    cacheManagerUnderTest.close();

    otherCache = createCacheManagerAndGetCache(1, false);
    assertDataInCache(otherCache, 1);
  }

  @Test
  public void testDestroyMultipleWithNoCacheCreated() throws Exception {
    {
      // first create a cache and put some data
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, true);
      final Cache<Long, TestClass> otherCache = cacheManagerUnderTest.getCache(getCacheAlias(1), Long.class, TestClass.class);
      writeDataToCache(cache, 0, false);
      writeDataToCache(otherCache, 1, false);
      cacheManagerUnderTest.close();
    }
    {
      // now open to destroy one of the caches.
      createCacheManagerForDestroy();
      cacheManagerUnderTest.destroyCache(getCacheAlias(0));
      cacheManagerUnderTest.destroyCache(getCacheAlias(1));

      // now use the same cache manager to open other cache
      final Cache<Long, TestClass> otherCache = createCache(1);
      writeDataToCache(otherCache, 1, false);
      cacheManagerUnderTest.close();
    }
    {
      // now open again and ensure that destroyed cache is not available
      // and that a new cache is returned instead.
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, true);
      final Cache<Long, TestClass> otherCache =
          cacheManagerUnderTest.getCache(getCacheAlias(1), Long.class, TestClass.class);

      assertDataInCache(otherCache, 1);
      writeDataToCache(cache, 0, false);
      cacheManagerUnderTest.close();
    }
  }

  @Test
  public void testDestroyCacheAfterRemoveCache() throws Exception {
    {
      // first create a cache and put some data
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, true);
      final Cache<Long, TestClass> otherCache = cacheManagerUnderTest.getCache(getCacheAlias(1), Long.class, TestClass.class);
      writeDataToCache(cache, 0, false);
      writeDataToCache(otherCache, 1, false);
      cacheManagerUnderTest.close();
    }
    {
      // now open one cache, remove it and then destroy it..
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, true);
      assertDataInCache(cache, 0);
      cacheManagerUnderTest.removeCache(getCacheAlias(0));
      cacheManagerUnderTest.destroyCache(getCacheAlias(0));

      // now use the same cache manager to open other cache
      final Cache<Long, TestClass> otherCache = cacheManagerUnderTest.getCache(getCacheAlias(1), Long.class,
          TestClass.class);
      assertDataInCache(otherCache, 1);

      final Cache<Long, TestClass> deletedCache = cacheManagerUnderTest.getCache(getCacheAlias(0), Long.class,
          TestClass.class);
      assertNull(deletedCache);
      cacheManagerUnderTest.close();
    }
  }

  @Test
  public void testDestroyCacheWithNoCacheCreatedAndCreateSameCache() throws Exception {
    {
      // first create a cache and put some data
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, true);
      final Cache<Long, TestClass> otherCache = cacheManagerUnderTest.getCache(getCacheAlias(1), Long.class, TestClass.class);
      writeDataToCache(cache, 0, false);
      writeDataToCache(otherCache, 1, false);
      cacheManagerUnderTest.close();
    }
    {
      // now open to destroy one of the caches and create same cache
      createCacheManagerForDestroy();
      cacheManagerUnderTest.destroyCache(getCacheAlias(0));

      // now use the same cache manager to open other cache
      final Cache<Long, TestClass> sameCache = createCache(0);
      writeDataToCache(sameCache, 0, false);
      assertDataInCache(sameCache, 0);
      cacheManagerUnderTest.close();
    }
    {
      // now open again and ensure data exists
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, false);
      assertDataInCache(cache, 0);

      // now use the same cache manager to open other cache
      final Cache<Long, TestClass> otherCache = createCache(1);
      assertDataInCache(otherCache, 1);
      cacheManagerUnderTest.close();
    }
  }

  @Test
  public void testDestroyCacheAndCreateSameCache() throws Exception {
    {
      // first create a cache and put some data and destroy one of the caches and create
      // another with same alias
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, true);
      final Cache<Long, TestClass> otherCache = cacheManagerUnderTest.getCache(getCacheAlias(1), Long.class, TestClass.class);
      writeDataToCache(cache, 0, false);
      writeDataToCache(otherCache, 1, false);
      cacheManagerUnderTest.destroyCache(getCacheAlias(1));
      assertDataInCache(cache, 0);
      try {
        otherCache.get(1L);
        fail("Unexpected success on a get of a removed cache " + getCacheAlias(1));
      } catch (IllegalStateException ise) {
        assertThat(ise.getMessage(), containsString("UNINITIALIZED"));
      }
      final Cache<Long, TestClass> sameCache = createCache(1);
      writeDataToCache(sameCache, 1, false);
      assertDataInCache(cache, 0);
      cacheManagerUnderTest.close();
    }
    {
      // now open and check again whether data is persistent
      final Cache<Long, TestClass> cache = createCacheManagerAndGetCache(1, false);
      assertDataInCache(cache, 1);
      writeDataToCache(cache, 1, true);
      assertDataInCache(cache, 1);
      cacheManagerUnderTest.close();
    }
  }

  @Test
  public void testDestroyAllMultipleTimes() throws Exception {
    Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, false);
    writeDataToCache(cache, 0, false);
    cacheManagerUnderTest.close();

    cacheManagerUnderTest.destroy();
    cacheManagerUnderTest.destroy();
  }

  @Test
  public void testDestroyCacheWhileUninitialized() throws Exception {
    Cache<Long, TestClass> cache = createCacheManagerAndGetCache(0, false);
    writeDataToCache(cache, 0, false);
    assertThat(cache.iterator().hasNext(), is(true));
    cacheManagerUnderTest.close();

    cacheManagerUnderTest.destroyCache(getCacheAlias(0));
  }

  @Test
  public void testDestroyUnknownCacheWhileInitialized() throws Exception {
    createCacheManagerForDestroy();
    cacheManagerUnderTest.destroyCache("non-existing");
  }

  @Test
  public void testDestroyUnknownCacheWhileUninitialized() throws Exception {
    createCacheManagerForDestroy();
    cacheManagerUnderTest.close();
    cacheManagerUnderTest.destroyCache("non-existing");
  }

  private TestClass getValue(int num, int cacheNo) {
    return new TestClass("value-" + num + "-" + cacheNo);
  }

  private Cache<Long, TestClass> createCacheManagerAndGetCache(int cacheNo, boolean openAll) {
    CacheManagerBuilder<PersistentCacheManager> frsCacheManagerBuilder = newCacheManagerBuilder().
        with(CacheManagerBuilder.persistence(currentFolder.getAbsolutePath()));
    ResourcePoolsBuilder resourcePoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(EnterpriseDiskResourcePoolBuilder.diskRestartable(1, MemoryUnit.MB));
    if (openAll) {
      for (int i = 0; i < NUM_CACHES; i++) {
        frsCacheManagerBuilder = frsCacheManagerBuilder.withCache(getCacheAlias(i),
            newCacheConfigurationBuilder(Long.class, TestClass.class, resourcePoolsBuilder));
      }
    } else {
      frsCacheManagerBuilder = frsCacheManagerBuilder.withCache(getCacheAlias(cacheNo),
          newCacheConfigurationBuilder(Long.class, TestClass.class, resourcePoolsBuilder));
    }
    cacheManagerUnderTest = frsCacheManagerBuilder.build(true);
    return cacheManagerUnderTest.getCache(getCacheAlias(cacheNo), Long.class, TestClass.class);
  }

  private void createCacheManagerForDestroy() {
    CacheManagerBuilder<PersistentCacheManager> frsCacheManagerBuilder = newCacheManagerBuilder().
        with(CacheManagerBuilder.persistence(currentFolder.getAbsolutePath()));
    cacheManagerUnderTest = frsCacheManagerBuilder.build(true);
  }

  private Cache<Long, TestClass> createCache(int cacheNo) {
    ResourcePoolsBuilder resourcePoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(EnterpriseDiskResourcePoolBuilder.diskRestartable(1, MemoryUnit.MB));
    return cacheManagerUnderTest.createCache(getCacheAlias(cacheNo),
        newCacheConfigurationBuilder(Long.class, TestClass.class, resourcePoolsBuilder));
  }

  private String getCacheAlias(int cacheNo) {
    return TEST_CACHE_PREFIX + cacheNo;
  }

  private void writeDataToCache(final Cache<Long, TestClass> cacheToWrite, int cacheNo, boolean dataExists) {
    if (dataExists) {
      assertThat(cacheToWrite.get(1L), is(getValue(0, cacheNo)));
      assertThat(cacheToWrite.get(100L), is(getValue(99, cacheNo)));
      assertNull(cacheToWrite.get(1000L));
    } else {
      assertNull(cacheToWrite.get(1L));
      assertNull(cacheToWrite.get(100L));
    }
    for (int k = 0; k < 100; k++) {
      cacheToWrite.put(k + 1L, getValue(k, cacheNo));
    }
    assertThat(cacheToWrite.get(1L), is(getValue(0, cacheNo)));
    assertNull(cacheToWrite.get(1000L));
  }

  private void assertDataInCache(final Cache<Long, TestClass> cacheToAssert, int cacheNo) {
    for (int k = 0; k < 100; k++) {
      assertThat(cacheToAssert.get(k + 1L), is(getValue(k, cacheNo)));
    }
    assertNull(cacheToAssert.get(1000L));
  }

  private static final class TestClass implements Serializable {
    private static final long serialVersionUID = 646243352795421454L;
    private final String someString;

    private TestClass(String someString) {
      this.someString = someString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestClass testClass = (TestClass)o;
      return someString.equals(testClass.someString);
    }

    @Override
    public int hashCode() {
      return someString.hashCode();
    }
  }
}
