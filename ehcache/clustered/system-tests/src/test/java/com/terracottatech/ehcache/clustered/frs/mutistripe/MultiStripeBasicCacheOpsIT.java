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
package com.terracottatech.ehcache.clustered.frs.mutistripe;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class MultiStripeBasicCacheOpsIT extends BaseMultiStripeClusteredTest {

  @Test
  public void testCRUD() throws Exception {
    try (PersistentCacheManager cacheManager = createNonRestartableCacheManagerWithCache(CACHE_MANAGER, CACHE)) {
      Cache<Long, String> cache = cacheManager.getCache(CACHE, Long.class, String.class);

      String value = "value";
      int numElements = 10;
      for (long key = 1; key <= numElements; ++key) {
        cache.put(key, value + key);
      }
      //Verify that GETs succeed
      for (long key = 1; key <= numElements; ++key) {
        assertThat(cache.get(key), is(value + key));
      }

      for (long key = 1; key < numElements / 2; ++key) {
        cache.remove(key);
      }
      //Verify that REMOVEs succeeded
      for (long key = 1; key < numElements / 2; ++key) {
        assertThat(cache.get(key), is(nullValue()));

      }

      cache.clear();
      //Verify that CLEAR succeeded
      for (long key = numElements / 2; key <= numElements; ++key) {
        assertThat(cache.get(key), is(nullValue()));

      }
    }
    destroyCacheManager(CACHE_MANAGER);
  }

  @Test
  public void testCAS() throws Exception {
    try (PersistentCacheManager cacheManager1 = createNonRestartableCacheManagerWithCache(CACHE_MANAGER, CACHE);
         PersistentCacheManager cacheManager2 = createNonRestartableCacheManagerWithCache(CACHE_MANAGER, CACHE)) {
      Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, Long.class, String.class);
      Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, Long.class, String.class);

      assertThat(cache1.putIfAbsent(1L, "one"), nullValue());
      assertThat(cache2.putIfAbsent(1L, "another one"), is("one"));

      assertThat(cache2.remove(1L, "another one"), is(false));
      assertThat(cache1.replace(1L, "another one"), is("one"));

      assertThat(cache2.replace(1L, "another one", "yet another one"), is(true));
      assertThat(cache1.remove(1L, "yet another one"), is(true));

      assertThat(cache2.replace(1L, "one"), nullValue());
      assertThat(cache1.replace(1L, "another one", "yet another one"), is(false));
    }
    destroyCacheManager(CACHE_MANAGER);
  }

  @Test
  public void testBulkOps() throws Exception {
    try (PersistentCacheManager cacheManager1 = createNonRestartableCacheManagerWithCache(CACHE_MANAGER, CACHE);
         PersistentCacheManager cacheManager2 = createNonRestartableCacheManagerWithCache(CACHE_MANAGER, CACHE)) {
      Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, Long.class, String.class);
      Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, Long.class, String.class);

      Map<Long, String> entriesMap = new HashMap<>();
      String value = "value";
      int numElements = 100;
      for (long key = 1; key <= numElements; ++key) {
        entriesMap.put(key, value + key);
      }

      cache1.putAll(entriesMap);
      //Verify that putAll succeeded
      Map<Long, String> cache2All = cache2.getAll(entriesMap.keySet());
      for (long key = 1; key <= numElements; ++key) {
        assertThat(cache2All.get(key), is(value + key));
      }

      cache2.removeAll(entriesMap.keySet());
      //Verify that removeAll succeeded
      Map<Long, String> cache1All = cache1.getAll(entriesMap.keySet());
      for (long key = 1; key <= numElements; ++key) {
        assertThat(cache1All.get(key), is(nullValue()));
      }
    }
    destroyCacheManager(CACHE_MANAGER);
  }
}
