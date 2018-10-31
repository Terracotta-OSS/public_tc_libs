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

package com.terracottatech.ehcache.clustered.frs;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class BasicClusteredCacheOpsAndRestartIT extends BaseActiveClusteredTest {
  @Parameterized.Parameters(name = "cacheManagerType={0}")
  public static CacheManagerType[] data() {
    return CacheManagerType.values();
  }

  @Parameterized.Parameter
  public CacheManagerType cacheManagerType;

  @Test
  public void testBasicCacheCrudAndRestart() throws Exception {

    try {
      try (PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(Long.class,
          String.class, cacheManagerType)) {
        Cache<Long, String> cache = cacheManager.getCache(CACHE, Long.class, String.class);
        cache.put(1L, "The one");
        assertThat(cache.containsKey(2L), is(false));
        cache.put(2L, "The two");
        assertThat(cache.containsKey(2L), is(true));

        cache.put(1L, "Another one");
        cache.put(3L, "The three");
        assertThat(cache.get(1L), is("Another one"));
        cache.remove(1L);
      }

      restartActive();

      try (PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(Long.class,
          String.class, cacheManagerType)) {

        Cache<Long, String> cache = cacheManager.getCache(CACHE, Long.class, String.class);
        //cache.get() should still succeed
        assertThat(cache.get(1L), is(nullValue()));
        assertThat(cache.get(2L), is("The two"));
        assertThat(cache.get(3L), is("The three"));
      }
    } finally {
      destroyRestartableCacheManagerWithCache();
    }
  }

  @Test
  public void testBasicCacheCasAndRestart() throws Exception {
    try {
      try (PersistentCacheManager cacheManager1 =
               createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType);
           PersistentCacheManager cacheManager2 =
               createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType)) {
        Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, Long.class, String.class);
        Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, Long.class, String.class);

        assertThat(cache1.putIfAbsent(1L, "one"), nullValue());
        assertThat(cache2.putIfAbsent(1L, "another one"), is("one"));

        assertThat(cache2.remove(1L, "another one"), is(false));
        assertThat(cache1.replace(1L, "another one"), is("one"));
        assertThat(cache2.replace(1L, "another one", "yet another one"), is(true));

        assertThat(cache1.remove(1L, "yet another one"), is(true));
        assertThat(cache2.putIfAbsent(1L, "one"), nullValue());
        assertThat(cache1.replace(1L, "one", "yet another one"), is(true));
      }

      restartActive();

      try (PersistentCacheManager cacheManager1 =
               createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType);
           PersistentCacheManager cacheManager2 =
               createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType)) {
        Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, Long.class, String.class);
        Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, Long.class, String.class);
        assertThat(cache1.get(1L), is("yet another one"));
        assertThat(cache2.get(1L), is("yet another one"));
      }
    } finally {
      destroyRestartableCacheManagerWithCache();
    }
  }

  @Test
  public void testBasicClusteredBulkAndRestart() throws Exception {
    try {
      try (PersistentCacheManager cacheManager1 =
               createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType)) {
        final Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, Long.class, String.class);

        Map<Long, String> entriesMap = new HashMap<>();
        entriesMap.put(1L, "one");
        entriesMap.put(2L, "two");
        entriesMap.put(3L, "three");
        cache1.putAll(entriesMap);
      }

      restartActive();

      try (PersistentCacheManager cacheManager1 =
               createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType);
           PersistentCacheManager cacheManager2 =
               createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType)) {
        final Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, Long.class, String.class);
        final Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, Long.class, String.class);

        Set<Long> keySet = new HashSet<>(Arrays.asList(1L, 2L, 3L));
        Map<Long, String> all = cache2.getAll(keySet);
        assertThat(all.get(1L), is("one"));
        assertThat(all.get(2L), is("two"));
        assertThat(all.get(3L), is("three"));

        cache2.removeAll(keySet);

        all = cache1.getAll(keySet);
        assertThat(all.get(1L), nullValue());
        assertThat(all.get(2L), nullValue());
        assertThat(all.get(3L), nullValue());
      }
    } finally {
      destroyRestartableCacheManagerWithCache();
    }
  }
}
