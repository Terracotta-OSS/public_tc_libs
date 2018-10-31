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
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.clustered.common.Consistency.EVENTUAL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BasicClusteredCacheOpsReplicationAndRestartIT extends BaseActivePassiveClusteredTest {

  private final String CACHE1 = "clustered-cache1";
  private final String CACHE2 = "clustered-cache2";

  @Parameters(name = "{0}")
  public static CacheManagerType[] data() {
    return CacheManagerType.values();
  }

  @Parameter
  public CacheManagerType cacheManagerType;

  @After
  public void tearDown() throws Exception {
    destroyRestartableCacheManager();
  }

  @Test
  public void testCRUD() throws Exception {

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
            .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> cacheConfig = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool))
            .add(new ClusteredStoreConfiguration(EVENTUAL))
            .build();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      caches.forEach(x -> {
        x.put(1L, "The one");
        x.put(2L, "The two");
        x.put(1L, "Another one");
        x.put(3L, "The three");
        x.put(4L, "The four");
        assertThat(x.get(1L), equalTo("Another one"));
        assertThat(x.get(2L), equalTo("The two"));
        assertThat(x.get(3L), equalTo("The three"));
        x.remove(4L);
      });

      shutdownActiveAndWaitForPassiveToBecomeActive();

      caches.forEach(x -> {
        assertThat(x.get(1L), equalTo("Another one"));
        assertThat(x.get(2L), equalTo("The two"));
        assertThat(x.get(3L), equalTo("The three"));
        assertThat(x.get(4L), nullValue());
      });
    }

    restartServer();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      caches.forEach(x -> {
        assertThat(x.get(1L), equalTo("Another one"));
        assertThat(x.get(2L), equalTo("The two"));
        assertThat(x.get(3L), equalTo("The three"));
        assertThat(x.get(4L), nullValue());
      });
    }
  }

  @Test
  public void testBulkOps() throws Exception {

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
            .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> cacheConfig = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool))
            .add(new ClusteredStoreConfiguration(EVENTUAL))
            .build();

    Map<Long, String> entriesMap = new HashMap<>();
    entriesMap.put(1L, "one");
    entriesMap.put(2L, "two");
    entriesMap.put(3L, "three");

    Set<Long> keySet = entriesMap.keySet();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      caches.forEach(cache -> cache.putAll(entriesMap));

      shutdownActiveAndWaitForPassiveToBecomeActive();

      caches.forEach(cache -> {
        Map<Long, String> all = cache.getAll(keySet);
        assertThat(all.get(1L), is("one"));
        assertThat(all.get(2L), is("two"));
        assertThat(all.get(3L), is("three"));
      });
    }

    restartServer();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      caches.forEach(cache -> {
        Map<Long, String> all = cache.getAll(keySet);
        assertThat(all.get(1L), is("one"));
        assertThat(all.get(2L), is("two"));
        assertThat(all.get(3L), is("three"));
      });
    }
  }

  @Test
  public void testCAS() throws Exception {

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
            .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> cacheConfig = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool))
            .add(new ClusteredStoreConfiguration(EVENTUAL))
            .build();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      caches.forEach(cache -> {
        assertThat(cache.putIfAbsent(1L, "one"), nullValue());
        assertThat(cache.putIfAbsent(2L, "two"), nullValue());
        assertThat(cache.putIfAbsent(3L, "three"), nullValue());
        assertThat(cache.replace(3L, "another one", "yet another one"), is(false));
      });

      shutdownActiveAndWaitForPassiveToBecomeActive();

      caches.forEach(cache -> {
        assertThat(cache.putIfAbsent(1L, "another one"), is("one"));
        assertThat(cache.remove(2L, "not two"), is(false));
        assertThat(cache.replace(3L, "three", "another three"), is(true));
        assertThat(cache.replace(2L, "new two"), is("two"));
      });
    }

    restartServer();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      caches.forEach(cache -> {
        assertThat(cache.putIfAbsent(1L, "another one"), is("one"));
        assertThat(cache.remove(2L, "not two"), is(false));
        assertThat(cache.replace(3L, "three", "another three"), is(true));
        assertThat(cache.replace(2L, "new two"), is("two"));
      });
    }
  }

  @Test
  public void testClear() throws Exception {

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
            .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> cacheConfig = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool))
            .add(new ClusteredStoreConfiguration(EVENTUAL))
            .build();

    Map<Long, String> entriesMap = new HashMap<>();
    entriesMap.put(1L, "one");
    entriesMap.put(2L, "two");
    entriesMap.put(3L, "three");

    Set<Long> keySet = entriesMap.keySet();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      caches.forEach(cache -> cache.putAll(entriesMap));

      caches.forEach(cache -> {
        Map<Long, String> all = cache.getAll(keySet);
        assertThat(all.get(1L), is("one"));
        assertThat(all.get(2L), is("two"));
        assertThat(all.get(3L), is("three"));
      });

      cache1.clear();
      cache2.clear();

      shutdownActiveAndWaitForPassiveToBecomeActive();

      keySet.forEach(x -> assertThat(cache1.get(x), nullValue()));
      keySet.forEach(x -> assertThat(cache2.get(x), nullValue()));

    }

    restartServer();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager.createCache(CACHE1, cacheConfig);
      Cache<Long, String> cache2 = cacheManager.createCache(CACHE2, cacheConfig);

      keySet.forEach(x -> assertThat(cache1.get(x), nullValue()));
      keySet.forEach(x -> assertThat(cache2.get(x), nullValue()));
    }
  }
}
