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
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class MixedCachesActivePassiveAndRestartIT extends BaseActivePassiveClusteredTest {
  private final String CACHE1 = "clustered-cache1";
  private final String CACHE2 = "clustered-cache2";

  @Parameters(name = "consistency={0}")
  public static Consistency[] data() {
    return Consistency.values();
  }

  @Parameter
  public Consistency cacheConsistency;

  private CacheConfiguration<Long, String> getTransientCacheConfig() {
    ClusteredResourcePool pool = ClusteredResourcePoolBuilder
        .clusteredDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> cacheConfig = CacheConfigurationBuilder
        .newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool))
        .add(new ClusteredStoreConfiguration(cacheConsistency))
        .build();
    return cacheConfig;
  }

  private CacheConfiguration<Long, String> getRestartableCacheConfig() {
    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> restartableCacheConfig = CacheConfigurationBuilder
        .newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool))
        .add(new ClusteredStoreConfiguration(cacheConsistency))
        .build();
    return restartableCacheConfig;
  }

  @Test
  public void testMixedCacheManager() throws Exception {
    try (PersistentCacheManager cacheManager = createRestartableCacheManager(CacheManagerType.FULL)) {
      Cache<Long, String> transientCache = cacheManager.createCache(CACHE1, getTransientCacheConfig());
      Cache<Long, String> restartableCache = cacheManager.createCache(CACHE2, getRestartableCacheConfig());

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(transientCache);
      caches.add(restartableCache);

      caches.forEach(x -> {
        x.put(1L, "The one");
        x.put(2L, "The two");
        x.put(1L, "Another one");
        x.put(3L, "The three");
        x.put(4L, "The four");
        assertThat(x.get(1L), equalTo("Another one"));
        assertThat(x.get(2L), equalTo("The two"));
        assertThat(x.get(3L), equalTo("The three"));
        assertThat(x.get(4L), equalTo("The four"));
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

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(CacheManagerType.FULL)) {
      Cache<Long, String> transientCache = cacheManager.createCache(CACHE1, getTransientCacheConfig());
      Cache<Long, String> restartableCache = cacheManager.createCache(CACHE2, getRestartableCacheConfig());

      assertThat(restartableCache.get(1L), equalTo("Another one"));
      assertThat(restartableCache.get(2L), equalTo("The two"));
      assertThat(restartableCache.get(3L), equalTo("The three"));
      assertThat(restartableCache.get(4L), nullValue());

      assertThat(transientCache.get(1L), nullValue());
      assertThat(transientCache.get(2L), nullValue());
      assertThat(transientCache.get(3L), nullValue());
      assertThat(transientCache.get(4L), nullValue());
    }

    destroyRestartableCacheManager();
  }

  @Test
  public void testNonRestartableCacheManager() throws Exception {
    try (PersistentCacheManager cacheManager = createNonRestartableCacheManager("my-cm")) {
      Cache<Long, String> transientCache1 = cacheManager.createCache(CACHE1, getTransientCacheConfig());
      Cache<Long, String> transientCache2 = cacheManager.createCache(CACHE2, getTransientCacheConfig());

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(transientCache1);
      caches.add(transientCache2);

      caches.forEach(x -> {
        x.put(1L, "The one");
        x.put(2L, "The two");
        x.put(1L, "Another one");
        x.put(3L, "The three");
        x.put(4L, "The four");
        assertThat(x.get(1L), equalTo("Another one"));
        assertThat(x.get(2L), equalTo("The two"));
        assertThat(x.get(3L), equalTo("The three"));
        assertThat(x.get(4L), equalTo("The four"));
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

    try (PersistentCacheManager cacheManager = createNonRestartableCacheManager("my-cm")) {
      Cache<Long, String> transientCache1 = cacheManager.createCache(CACHE1, getTransientCacheConfig());
      Cache<Long, String> transientCache2 = cacheManager.createCache(CACHE2, getTransientCacheConfig());

      assertThat(transientCache1.get(1L), nullValue());
      assertThat(transientCache1.get(2L), nullValue());
      assertThat(transientCache1.get(3L), nullValue());
      assertThat(transientCache1.get(4L), nullValue());

      assertThat(transientCache2.get(1L), nullValue());
      assertThat(transientCache2.get(2L), nullValue());
      assertThat(transientCache2.get(3L), nullValue());
      assertThat(transientCache2.get(4L), nullValue());
    }
    destroyCacheManager("my-cm");
  }

}
