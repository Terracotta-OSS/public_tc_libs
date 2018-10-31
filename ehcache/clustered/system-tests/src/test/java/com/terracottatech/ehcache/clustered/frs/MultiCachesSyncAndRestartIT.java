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
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class MultiCachesSyncAndRestartIT extends BaseActivePassiveClusteredTest {
  private static final String CACHE1 = "clustered-cache1";
  private static final String CACHE2 = "clustered-cache2";
  private static final String CACHE3 = "clustered-cache3";

  // NOTE: Need to change this to 10000 for reproducing TDB-1870.
  private static final long NUM_ITERATIONS = 10L;
  private static final long ITERATION_TO_CHECK = (NUM_ITERATIONS * 2) - 2;

  private CacheConfiguration<Long, String> getRestartableCacheConfig() {
    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 16, MemoryUnit.MB);

    return CacheConfigurationBuilder
        .newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool))
        .add(new ClusteredStoreConfiguration(Consistency.EVENTUAL))
        .build();
  }

  @Test
  public void testMultiCacheCacheManager() throws Exception {
    try (PersistentCacheManager cacheManager = createRestartableCacheManager(CacheManagerType.FULL)) {
      CLUSTER.getClusterControl().terminateOnePassive();
      Cache<Long, String> rc1 = cacheManager.createCache(CACHE1, getRestartableCacheConfig());
      Cache<Long, String> rc2 = cacheManager.createCache(CACHE2, getRestartableCacheConfig());
      Cache<Long, String> rc3 = cacheManager.createCache(CACHE3, getRestartableCacheConfig());

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(rc1);
      caches.add(rc2);
      caches.add(rc3);

      caches.forEach(x -> {
        for (long i = 1L; i < NUM_ITERATIONS; i++) {
          x.put(i, "dummy" + i);
        }
        x.remove(9L);
      });

      CLUSTER.getClusterControl().startOneServer();

      caches.forEach(x -> {
        for (long i = NUM_ITERATIONS; i < NUM_ITERATIONS * 2; i++) {
          x.put(i, "dummy" + i);
        }
      });

      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      shutdownActiveAndWaitForPassiveToBecomeActive();

      caches.forEach(x -> {
        assertThat(x.get(ITERATION_TO_CHECK), equalTo("dummy" + ITERATION_TO_CHECK));
        assertThat(x.get(9L), nullValue());
      });
    }

    restartServer();

    try (PersistentCacheManager cacheManager = createRestartableCacheManager(CacheManagerType.FULL)) {
      CLUSTER.getClusterControl().terminateOnePassive();
      Cache<Long, String> rc1 = cacheManager.createCache(CACHE1, getRestartableCacheConfig());
      Cache<Long, String> rc2 = cacheManager.createCache(CACHE2, getRestartableCacheConfig());
      Cache<Long, String> rc3 = cacheManager.createCache(CACHE3, getRestartableCacheConfig());

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(rc1);
      caches.add(rc2);
      caches.add(rc3);

      caches.forEach(x -> {
        assertThat(x.get(ITERATION_TO_CHECK), equalTo("dummy" + ITERATION_TO_CHECK));
        assertThat(x.get(9L), nullValue());
      });
    }
    destroyRestartableCacheManager();
  }
}