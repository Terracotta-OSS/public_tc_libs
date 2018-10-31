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

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.terracottatech.ehcache.clustered.frs.CacheManagerType.FULL;
import static com.terracottatech.ehcache.clustered.frs.CacheManagerType.HYBRID;
import static java.util.Arrays.asList;
import static org.ehcache.clustered.common.Consistency.EVENTUAL;
import static org.ehcache.clustered.common.Consistency.STRONG;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class MultithreadedFailoverIT extends BaseActivePassiveClusteredTest {
  private static final int NUM_OF_THREADS = 10;
  private static final int JOB_SIZE = 100;

  @Parameters(name = "consistency={0}, cacheManagerType={1}")
  public static Iterable<Object[]> data() {
    return asList(new Object[][] {
            {STRONG, FULL},
            {STRONG, HYBRID},
            {EVENTUAL, FULL},
            {EVENTUAL, HYBRID},
    });
  }

  @SuppressWarnings("DefaultAnnotationParam")
  @Parameter(0)
  public Consistency cacheConsistency;

  @Parameter(1)
  public CacheManagerType cacheManagerType;

  @Test
  public void testCRUD() throws Exception {

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> config = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool))
            .add(ClusteredStoreConfigurationBuilder.withConsistency(cacheConsistency))
            .build();

    try (PersistentCacheManager cacheManager1 = createRestartableCacheManager("cacheManager1", cacheManagerType);
         PersistentCacheManager cacheManager2 = createRestartableCacheManager("cacheManager2", cacheManagerType)) {
      Cache<Long, String> cache1 = cacheManager1.createCache(CACHE, config);
      Cache<Long, String> cache2 = cacheManager2.createCache(CACHE, config);

      List<Cache<Long, String>> caches = new ArrayList<>();
      caches.add(cache1);
      caches.add(cache2);

      Random random = new Random();
      final String value = "value";

      Set<Long> universalSet = Collections.newSetFromMap(new ConcurrentHashMap<>());

      ExecutorService executorService = Executors.newWorkStealingPool(NUM_OF_THREADS);

      List<Future<?>> futures = new ArrayList<>();

      caches.forEach(cache -> {
        for (int i = 0; i < NUM_OF_THREADS; i++) {
          futures.add(executorService.submit(() -> random.longs().limit(JOB_SIZE).forEach(x -> {
            cache.put(x, value + x);
            universalSet.add(x);
          })));
        }
      });

      shutdownActiveAndWaitForPassiveToBecomeActive();

      // Ensure that all puts have succeeded
      for (Future<?> f : futures) {
        f.get();
      }

      caches.forEach(cache -> {
        for (int i = 0; i < NUM_OF_THREADS; i++) {
          futures.add(executorService.submit(() -> universalSet.forEach(x -> assertThat(cache.get(x), is(value + x)))));
        }
      });
    }
    destroyCacheManager("cacheManager1");
    destroyCacheManager("cacheManager2");
  }
}
