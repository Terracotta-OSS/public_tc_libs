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
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class EnterpriseClusteredRestartabilityIT extends BaseActiveClusteredTest {
  @Parameterized.Parameters(name = "cacheManagerType={0}")
  public static CacheManagerType[] type() {
    return new CacheManagerType[] { CacheManagerType.FULL };
  }

  @Parameterized.Parameter
  public CacheManagerType cacheManagerType;

  @Test
  public void testRestartabilityWithOneCacheManager() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, cacheManagerType);

    Cache<Long, String> cache = cacheManager.getCache(CACHE, keyClass, valueClass);

    cache.put(1L, "One");
    assertThat(cache.putIfAbsent(2L, "Two"), nullValue());

    cacheManager.close();

    restartActive();

    cacheManager.init();
    cache = cacheManager.getCache(CACHE, keyClass, valueClass);

    assertThat(cache.get(1L), is("One"));
    assertThat(cache.get(2L), is("Two"));
    cacheManager.close();
    cacheManager.destroy();
  }

  @Test
  public void testRestartabilityWithMultipleCacheManagers() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    EnterpriseServerSideConfigurationBuilder serverSideConfigBuilder2 = EnterpriseClusteringServiceConfigurationBuilder
            .enterpriseCluster(getURI().resolve("/cacheManager2"))
            .autoCreate()
            .restartable("root")
            .withRestartableOffHeapMode(RestartableOffHeapMode.FULL);

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder2 = CacheManagerBuilder
            .newCacheManagerBuilder()
            .with(serverSideConfigBuilder2)
            .withCache(CACHE, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool)));

    PersistentCacheManager cacheManager1 = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, cacheManagerType);
    PersistentCacheManager cacheManager2 = cacheManagerBuilder2.build(true);
    watcher.addCacheManager(cacheManager2);

    Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);
    Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

    cache1.put(1L, "One");

    cache2.put(1L, "Another One");

    assertThat(cache1.get(1L), equalTo("One"));
    assertThat(cache2.get(1L), equalTo("Another One"));

    cacheManager1.close();
    cacheManager2.close();

    restartActive();

    cacheManager1.init();
    cacheManager2.init();

    cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);
    cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

    assertThat(cache1.get(1L), equalTo("One"));
    assertThat(cache2.get(1L), equalTo("Another One"));

    cacheManager1.close();
    cacheManager2.close();
    cacheManager1.destroy();
    cacheManager2.destroy();
  }

  @Test
  public void testRestartabilityWithClusteredCacheManager() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager1 = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, cacheManagerType);
    PersistentCacheManager cacheManager2 = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, cacheManagerType);

    Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);
    Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

    cache1.put(1L, "One");
    assertThat(cache2.get(1L), equalTo("One"));

    cache2.put(2L, "Two");
    assertThat(cache1.get(2L), equalTo("Two"));

    cacheManager1.close();
    cacheManager2.close();

    restartActive();

    cacheManager1.init();
    cacheManager2.init();

    cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);
    cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

    assertThat(cache1.get(1L), equalTo("One"));
    assertThat(cache1.get(2L), equalTo("Two"));
    assertThat(cache2.get(1L), equalTo("One"));
    assertThat(cache2.get(2L), equalTo("Two"));

    cacheManager1.close();
    cacheManager2.close();
    cacheManager1.destroy();
  }

  @Test
  public void testRestartabilityWithDifferentCacheConfigurations() throws Exception {
    final String testCacheName1 = "dedicated-cache";
    final String testCacheName2 = "shared-cache-1";
    final String testCacheName3 = "shared-cache-2";

    final String SHARED_POOL_A = "resource-pool-a";
    final String SHARED_POOL_B = "resource-pool-b";

    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    DedicatedClusteredResourcePool dedicatedRestartablePool =
            ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(2, MemoryUnit.MB);
    SharedClusteredResourcePool sharedRestartablePool1 =
            ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_A);
    SharedClusteredResourcePool sharedRestartablePool2 =
            ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_B);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
            CacheManagerBuilder.newCacheManagerBuilder()
                    .with(enterpriseCluster(getURI().resolve("/cacheManager"))
                            .autoCreate()
                            .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
                            .resourcePool(SHARED_POOL_A, 2, MemoryUnit.MB, SECONDARY_RESOURCE)
                            .resourcePool(SHARED_POOL_B, 4, MemoryUnit.MB)
                            .restartable("root")
                            .withRestartableOffHeapMode(RestartableOffHeapMode.FULL))
                    .withCache(testCacheName1, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClass, valueClass,
                            ResourcePoolsBuilder.newResourcePoolsBuilder().with(dedicatedRestartablePool)))
                    .withCache(testCacheName2, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClass, valueClass,
                            ResourcePoolsBuilder.newResourcePoolsBuilder().with(sharedRestartablePool1)))
                    .withCache(testCacheName3, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClass, valueClass,
                            ResourcePoolsBuilder.newResourcePoolsBuilder().with(sharedRestartablePool2)));

    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);
    watcher.addCacheManager(cacheManager);

    Cache<Long, String> cache1 = cacheManager.getCache(testCacheName1, keyClass, valueClass);
    Cache<Long, String> cache2 = cacheManager.getCache(testCacheName2, keyClass, valueClass);
    Cache<Long, String> cache3 = cacheManager.getCache(testCacheName3, keyClass, valueClass);

    cache1.put(1L, "One");
    cache2.put(2L, "Two");
    cache3.put(3L, "Three");

    cacheManager.close();

    restartActive();

    cacheManager.init();

    cache1 = cacheManager.getCache(testCacheName1, keyClass, valueClass);
    cache2 = cacheManager.getCache(testCacheName2, keyClass, valueClass);
    cache3 = cacheManager.getCache(testCacheName3, keyClass, valueClass);

    assertThat(cache1.get(1L), is("One"));
    assertThat(cache2.get(2L), is("Two"));
    assertThat(cache3.get(3L), is("Three"));

    cacheManager.close();
    cacheManager.destroy();
  }

  @Test
  public void testRestartabilityWithMixedCacheConfigurations() throws Exception {
    final String restartableCacheName1 = "dedicated-restartable-cache";
    final String restartableCacheName2 = "shared-restartable-cache-1";
    final String nonRestartableCacheName = "shared-non-restartable-cache";

    DedicatedClusteredResourcePool dedicatedRestartablePool =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(2, MemoryUnit.MB);
    SharedClusteredResourcePool sharedRestartablePool =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_A);
    SharedClusteredResourcePool sharedNonRestartablePool2 =
        ClusteredResourcePoolBuilder.clusteredShared(SHARED_POOL_B);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(enterpriseCluster(getURI().resolve("/cacheManager"))
                .autoCreate()
                .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
                .resourcePool(SHARED_POOL_A, 2, MemoryUnit.MB, SECONDARY_RESOURCE)
                .resourcePool(SHARED_POOL_B, 4, MemoryUnit.MB)
                .restartable("root")
                .withRestartableOffHeapMode(RestartableOffHeapMode.FULL))
            .withCache(restartableCacheName1, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(dedicatedRestartablePool)))
            .withCache(restartableCacheName2, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(sharedRestartablePool)))
            .withCache(nonRestartableCacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(sharedNonRestartablePool2)));

    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);
    watcher.addCacheManager(cacheManager);

    Cache<Long, String> restartableCache1 = cacheManager.getCache(restartableCacheName1, Long.class, String.class);
    Cache<Long, String> restartableCache2 = cacheManager.getCache(restartableCacheName2, Long.class, String.class);
    Cache<Long, String> nonRestartableCache = cacheManager.getCache(nonRestartableCacheName, Long.class, String.class);

    restartableCache1.put(1L, "One");
    restartableCache2.put(2L, "Two");
    nonRestartableCache.put(3L, "Three");
    cacheManager.close();

    restartActive();

    cacheManager.init();
    restartableCache1 = cacheManager.getCache(restartableCacheName1, Long.class, String.class);
    restartableCache2 = cacheManager.getCache(restartableCacheName2, Long.class, String.class);
    nonRestartableCache = cacheManager.getCache(nonRestartableCacheName, Long.class, String.class);

    assertThat(restartableCache1.get(1L), is("One"));
    assertThat(restartableCache2.get(2L), is("Two"));
    assertNull(nonRestartableCache.get(3L));

    cacheManager.close();
    cacheManager.destroy();
  }

  @Test
  public void testRestartabilityWithNonRestartableCacheConfigurations() throws Exception {
    PersistentCacheManager cacheManager = createNonRestartableCacheManager("non-restartable-cm");
    DedicatedClusteredResourcePool dedicatedPool =
        ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.MB);
    Cache<Long, String> testCache = cacheManager.createCache("testCache",
        newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(dedicatedPool)));
    testCache.put(1L, "one");
    assertThat(testCache.get(1L), is("one"));
    cacheManager.close();

    restartActive();

    cacheManager.init();

    testCache = cacheManager.getCache("testCache", Long.class, String.class);
    assertNull(testCache.get(1L));

    cacheManager.close();
    cacheManager.destroy();
  }
}
