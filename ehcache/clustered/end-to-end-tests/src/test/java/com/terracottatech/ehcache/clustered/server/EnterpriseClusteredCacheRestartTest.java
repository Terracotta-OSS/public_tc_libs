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
package com.terracottatech.ehcache.clustered.server;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class EnterpriseClusteredCacheRestartTest extends BaseEnterpriseClusteredTest {
  @Parameterized.Parameters(name = "cacheManagerType={0}")
  public static Iterable<Boolean> data() {
    return asList(false, true);
  }

  @Parameterized.Parameter
  public boolean isHybrid;

  @Test
  public void testRestartableCacheManager() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, isHybrid);
    Cache<Long, String> cache = cacheManager.getCache(CACHE, keyClass, valueClass);

    cache.put(1L, "One");
    assertThat(cache.get(1L), is("One"));

    restartActive();

    assertThat(cache.get(1L), is("One"));
    cacheManager.close();
    testSimpleCacheManagerWithRestartConfig(Long.class, String.class, isHybrid, (cache1) -> {
      cache1.put(10L, "Value1");
      assertThat(cache1.get(10L), is("Value1"));
    }, (cache2) -> assertThat(cache2.get(10L), is("Value1")));
  }

  @Test
  public void testRestartableCacheManagerWithMultipleRestartableCaches() throws Exception {
    testMultipleWithRestartConfig(Long.class, String.class, isHybrid,
            (caches) -> {
              final AtomicLong k = new AtomicLong(1L);
              caches.forEach((cache) -> {
                final long key = k.getAndAdd(10L);
                final String val = "Value" + key;
                assertNull(cache.putIfAbsent(key, val));
                assertThat(cache.putIfAbsent(key, val), is(val));
                assertNull(cache.get(9999L));
              });
            },
            (caches) -> {
              final AtomicLong k = new AtomicLong(1L);
              caches.forEach((cache) -> {
                final long key = k.getAndAdd(10L);
                final String val = "Value" + key;
                assertThat(cache.get(key), is(val));
                assertNull(cache.get(9999L));
              });
            });
  }

  @Test
  public void testConfigurationIsPersisted() throws Exception {
    //Path corresponding to root1
    final String path = PassthroughTestHelper.getDataDirectories().getDirectory().get(0).getValue();

    PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(Long.class, String.class, isHybrid);

    assertTrue(Files.exists(Paths.get(path, STRIPE0, EHCACHE_STORAGE_SPACE, FRS_CONTAINER, METADATA_DIR_NAME)));
    assertEquals(1L, countDirEntries(path));

    restartActive();

    //Files should still be present
    assertTrue(Files.exists(Paths.get(path, STRIPE0, EHCACHE_STORAGE_SPACE, FRS_CONTAINER, METADATA_DIR_NAME)));
    assertEquals(1L, countDirEntries(path));

    cacheManager.close();
  }

  private long countDirEntries(String dirName) throws IOException {
    try (Stream<Path> list = Files.list(Paths.get(dirName))) {
      return list.count();
    }
  }

  private <K, V> void testMultipleWithRestartConfig(Class<K> keyClass,
                                                    Class<V> valueClass,
                                                    boolean hybrid,
                                                    Consumer<List<Cache<K, V>>> cacheConsumer1,
                                                    Consumer<List<Cache<K, V>>> cacheConsumer2) throws Exception {
    final String testCacheName1 = "cache-1";
    final String testCacheName2 = "cache-2";
    final String testCacheName3 = "cache-3";

    RestartableOffHeapMode restartableOffHeapMode;

    final ClusteredResourcePool pool1;
    final ClusteredResourcePool pool2;
    final ClusteredResourcePool pool3;

    if (hybrid) {
      // as of now we cannot do non restartable cache and conduct restart tests
      pool1 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(8, MemoryUnit.MB, 50);
      pool2 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(4, MemoryUnit.MB, 0);
      pool3 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(4, MemoryUnit.MB);
      restartableOffHeapMode = RestartableOffHeapMode.PARTIAL;
    } else {
      pool1 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(8, MemoryUnit.MB);
      pool2 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_A);
      pool3 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_B);
      restartableOffHeapMode = RestartableOffHeapMode.FULL;
    }

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
            CacheManagerBuilder.newCacheManagerBuilder()
                    .with(enterpriseCluster(URI.create(STRIPE_URI + "/cacheManager"))
                            .autoCreate()
                            .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
                            .resourcePool(SHARED_POOL_A, 4, MemoryUnit.MB, SECONDARY_RESOURCE)
                            .resourcePool(SHARED_POOL_B, 8, MemoryUnit.MB)
                            .restartable(DATA_ROOT_ID)
                            .withRestartableOffHeapMode(restartableOffHeapMode))
                    .withCache(testCacheName1, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClass, valueClass,
                            ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool1)))
                    .withCache(testCacheName2, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClass, valueClass,
                            ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool2)))
                    .withCache(testCacheName3, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClass, valueClass,
                            ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool3)));

    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final List<Cache<K, V>> caches = new ArrayList<>();

    caches.add(cacheManager.getCache(testCacheName1, keyClass, valueClass));
    caches.add(cacheManager.getCache(testCacheName2, keyClass, valueClass));
    caches.add(cacheManager.getCache(testCacheName3, keyClass, valueClass));

    cacheConsumer1.accept(caches);

    restartActive();

    cacheConsumer2.accept(caches);
    cacheManager.close();
  }

  private <K, V> void testSimpleCacheManagerWithRestartConfig(Class<K> keyClass, Class<V> valueClass, boolean hybrid,
                                                              Consumer<Cache<K, V>> cacheConsumer1,
                                                              Consumer<Cache<K, V>> cacheConsumer2) throws Exception {
    final PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass,
        hybrid);
    final Cache<K, V> cache = cacheManager.getCache(CACHE, keyClass, valueClass);

    cacheConsumer1.accept(cache);

    restartActive();

    cacheConsumer2.accept(cache);
    cacheManager.close();
  }
}
