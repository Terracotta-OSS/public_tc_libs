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

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class BaseEnterpriseLifecycleTest extends BaseEnterpriseClusteredTest {
  @Test
  public void testRecreateRestartableCache() throws Exception {
    PersistentCacheManager cacheManager = createCacheManager("cacheManager");

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
            .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    final CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(restartablePool))
            .build();

    final Cache<Long, String> cache = cacheManager.createCache(CACHE, cacheConfiguration);
    cache.put(1L, "One");

    restartActive();

    try {
      assertThat(cache.get(1L), is("One"));

      //Create cache with same config, should fail
      cacheManager.createCache(CACHE, cacheConfiguration);
      fail("IllegalArgumentException Expected");
    } catch (IllegalArgumentException e) {
      //expected
    }
    cacheManager.close();
  }

  @Test
  public void testBasicDestroyRestartableCache() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager = createCacheManagerWithCache(keyClass, valueClass);
    final Cache<Long, String> cache = cacheManager.getCache(CACHE, keyClass, valueClass);
    cache.put(1L, "One");
    assertThat(cache.get(1L), is("One"));

    cacheManager.destroyCache(CACHE);

    restartActive();

    cacheManager.destroyCache(CACHE);
    cacheManager.close();
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  @Test
  public void testRecreateRestartableCachemanagerWithoutDestroy() throws Exception {
    PersistentCacheManager cacheManager = createCacheManager("cacheManager");

    restartActive();

    PersistentCacheManager cacheManager2 = null;
    try {
      //Trying to create un-destroyed cacheManager with a different config should fail
      cacheManager2 = createNonRestartableCacheManager("cacheManager");
      fail("CacheManager creation should fail because a CacheManager with the same name and different config already exists");
    } catch (StateTransitionException e) {
      //expected
    } finally {
      if (cacheManager2 != null) {
        cacheManager2.close();
      }
      cacheManager.close();
    }
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  @Test
  public void testRecreateNonRestartableCachemanagerWithoutDestroy() throws Exception {
    PersistentCacheManager cacheManager = createNonRestartableCacheManager("cacheManager");

    restartActive();

    PersistentCacheManager cacheManager2 = null;
    try {
      //Trying to create existing cacheManager with a different config should fail
      cacheManager2 = createCacheManager("cacheManager");
      fail("CacheManager creation should fail because a CacheManager with the same name and different config already exists");
    } catch (StateTransitionException e) {
      //expected
    } finally {
      if (cacheManager2 != null) {
        cacheManager2.close();
      }
      cacheManager.close();
    }
  }

  @Test
  public void testRecreateRestartableCachemanagerAfterDestroy() throws Exception {
    PersistentCacheManager cacheManager = createCacheManager("cacheManager");

    cacheManager.close();
    cacheManager.destroy();

    restartActive();

    //Creating a CacheManager named cacheManager with a different config should succeed
    PersistentCacheManager cacheManager2 = createNonRestartableCacheManager("cacheManager");
    if (cacheManager2 != null) {
      cacheManager2.close();
    }
  }

  @Test
  public void testRecreateNonRestartableCachemanagerAfterDestroy() throws Exception {
    PersistentCacheManager cacheManager = createNonRestartableCacheManager("cacheManager");

    cacheManager.close();
    cacheManager.destroy();

    restartActive();

    //Trying to create destroyed cacheManager with a different config should succeed
    PersistentCacheManager cacheManager2 = createCacheManager("cacheManager");
    if (cacheManager2 != null) {
      cacheManager2.close();
    }
  }

  @Test
  public void testDestroyRestartableCacheManager() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager = createCacheManagerWithCache(keyClass, valueClass);

    Cache<Long, String> cache = cacheManager.getCache(CACHE, keyClass, valueClass);
    cache.put(1L, "one");

    cacheManager.close();
    cacheManager.destroy();

    restartActive();

    PersistentCacheManager cacheManager2 = createCacheManagerWithCache(keyClass, valueClass);

    Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);
    assertThat(cache2.get(1L), is(nullValue()));

    cacheManager2.close();
  }

  @Test
  public void testDestroyNonRestartableCacheManager() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager = createNonRestartableCacheManager();

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder
            .clusteredDedicated(DEFAULT_PRIMARY_RESOURCE, 2, MemoryUnit.MB);

    Cache<Long, String> cache = cacheManager.createCache(CACHE, CacheConfigurationBuilder
            .newCacheConfigurationBuilder(keyClass, valueClass,
                    ResourcePoolsBuilder.newResourcePoolsBuilder().with(resourcePool)));
    cache.put(1L, "one");

    cacheManager.close();
    cacheManager.destroy();

    restartActive();

    PersistentCacheManager cacheManager2 = createNonRestartableCacheManager();

    Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);
    assertNull(cache2);
    cacheManager2.close();
  }

  @Test
  public void testRecreateCacheManagerAfterDestroyWithMultipleCacheManagersConfigured() throws Exception {
    PersistentCacheManager cacheManager1 = createCacheManager("cacheManager1");
    PersistentCacheManager cacheManager2 = createCacheManager("cacheManager2");

    cacheManager2.close();
    cacheManager2.destroy();

    restartActive();

    //Creating a CacheManager named cacheManager2 with a different config should succeed
    PersistentCacheManager cacheManager3 = createNonRestartableCacheManager("cacheManager2");

    if (cacheManager3 != null) {
      cacheManager3.close();
    }
    cacheManager1.close();
  }

  protected abstract PersistentCacheManager createCacheManager(String cacheManagerName);

  protected abstract <K, V> PersistentCacheManager createCacheManagerWithCache(Class<K> keyClass,
                                                                               Class<V> valueClass);
}
