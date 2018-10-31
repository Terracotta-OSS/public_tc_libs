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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class EnterpriseClusteredCacheLifecycleIT extends BaseActiveClusteredTest {
  @Parameterized.Parameters(name = "cacheManagerType={0}")
  public static CacheManagerType[] type() {
    return new CacheManagerType[] { CacheManagerType.FULL, CacheManagerType.HYBRID };
  }

  @Parameterized.Parameter
  public CacheManagerType cacheManagerType;

  @Test
  public void testRecreateRestartableCache() throws Exception {
    PersistentCacheManager cacheManager = createRestartableCacheManager(cacheManagerType);

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(restartablePool))
            .build();

    Cache<Long, String> cache = cacheManager.createCache(CACHE, cacheConfiguration);
    cache.put(1L, "One");
    cacheManager.close();

    restartActive();

    cacheManager.init();
    cache = cacheManager.getCache(CACHE, Long.class, String.class);
    assertThat(cache.get(1L), is("One"));

    try {
      //Create cache with same config, should fail
      cacheManager.createCache(CACHE, cacheConfiguration);
      fail("IllegalArgumentException Expected");
    } catch (IllegalArgumentException e) {
      //expected
    }
    cacheManager.close();
    cacheManager.destroy();
  }

  @Test
  public void testBasicDestroyRestartableCache() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, cacheManagerType);

    Cache<Long, String> cache = cacheManager.getCache(CACHE, keyClass, valueClass);
    cache.put(1L, "One");
    assertThat(cache.get(1L), is("One"));

    cacheManager.destroyCache(CACHE);
    cacheManager.close();

    restartActive();

    cacheManager.destroyCache(CACHE);
    cacheManager.destroy();
  }
}
