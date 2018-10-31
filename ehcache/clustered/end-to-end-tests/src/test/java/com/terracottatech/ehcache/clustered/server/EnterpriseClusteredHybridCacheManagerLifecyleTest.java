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

import org.ehcache.PersistentCacheManager;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class EnterpriseClusteredHybridCacheManagerLifecyleTest extends BaseEnterpriseLifecycleTest {
  @Test
  public void testRecreateCacheManagerAfterDestroyWithHybridAndRestartableSameRoot() throws Exception {
    {
      PersistentCacheManager hybridCacheManager = createRestartableCacheManagerWithRestartableCache("hybrid-cm",
          Long.class, String.class, true);
      PersistentCacheManager restartableCacheManager = createRestartableCacheManagerWithRestartableCache(
          "restartable-cm", Long.class, String.class, false);
      PersistentCacheManager morphingCacheManager = createRestartableCacheManager("morphing-cm", false);

      hybridCacheManager.getCache(CACHE, Long.class, String.class).put(1L, "testString");
      restartableCacheManager.getCache(CACHE, Long.class, String.class).put(10L, "t1");

      morphingCacheManager.close();
      morphingCacheManager.destroy();
      restartActive();
    }

    {
      PersistentCacheManager nonRestartableCacheManager = createNonRestartableCacheManager("morphing-cm");
      nonRestartableCacheManager.close();

      PersistentCacheManager hybridCacheManager = createRestartableCacheManagerWithRestartableCache("hybrid-cm",
          Long.class, String.class, true);
      PersistentCacheManager restartableCacheManager = createRestartableCacheManagerWithRestartableCache(
          "restartable-cm", Long.class, String.class, false);

      assertThat(hybridCacheManager.getCache(CACHE, Long.class, String.class).get(1L), is("testString"));
      assertThat(restartableCacheManager.getCache(CACHE, Long.class, String.class).get(10L), is("t1"));
    }
  }

  @Test
  public void testRecreateCacheManagerAfterDestroyWithHybridAndRestartableDifferentRoot() throws Exception {
    {
      PersistentCacheManager hybridCacheManager = createRestartableCacheManagerWithRestartableCache("hybrid-cm",
          Long.class, Long.class, true, 1);
      PersistentCacheManager restartableCacheManager = createRestartableCacheManagerWithRestartableCache(
          "restartable-cm", Long.class, String.class, false, 2);
      createRestartableCacheManager();

      hybridCacheManager.getCache(CACHE, Long.class, Long.class).put(1L, 1L);
      restartableCacheManager.getCache(CACHE, Long.class, String.class).put(10L, "t***");

      restartActive();
    }

    {
      PersistentCacheManager hybridCacheManager = createRestartableCacheManagerWithRestartableCache("hybrid-cm",
          Long.class, Long.class, true, 1);
      PersistentCacheManager restartableCacheManager = createRestartableCacheManagerWithRestartableCache(
          "restartable-cm", Long.class, String.class, false, 2);

      assertThat(hybridCacheManager.getCache(CACHE, Long.class, Long.class).get(1L), is(1L));
      assertThat(restartableCacheManager.getCache(CACHE, Long.class, String.class).get(10L), is("t***"));
    }
  }

  @Override
  protected PersistentCacheManager createCacheManager(String cacheManagerName) {
    return createRestartableCacheManager(cacheManagerName, true);
  }

  @Override
  protected <K, V> PersistentCacheManager createCacheManagerWithCache(Class<K> keyClass, Class<V> valueClass) {
    return createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, true);
  }
}
