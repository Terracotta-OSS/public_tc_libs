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
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class RestartStoreIdentifierUsageTest extends BaseEnterpriseClusteredTest {
  @Test
  public void testSameCacheManagerWithSameFrsId() {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager1 = createRestartableCacheManagerWithRestartableCache("cm1", keyClass, valueClass, true, 1, "frsOne");
    Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);

    PersistentCacheManager cacheManager2 = createRestartableCacheManagerWithRestartableCache("cm1", keyClass, valueClass, true, 1, "frsOne");
    Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

    cache1.put(1L, "One");
    assertThat(cache1.get(1L), is("One"));
    assertThat(cache2.get(1L), is("One"));
    cacheManager1.close();
    cacheManager2.close();
  }

  @Test
  public void testMultipleCacheManagersWithSameFrsId() {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    CacheManager cacheManager = createRestartableCacheManagerWithRestartableCache("cm1", keyClass, valueClass, true, 1, "frsOne");

    try {
      createRestartableCacheManagerWithRestartableCache("cm2", keyClass, valueClass, true, 1, "frsOne");
      fail("Two cache manager with same Frs Identifier MUST not succeed on the same server");
    } catch (StateTransitionException e) {
      Assert.assertThat(e.getMessage(), containsString("Could not create"));
      Throwable rootCause = e;
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      Assert.assertThat(rootCause.getMessage(), containsString("Cannot map to another Cluster Tier Manager"));
    }
    cacheManager.close();
  }

  @Test
  public void testMultipleCacheManagerWithDifferentFrsId() {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager1 = createRestartableCacheManagerWithRestartableCache("cm1", keyClass, valueClass, true, 1, "frsOne");
    Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);

    PersistentCacheManager cacheManager2 = createRestartableCacheManagerWithRestartableCache("cm2", keyClass, valueClass, true, 1, "frsTwo");
    Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

    cache1.put(1L, "One");
    cache2.put(1L, "anotherOne");
    assertThat(cache1.get(1L), is("One"));
    assertThat(cache2.get(1L), is("anotherOne"));
    cacheManager1.close();
    cacheManager2.close();
  }
}