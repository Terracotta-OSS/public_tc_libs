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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class EnterpriseClusteredRestartabilityWithActivePassiveIT extends BaseActivePassiveClusteredTest {
  private Class<Long> keyClass = Long.class;
  private Class<String> valueClass = String.class;

  @Parameterized.Parameters(name = "{0}")
  public static CacheManagerType[] type() {
    return new CacheManagerType[] { CacheManagerType.FULL, CacheManagerType.HYBRID };
  }

  @Parameterized.Parameter
  public CacheManagerType cacheManagerType;

  @Test
  public void testRestartabilityWithMultipleCacheManagers() throws Exception {

    try (PersistentCacheManager cacheManager1 =
             createRestartableCacheManagerWithRestartableCache("cacheManager1", keyClass, valueClass, cacheManagerType);
         PersistentCacheManager cacheManager2 =
             createRestartableCacheManagerWithRestartableCache("cacheManager2", keyClass, valueClass, cacheManagerType)) {
      final Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);
      final Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

      cache1.put(1L, "One");
      cache2.put(1L, "Another One");

      shutdownActiveAndWaitForPassiveToBecomeActive();

      //Do some PUTs when we only have 1 server running
      cache1.put(2L, "Two");
      cache2.put(2L, "Another Two");

      //Start the previously shutdown Active, which will become Passive now
      CLUSTER.getClusterControl().startOneServer();

      //Wait for Passive to come up
      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

      //Do some PUTs after Passive has come up (and the original Active-Passive have reversed their roles)
      cache1.put(3L, "Three");
      cache2.put(3L, "Another Three");

      //Overwrite some keys
      cache1.put(1L, "New One");
      cache2.put(1L, "Another New One");
    }

    //Restart all servers and test FRS
    CLUSTER.getClusterControl().terminateAllServers();
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    try (PersistentCacheManager cacheManager1 =
             createRestartableCacheManagerWithRestartableCache("cacheManager1", keyClass, valueClass, cacheManagerType);
         PersistentCacheManager cacheManager2 =
             createRestartableCacheManagerWithRestartableCache("cacheManager2", keyClass, valueClass, cacheManagerType)) {
      final Cache<Long, String> cache1 = cacheManager1.getCache(CACHE, keyClass, valueClass);
      final Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);

      assertThat(cache1.get(1L), equalTo("New One"));
      assertThat(cache2.get(1L), equalTo("Another New One"));

      assertThat(cache1.get(2L), equalTo("Two"));
      assertThat(cache2.get(2L), equalTo("Another Two"));

      assertThat(cache1.get(3L), equalTo("Three"));
      assertThat(cache2.get(3L), equalTo("Another Three"));
    }
    destroyCacheManager("cacheManager1");
    destroyCacheManager("cacheManager2");
  }
}