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
import org.ehcache.StateTransitionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class EnterpriseClusteredCacheManagerLifecycleIT extends BaseActiveClusteredTest {
  @Parameterized.Parameters(name = "cacheManagerType={0}")
  public static CacheManagerType[] type() {
    return new CacheManagerType[] { CacheManagerType.FULL, CacheManagerType.HYBRID };
  }

  @Parameterized.Parameter
  public CacheManagerType cacheManagerType;

  @Test
  public void testDestroyRestartableCacheManager() throws Exception {
    Class<Long> keyClass = Long.class;
    Class<String> valueClass = String.class;

    PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, cacheManagerType);

    Cache<Long, String> cache = cacheManager.getCache(CACHE, keyClass, valueClass);
    cache.put(1L, "one");

    cacheManager.close();
    cacheManager.destroy();

    restartActive();

    PersistentCacheManager cacheManager2 = createRestartableCacheManagerWithRestartableCache(keyClass, valueClass, cacheManagerType);

    Cache<Long, String> cache2 = cacheManager2.getCache(CACHE, keyClass, valueClass);
    assertThat(cache2.get(1L), is(nullValue()));

    cacheManager2.close();
    cacheManager2.destroy();
  }

  @Test
  public void testRecreateCacheManagerAfterDestroyWithMultipleCacheManagersConfigured() throws Exception {
    PersistentCacheManager cacheManager1 = createRestartableCacheManager(cacheManagerType);
    PersistentCacheManager cacheManager2 = createRestartableCacheManager("cacheManager2", cacheManagerType);

    cacheManager2.close();
    cacheManager2.destroy();

    restartActive();

    //Creating a CacheManager named cacheManager2 with a different config should succeed
    PersistentCacheManager cacheManager3 = createNonRestartableCacheManager("cacheManager2");

    cacheManager1.close();
    cacheManager1.destroy();
    cacheManager3.close();
    cacheManager3.destroy();
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  @Test
  public void testRecreateRestartableCachemanagerWithoutDestroy() throws Exception {
    PersistentCacheManager cacheManager = createRestartableCacheManager("cacheManager", cacheManagerType);
    cacheManager.close();

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
      cacheManager.destroy();
    }
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  @Test
  public void testRecreateNonRestartableCachemanagerWithoutDestroy() throws Exception {
    PersistentCacheManager cacheManager = createNonRestartableCacheManager("cacheManager");
    cacheManager.close();

    restartActive();

    PersistentCacheManager cacheManager2 = null;
    try {
      //Trying to create existing cacheManager with a different config should fail
      cacheManager2 = createRestartableCacheManager("cacheManager", cacheManagerType);
      fail("CacheManager creation should fail because a CacheManager with the same name and different config already exists");
    } catch (StateTransitionException e) {
      //expected
    } finally {
      if (cacheManager2 != null) {
        cacheManager2.close();
      }
      cacheManager.destroy();
    }
  }

  @Test
  public void testRecreateRestartableCachemanagerAfterDestroy() throws Exception {
    PersistentCacheManager cacheManager = createRestartableCacheManager("cacheManager", cacheManagerType);

    cacheManager.close();
    cacheManager.destroy();

    restartActive();

    //Creating a CacheManager named cacheManager with a different config should succeed
    PersistentCacheManager cacheManager2 = createNonRestartableCacheManager("cacheManager");
    cacheManager2.close();
    cacheManager2.destroy();
  }

  @Test
  public void testRecreateNonRestartableCachemanagerAfterDestroy() throws Exception {
    PersistentCacheManager cacheManager = createNonRestartableCacheManager("cacheManager");

    cacheManager.close();
    cacheManager.destroy();

    restartActive();

    //Trying to create destroyed cacheManager with a different config should succeed
    PersistentCacheManager cacheManager2 = createRestartableCacheManager("cacheManager", cacheManagerType);
    cacheManager2.close();
    cacheManager2.destroy();
  }
}
