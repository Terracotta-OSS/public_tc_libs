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
import org.ehcache.config.CacheConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class DestroyLoopIT extends BaseActiveClusteredTest {
  @Parameterized.Parameters(name = "cacheManagerType={0}")
  public static CacheManagerType[] data() {
    return CacheManagerType.values();
  }

  @Parameterized.Parameter
  public CacheManagerType cacheManagerType;

  @Test
  public void testDestroyLoopSingleCM() throws Exception {
    destroyLoop(true,false,3, 1,
        s -> createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType),
        () -> {
          destroyRestartableCacheManagerWithCache();
          return null;
        });
    restartActive();
    destroyLoop(true,true,1, 1,
        s -> createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType),
        () -> {
          destroyRestartableCacheManagerWithCache();
          return null;
        });
  }

  @Test
  public void testDestroyLoopMultiClientSameCM() throws Exception {
    destroyLoop(true,false,3, 2,
        s -> createRestartableCacheManagerWithRestartableCache(Long.class, String.class, cacheManagerType),
        () -> {
          destroyRestartableCacheManagerWithCache();
          return null;
        });
    destroyRestartableCacheManagerWithCache();
  }

  @Test
  public void testDestroyLoopMultiCM() throws Exception {
    final List<String> cacheManagerNames = new ArrayList<>();
    destroyLoop(false,false,3, 2,
        s -> trackedCacheManagerCreate(false, s, cacheManagerNames),
        () -> {
          destroyAll(cacheManagerNames);
          return null;
        }
    );
    cacheManagerNames.clear();
    restartActive();
    destroyLoop(false,true,1, 2,
        s -> trackedCacheManagerCreate(false, s, cacheManagerNames),
        () -> {
          destroyAll(cacheManagerNames);
          return null;
        });
  }

  @Test
  public void testDestroyLoopMixedCM() throws Exception {
    final List<String> cacheManagerNames = new ArrayList<>();
    destroyLoop(false,false,3, 2,
        s -> trackedCacheManagerCreate(true, s, cacheManagerNames),
        () -> {
          destroyAll(cacheManagerNames);
          return null;
        });
    destroyAll(cacheManagerNames);
  }

  private PersistentCacheManager trackedCacheManagerCreate(boolean mixed, int s, List<String> cacheManagerNameList) {
    String cmName = "cm" + cleanedTestName() + s;
    cacheManagerNameList.add(cmName);
    return !mixed || s % 2 == 0 ?
        createRestartableCacheManagerWithRestartableCache(cmName, Long.class, String.class, cacheManagerType) :
        createNonRestartableCacheManagerWithCache(cmName, 4, Long.class, String.class);
  }

  private void destroyAll(List<String> cacheManagerNameList) throws Exception {
    for (String s : cacheManagerNameList) {
      destroyCacheManager(s);
    }
  }

  private void destroyLoop(boolean sameCM, boolean exists, int numIterations, int numCM,
                           Function<Integer, PersistentCacheManager> cmCreator,
                           Callable<Void> cmDestroyer) throws Exception {
    for (int i = 0; i < numIterations; i++) {
      final boolean firstIteration = i == 0;
      final boolean lastIteration = i == numIterations - 1;
      final boolean read = firstIteration && exists;
      try (CacheManagerContainer ccm = new CacheManagerContainer(numCM, cmCreator)) {
        AtomicBoolean firstCM = new AtomicBoolean(true);
        ccm.cacheManagerList.forEach(cm -> {
          Cache<Long, String> cache = cm.getCache(CACHE, Long.class, String.class);
          if (read && (!sameCM || firstCM.get())) {
            assertThat(cache.get(1L), is("yet another one"));
            assertThat(cache.remove(1L, "yet another one"), is(true));
          }
          if (sameCM && !firstCM.get()) {
            assertThat(cache.remove(1L, "yet another one"), is(true));
          }
          assertThat(cache.putIfAbsent(1L, "one"), nullValue());
          assertThat(cache.replace(1L, "one", "yet another one"), is(true));
          firstCM.set(false);
        });
      }

      // destroy all but one. if exists is true destroy all
      if (exists || !lastIteration) {
        cmDestroyer.call();
      } else {
        int maxIdx = sameCM ? 1 : numCM;
        for (int k = 0; k < maxIdx; k++) {
          // all cache managers closed..open and destroy cache..
          try (PersistentCacheManager cm = cmCreator.apply(k)) {
            // if it is not the last
            @SuppressWarnings("unchecked")
            CacheConfiguration<Long, String> cacheConfiguration = (CacheConfiguration<Long, String>)
                cm.getRuntimeConfiguration().getCacheConfigurations().get(CACHE);
            cm.destroyCache(CACHE);
            Cache<Long, String> cache = cm.createCache(CACHE, cacheConfiguration);
            assertThat(cache.putIfAbsent(1L, "one"), nullValue());
            assertThat(cache.replace(1L, "one", "yet another one"), is(true));
          }
        }
      }
    }
  }

  private static class CacheManagerContainer implements AutoCloseable {
    private final List<PersistentCacheManager> cacheManagerList;

    private CacheManagerContainer(int numCacheManagers, Function<Integer, PersistentCacheManager> cmFunction) {
      List<PersistentCacheManager> cm = new ArrayList<>();
      for (int i = 0; i < numCacheManagers; i++) {
        cm.add(cmFunction.apply(i));
      }
      cacheManagerList = Collections.unmodifiableList(cm);
    }

    @Override
    public void close() {
      cacheManagerList.forEach(PersistentCacheManager::close);
    }
  }
}