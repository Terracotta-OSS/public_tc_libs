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
package com.terracottatech.ehcache.disk;

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;

/**
 * Base class for all run time cache manager configuration based tests.
 *
 * @author RKAV
 */
public abstract class AbstractFastRestartStoreTest extends AbstractEnterpriseDiskCacheTest {
  private final String[] TEST_CACHES = {"ee-cache1", "ee-cache2"};

  protected PersistentCacheManager buildCacheManager(String... testCaches) {
    CacheManagerBuilder<PersistentCacheManager> frsCacheManagerBuilder = newCacheManagerBuilder().
        with(CacheManagerBuilder.persistence(currentFolder.getAbsolutePath()));

    for (String cacheName : testCaches) {
      int cacheIdx = Integer.parseInt(cacheName.substring(cacheName.length() - 1));
      ResourcePoolsBuilder resourcePoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder();
      resourcePoolsBuilder = buildTestResourcePool(cacheIdx, resourcePoolsBuilder);
      frsCacheManagerBuilder = frsCacheManagerBuilder.withCache(cacheName,
          newCacheConfigurationBuilder(Long.class, String.class, resourcePoolsBuilder));
    }
    return frsCacheManagerBuilder.build(true);
  }

  String[] getTestCaches() {
    return TEST_CACHES;
  }

  String getCache(int idx) {
    return TEST_CACHES[idx];
  }

  abstract ResourcePoolsBuilder buildTestResourcePool(int cacheIdx, ResourcePoolsBuilder resourcePoolsBuilder);
}
