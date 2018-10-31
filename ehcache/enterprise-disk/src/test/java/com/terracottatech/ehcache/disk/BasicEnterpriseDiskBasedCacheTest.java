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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.ehcache.disk.config.builders.EnterpriseDiskResourcePoolBuilder;

import java.io.File;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Simple cache construction tests for enterprise disk based caches.
 *
 * @author RKAV
 */
public class BasicEnterpriseDiskBasedCacheTest {
  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();
  private File currentFolder;

  @Before
  public void setup() throws Exception {
    currentFolder = tmpFolder.newFolder();
  }

  @Test
  public void enterpriseCacheManagerWithoutPersistenceTest() throws Exception {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("normal-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)))
        .build(true);

    try {
      // the following should fail as it is not a persistent cache..
      cacheManager.createCache("myFrsCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder().with(
                  EnterpriseDiskResourcePoolBuilder.diskRestartable(2, MemoryUnit.MB))).build());
      fail("Unexpected creation of FRS cache");
    } catch (IllegalStateException ilex) {
      assertThat(ilex.getCause().getMessage(), containsString("No service found for persistable resource"));
    }

    cacheManager.removeCache("normal-cache");
    cacheManager.close();
  }


  @Test
  public void enterpriseCacheManagerWithPersistenceTest() throws Exception {
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(CacheManagerBuilder.persistence(currentFolder.getAbsolutePath()))
        .withCache("disk-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(10, MemoryUnit.MB, true))
        )
        .build(true);

    Cache<Long, String> myDiskCache = persistentCacheManager.getCache("disk-cache", Long.class, String.class);

    Cache<Long, String> myFrsCache = persistentCacheManager.createCache("frs-cache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(
                EnterpriseDiskResourcePoolBuilder.diskRestartable(2, MemoryUnit.MB))).build());

    myDiskCache.put(1L, "testDisk1");
    assertThat(myDiskCache.get(1L), is("testDisk1"));
    assertNull(myFrsCache.get(1L));
    myFrsCache.put(1L, "testFrs1");
    assertThat(myFrsCache.get(1L), is("testFrs1"));
    assertThat(myDiskCache.get(1L), is("testDisk1"));
    persistentCacheManager.removeCache("disk-cache");
    assertThat(myFrsCache.get(1L), is("testFrs1"));
    myFrsCache.put(2L, "testFrs2");
    assertThat(myFrsCache.get(2L), is("testFrs2"));
    persistentCacheManager.removeCache("frs-cache");
    try {
      myFrsCache.get(1L);
      fail("FRS Cache is removed. Get should fail");
    } catch (Exception ignored) {
    }
    persistentCacheManager.close();
    persistentCacheManager.destroy();
  }
}