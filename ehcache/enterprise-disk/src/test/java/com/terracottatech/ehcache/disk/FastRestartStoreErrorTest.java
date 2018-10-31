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
import org.ehcache.StateTransitionException;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.ehcache.disk.config.builders.EnterpriseDiskResourcePoolBuilder;

import java.io.File;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;

/**
 * Error Test cases for FRS backed offheap caches.
 *
 * @author RKAV
 */
public class FastRestartStoreErrorTest {

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();
  private File currentFolder;

  @Before
  public void setup() throws Exception {
    currentFolder = tmpFolder.newFolder();
  }

  @Test(expected = StateTransitionException.class)
  public void testWrongTierCombination() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> frsCacheManagerBuilder = newCacheManagerBuilder().
        with(CacheManagerBuilder.persistence(currentFolder.getAbsolutePath())).
        withCache("test-cache", newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(1, MemoryUnit.MB).
                with(EnterpriseDiskResourcePoolBuilder.diskRestartable(2, MemoryUnit.MB))));
    frsCacheManagerBuilder.build(true);
  }
}