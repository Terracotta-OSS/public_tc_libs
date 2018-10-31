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
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Abstract class to test different enterprise ready cache managers.
 *
 * @author RKAV
 */
public abstract class AbstractEnterpriseDiskCacheTest {
  private PersistentCacheManager cacheManagerUnderTest;

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();
  File currentFolder;

  @Before
  public void setup() throws Exception {
    currentFolder = tmpFolder.newFolder();
    cacheManagerUnderTest = buildCacheManager(getTestCaches());
  }

  @After
  public void tearDown() throws CachePersistenceException {
    if (cacheManagerUnderTest != null) {
      if (cacheManagerUnderTest.getStatus().equals(Status.AVAILABLE)) {
        cacheManagerUnderTest.close();
      }
      cacheManagerUnderTest.destroy();
    }
  }

  @Test
  public void testSimplePut() throws Exception {
    final Cache<Long, String> cache = cacheManagerUnderTest.getCache(getCache(0), Long.class, String.class);

    cache.put(1L, "value1");
    assertThat(cache.get(1L), is("value1"));
    assertNull(cache.get(100L));

    cache.put(2L, "value2");
    assertThat(cache.get(1L), is("value1"));
    assertThat(cache.get(2L), is("value2"));
  }

  @Test
  public void testPersistentPut() throws Exception {
    final Cache<Long, String> cache = cacheManagerUnderTest.getCache(getCache(0), Long.class, String.class);

    assertNull(cache.get(1L));
    assertNull(cache.get(2L));

    cache.put(1L, "value1");
    assertThat(cache.get(1L), is("value1"));

    cache.put(2L, "value2");
    assertThat(cache.get(1L), is("value1"));
    assertThat(cache.get(2L), is("value2"));

    cacheManagerUnderTest.removeCache(getCache(0));
    cacheManagerUnderTest.close();

    final PersistentCacheManager cacheManager = buildCacheManager(getTestCaches());
    final Cache<Long, String> cache1 = cacheManager.getCache(getCache(0), Long.class, String.class);

    assertThat(cache1.get(1L), is("value1"));
    assertThat(cache1.get(2L), is("value2"));
    cacheManager.close();
    cacheManager.destroy();
  }

  @Test
  public void testPersistentPutMultiCache() throws Exception {
    final Cache<Long, String> cache1 = cacheManagerUnderTest.getCache(getCache(0), Long.class, String.class);
    final Cache<Long, String> cache2 = cacheManagerUnderTest.getCache(getCache(1), Long.class, String.class);
    assertNull(cache1.get(1L));
    assertNull(cache2.get(2L));

    assertNull(cache2.get(100L));
    assertNull(cache2.get(200L));

    cache1.put(1L, "value1");
    cache1.put(2L, "value2");
    assertThat(cache1.get(1L), is("value1"));
    assertThat(cache1.get(2L), is("value2"));

    cache2.put(1L, "value1_1");
    cache2.put(200L, "value200");
    cache2.put(100L, "value100");
    assertThat(cache2.get(1L), is("value1_1"));
    assertThat(cache1.get(1L), is("value1"));
    assertThat(cache2.get(200L), is("value200"));

    cacheManagerUnderTest.close();

    final PersistentCacheManager cacheManager = buildCacheManager(getTestCaches());
    final Cache<Long, String> cache11 = cacheManager.getCache(getCache(0), Long.class, String.class);
    final Cache<Long, String> cache12 = cacheManager.getCache(getCache(1), Long.class, String.class);

    assertThat(cache11.get(1L), is("value1"));
    assertThat(cache11.get(2L), is("value2"));
    assertThat(cache12.get(1L), is("value1_1"));
    assertThat(cache12.get(200L), is("value200"));

    cache11.put(100L, "value1_100");
    cache12.put(1L, "value1_1_new");

    assertThat(cache11.get(100L), is("value1_100"));
    assertThat(cache11.get(1L), is("value1"));
    assertThat(cache12.get(1L), is("value1_1_new"));
    cacheManager.close();
    cacheManager.destroy();
  }

  abstract PersistentCacheManager buildCacheManager(String... testCaches);
  abstract String[] getTestCaches();
  abstract String getCache(int idx);
}
