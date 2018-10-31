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

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class OversizedCacheOpsRestartIT extends BaseActiveClusteredTest {
  @Test
  public void overSizedCacheOps() throws Exception {
    try (PersistentCacheManager cacheManager =
             createRestartableCacheManagerWithRestartableCache("cm-restartable", Long.class, String.class, 1)) {
      Cache<Long, String> cache = cacheManager.getCache(CACHE, Long.class, String.class);

      cache.put(1L, "The one");
      cache.put(2L, "The two");
      cache.put(1L, "Another one");
      cache.put(3L, "The three");
      assertThat(cache.get(1L), equalTo("Another one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
      cache.put(1L, buildLargeString(2));
      assertThat(cache.get(1L), is(nullValue()));
      // ensure others are not evicted
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
    }

    restartActive();

    try (PersistentCacheManager cacheManager =
             createRestartableCacheManagerWithRestartableCache("cm-restartable", Long.class, String.class, 1)) {
      Cache<Long, String> cache = cacheManager.getCache(CACHE, Long.class, String.class);

      assertThat(cache.get(1L), is(nullValue()));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
    }
  }

  private String buildLargeString(int sizeInMB) {
    char[] filler = new char[sizeInMB * 1024 * 1024];
    Arrays.fill(filler, '0');
    return new String(filler);
  }
}
