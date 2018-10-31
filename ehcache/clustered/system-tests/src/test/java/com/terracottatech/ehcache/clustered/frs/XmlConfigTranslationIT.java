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
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * XmlConfigTranslationIT
 */
public class XmlConfigTranslationIT extends BaseActiveClusteredTest {
  @Test
  public void unparseCacheManagerWithoutResource() throws CachePersistenceException {
    PersistentCacheManager cacheManager = createRestartableCacheManagerWithRestartableCacheWithoutResource("MyCacheManager",
        Long.class, String.class, CacheManagerType.FULL);
    XmlConfiguration xmlConfig = new XmlConfiguration(cacheManager.getRuntimeConfiguration());
    CacheManager myCacheManager = CacheManagerBuilder.newCacheManager(xmlConfig);
    myCacheManager.init();
    Cache<Long, String> myCache = myCacheManager.getCache("clustered-cache", Long.class, String.class);
    assertThat(myCache, is(notNullValue()));
    myCacheManager.close();
    cacheManager.destroyCache("clustered-cache");
    cacheManager.close();
  }

}
