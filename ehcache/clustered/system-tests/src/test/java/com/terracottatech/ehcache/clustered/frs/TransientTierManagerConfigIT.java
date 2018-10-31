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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.terracottatech.testing.rules.EnterpriseCluster;

import java.net.URI;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TransientTierManagerConfigIT extends AbstractClusteredTest {
  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>\n"
      + "</config>\n"
      + "<config>"
      + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
      + "<data:directory name=\"root\">../" + DATA_DIRS[0] + "</data:directory>\n"
      + "</data:data-directories>\n"
      + "</config>\n";

  @Rule
  public EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
  }

  @Test
  public void testTransientTierManagerConfig() throws Exception {
    final PersistentCacheManager cacheManager = createNonRestartableCacheManager("non-restartable-cm");
    Cache<Long, String> testCache = cacheManager.createCache("testCache",
        newCacheConfigurationBuilder(Long.class, String.class, heap(10)));
    testCache.put(1L, "one");
    assertThat(testCache.get(1L), is("one"));
    cacheManager.close();
  }

  @Override
  protected URI getURI() {
    return CLUSTER.getConnectionURI();
  }
}
