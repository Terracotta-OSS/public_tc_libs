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

import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.Test;

import java.net.URI;

import static com.terracottatech.ehcache.clustered.frs.CacheManagerType.FULL;
import static org.junit.Assert.fail;

/**
 * Bad Cache manager config tests
 */
public class RestartableTierManagerConfigErrorIT extends BaseActiveClusteredTest {

  @Test
  public void testNonExistingRoot() throws Exception {
    try {
      createRestartableCacheManager("restartable-cm", FULL, "bad-root");
      fail("Unexpected success with a bad root");
    } catch (StateTransitionException ignored) {
    }
  }

  @Test
  public void testNonExistingResource() throws Exception {
    try {
      createNonRestartableCacheManagerWithBadResource("test-cm");
      fail("Unexpected success with a bad resource");
    } catch (StateTransitionException ignored) {
    }
  }

  @Test
  public void testOverSizedPool() throws Exception {
    try {
      createRestartableCacheManagerWithRestartableCache("test-cm", Long.class, String.class, 512);
      fail("Unexpected success with a over sized resource pool");
    } catch (StateTransitionException ignored) {
    }
    // Shutdown and restart to make sure it will restart clean
    CLUSTER.getClusterControl().terminateAllServers();
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
  }

  @Override
  protected URI getURI() {
    return CLUSTER.getConnectionURI();
  }

  PersistentCacheManager createNonRestartableCacheManagerWithBadResource(String cacheManagerName) {
    ServerSideConfigurationBuilder ossBuilder = ClusteringServiceConfigurationBuilder.
        cluster(this.getURI().resolve("/" + cacheManagerName))
        .autoCreate().defaultServerResource("junkResource");

    return CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(ossBuilder)
        .build(true);
  }
}
