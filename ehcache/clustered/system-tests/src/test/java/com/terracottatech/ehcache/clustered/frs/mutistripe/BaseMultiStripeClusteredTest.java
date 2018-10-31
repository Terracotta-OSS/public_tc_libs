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
package com.terracottatech.ehcache.clustered.frs.mutistripe;

import com.terracottatech.ehcache.clustered.frs.AbstractClusteredTest;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.net.URI;
import java.time.Duration;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;

public abstract class BaseMultiStripeClusteredTest extends AbstractClusteredTest {
  static final int NUM_STRIPES = 2;
  static final long DEFAULT_POOL_SIZE = 4;

  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(2, 2).withPlugins(RESOURCE_CONFIG).build();

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
  }

  void shutdownActivesAndWaitForPassivesToBecomeActives() throws Exception {
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
  }

  @Override
  protected URI getURI() {
    return CLUSTER.getConnectionURI();
  }

  URI getStripeURI(int index) {
    return CLUSTER.getStripeConnectionURI(index);
  }

  PersistentCacheManager createNonRestartableCacheManagerWithCache(String cacheManagerName, String cacheName) {
    return createNonRestartableCacheManagerWithCache(cacheManagerName, cacheName, DEFAULT_POOL_SIZE);
  }

  PersistentCacheManager createNonRestartableCacheManagerWithCache(String cacheManagerName, String cacheName, long size) {
    return createNonRestartableCacheManagerWithCache(cacheManagerName, getURI(), cacheName, size);
  }

  PersistentCacheManager createNonRestartableCacheManagerWithCache(String cacheManagerName, URI uri, String cacheName, long size) {
    ServerSideConfigurationBuilder ossBuilder = ClusteringServiceConfigurationBuilder
        .cluster(uri.resolve("/" + cacheManagerName))
        .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
        .autoCreate()
        .defaultServerResource(DEFAULT_PRIMARY_RESOURCE);

    CacheConfigurationBuilder<Long, String> cacheConfig = CacheConfigurationBuilder
        .newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
            .with(ClusteredResourcePoolBuilder.clusteredDedicated(size, MemoryUnit.MB)));

    return CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(ossBuilder)
        .withCache(cacheName, cacheConfig)
        .build(true);
  }
}
