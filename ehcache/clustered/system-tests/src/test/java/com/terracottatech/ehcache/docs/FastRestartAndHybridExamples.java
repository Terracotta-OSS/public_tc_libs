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

package com.terracottatech.ehcache.docs;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.testing.rules.EnterpriseCluster;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static org.junit.Assert.fail;

public class FastRestartAndHybridExamples {
  private static final String[] DATA_DIRS = {"data", "dataroot"};
  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
          + "<ohr:resource name=\"secondary-server-resource\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>\n"
          + "</config>\n"
          + "<config>"
          + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
          + "<data:directory name=\"data-root-identifier\">../" + DATA_DIRS[0] + "</data:directory>\n"
          + "<data:directory name=\"dataroot\" use-for-platform=\"true\">../" + DATA_DIRS[1] + "</data:directory>\n"
          + "</data:data-directories>\n"
          + "</config>\n";

  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  private static URI connectionURI;

  @BeforeClass
  public static void setUp() throws Exception {
    connectionURI = CLUSTER.getConnectionURI();
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void testFullCacheManager() throws Exception {
    // This code contains a hard-coded cluster URI to give a neat example of CacheManager API
    // This test is expected to fail if the server isn't started on port 9510. Catch the exception
    // and ignore it.
    try {
      // tag::restartableCacheManager[]
      EnterpriseServerSideConfigurationBuilder serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
          .enterpriseCluster(URI.create("terracotta://localhost:9510/my-application"))   // <1>
          .timeouts(TimeoutsBuilder.timeouts().connection(Duration.of(2, ChronoUnit.SECONDS)))
          .autoCreate()
          .defaultServerResource("primary-server-resource")
          .restartable("data-root-identifier") // <2>
          .withRestartIdentifier("restart-identifier");    // <3>

      PersistentCacheManager cacheManager = CacheManagerBuilder
          .newCacheManagerBuilder()
          .with(serverSideConfigBuilder)  // <4>
          .build(true);

      cacheManager.close();
      cacheManager.destroy(); // <5>
      // end::restartableCacheManager[]
      fail("StateTransitionException expected");
    } catch (StateTransitionException e) {
      // expected
    }
  }

  @Test
  public void testHybridCacheManager() throws Exception {
    // This code contains a hard-coded cluster URI to give a neat example of CacheManager API
    // This test is expected to fail if the server isn't started on port 9510. Catch the exception
    // and ignore it.
    try {
      // tag::hybridCacheManager[]
      EnterpriseServerSideConfigurationBuilder serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
          .enterpriseCluster(URI.create("terracotta://localhost:9510/my-application"))
          .timeouts(TimeoutsBuilder.timeouts().connection(Duration.of(2, ChronoUnit.SECONDS)))
          .autoCreate()
          .defaultServerResource("primary-server-resource")
          .restartable("data-root-identifier")
          .withRestartableOffHeapMode(RestartableOffHeapMode.PARTIAL); // <1>

      PersistentCacheManager cacheManager = CacheManagerBuilder
          .newCacheManagerBuilder()
          .with(serverSideConfigBuilder)
          .build(true);

      cacheManager.close();
      cacheManager.destroy();
      // end::hybridCacheManager[]
      fail("StateTransitionException expected");
    } catch (StateTransitionException e) {
      // expected
    }
  }

  @Test
  public void testDedicatedAndSharedPools() throws Exception {
    // tag::dedicatedAndSharedPools[]
    ClusteredResourcePool restartableDedicatedPool = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableDedicated("primary-server-resource", 4, MemoryUnit.MB); // <1>

    ClusteredResourcePool restartableSharedPool = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableShared("shared-pool"); // <2>
    // end::dedicatedAndSharedPools[]
  }

  @Test
  public void testMixedCacheManager() throws Exception {
    // tag::completeExample[]
    EnterpriseServerSideConfigurationBuilder serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(connectionURI)
        .autoCreate()
        .defaultServerResource("primary-server-resource")
        .resourcePool("shared-pool", 20, MemoryUnit.MB, "secondary-server-resource") // <1>
        .restartable("data-root-identifier");

    PersistentCacheManager cacheManager = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(serverSideConfigBuilder)
        .build(true);

    ClusteredResourcePool restartableDedicatedPool = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableDedicated("primary-server-resource", 4, MemoryUnit.MB);

    ClusteredResourcePool restartableSharedPool = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableShared("shared-pool");

    ClusteredResourcePool nonRestartableDedicatedPool = ClusteredResourcePoolBuilder
        .clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB); // <2>

    ClusteredResourcePool nonRestartableSharedPool = ClusteredResourcePoolBuilder
        .clusteredShared("shared-pool"); // <3>

    Cache<Long, String> restartableDedicatedPoolCache = cacheManager
        .createCache("restartableDedicatedPoolCache", CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartableDedicatedPool)));   // <4>

    Cache<Long, String> restartableSharedPoolCache = cacheManager
        .createCache("restartableSharedPoolCache", CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartableSharedPool)));   // <5>

    Cache<String, Boolean> nonRestartableDedicatedPoolCache = cacheManager
        .createCache("nonRestartableDedicatedPoolCache", CacheConfigurationBuilder
            .newCacheConfigurationBuilder(String.class, Boolean.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(nonRestartableDedicatedPool))); // <6>

    Cache<String, Boolean> nonRestartableSharedPoolCache = cacheManager
        .createCache("nonRestartableSharedPoolCache", CacheConfigurationBuilder
            .newCacheConfigurationBuilder(String.class, Boolean.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(nonRestartableSharedPool))); // <7>

    cacheManager.close();
    cacheManager.destroy();
    // end::completeExample[]
  }

  @Test
  public void testHybridCacheManagerWithCaches() throws Exception {
    // tag::completeHybridExample[]
    EnterpriseServerSideConfigurationBuilder serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(connectionURI.resolve("/cacheManager"))
        .autoCreate()
        .defaultServerResource("primary-server-resource")
        .restartable("data-root-identifier")
        .withRestartableOffHeapMode(RestartableOffHeapMode.PARTIAL); // <1>

    PersistentCacheManager cacheManager = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(serverSideConfigBuilder)
        .build(true);

    ClusteredResourcePool pool1 = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableDedicated("primary-server-resource", 4, MemoryUnit.MB, 25); // <2>

    ClusteredResourcePool pool2 = ClusteredRestartableResourcePoolBuilder
        .clusteredRestartableDedicated("primary-server-resource", 12, MemoryUnit.MB, 50); // <3>

    Cache<Long, String> restartableDedicatedPoolCache = cacheManager
        .createCache("restartableDedicatedPoolCache", CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool1)));   // <4>

    Cache<Long, String> restartableSharedPoolCache = cacheManager
        .createCache("restartableSharedPoolCache", CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool2)));   // <5>

    cacheManager.close();
    cacheManager.destroy();
    // end::completeHybridExample[]
  }
}
