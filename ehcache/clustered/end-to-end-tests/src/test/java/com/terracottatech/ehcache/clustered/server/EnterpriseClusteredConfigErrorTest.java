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
package com.terracottatech.ehcache.clustered.server;

import com.terracottatech.persistence.frs.FRSPersistenceServiceProvider;
import com.terracottatech.persistence.frs.config.FRSPersistenceServiceProviderConfiguration;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateServiceProvider;
import org.terracotta.exception.ServerSideExceptionWrapper;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.net.URI;

import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.FULL;
import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.PARTIAL;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EnterpriseClusteredConfigErrorTest extends BasicAbstractEnterprisePassthroughTest {
  private static final String DEFAULT_PRIMARY_RESOURCE = "primary-server-resource";
  private static final String DEFAULT_SECONDARY_RESOURCE = "secondary-server-resource";
  private static final String PRIMARY_POOL = "primary-pool";
  private static final String SECONDARY_POOL = "secondary-pool";
  private static final String DATA_ROOT_ID = "root1";

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    return  new ServerInitializerBuilder()
        .resource(DEFAULT_PRIMARY_RESOURCE, 128, MemoryUnit.MB)
        .resource(DEFAULT_SECONDARY_RESOURCE, 128, MemoryUnit.MB)
        .dataRoot(DATA_ROOT_ID, createDataRootDir())
        .serviceProvider(new FRSPersistenceServiceProvider(), new FRSPersistenceServiceProviderConfiguration(DATA_ROOT_ID))
        .overrideServiceProvider(new EnterpriseEhcacheStateServiceProvider(), () -> EnterpriseEhcacheStateServiceProvider.class)
        .build();
  }

  @After
  public void cleanup() throws Exception {
    newCacheManagerBuilder()
        .with(EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(clusterURI()).expecting())
        .build().destroy();
  }

  private URI clusterURI() {
    return URI.create(buildClusterUri().toString() + "/my-application");
  }

  @Test
  public void testClusteredRestartableTierManagerCreationAfterNonRestartable() {
    PersistentCacheManager cm = createNormalCacheManager(clusterURI());
    try {
      // this should fail
      createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
      fail("Mismatched restart configuration for cache manager. Expected to fail");
    } catch (StateTransitionException e) {
      assertThat(e.getMessage(), containsString("Unable to validate"));
    }
    cm.close();
  }

  @Test
  public void testClusteredNonRestartableTierManagerCreationAfterRestartable() {
    PersistentCacheManager cm = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
    try {
      // this should fail
      createNormalCacheManager(clusterURI());
      fail("Mismatched restart configuration for cache manager. Expected to fail");
    } catch (StateTransitionException e) {
      assertThat(e.getMessage(), containsString("Unable to validate"));
    }
    cm.close();
  }

  @Test
  public void testMismatchedDataRootForRestartableTierManagerCreation() {
    PersistentCacheManager cm = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
    try {
      // this should fail
      createRestartableCacheManager(clusterURI(), "new-root", FULL, null);
      fail("Mismatched restart configuration for cache manager. Expected to fail");
    } catch (StateTransitionException e) {
      assertThat(e.getMessage(), containsString("Unable to validate"));
    }
    cm.close();
  }

  @Test
  public void testMismatchedOffHeapModeForRestartableTierManagerCreation() {
    PersistentCacheManager cm = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
    try {
      // this should fail
      createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, PARTIAL, null);
      fail("Mismatched restart configuration for cache manager. Expected to fail");
    } catch (StateTransitionException e) {
      assertThat(e.getMessage(), containsString("Unable to validate"));
    }
    cm.close();
  }

  @Test
  public void testMismatchedFrsIdentifierForRestartableTierManagerCreation() {
    PersistentCacheManager cm = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
    try {
      // this should fail
      createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, "tierMgr");
      fail("Mismatched restart configuration for cache manager. Expected to fail");
    } catch (StateTransitionException e) {
      assertThat(e.getMessage(), containsString("Unable to validate"));
    }
    cm.close();
  }

  @Test
  public void testDestroyWithRestartableAndNonRestartableCacheManagersWithSameIdentifier() throws Exception {
    PersistentCacheManager nonRestartableCacheManager = createNormalCacheManager(clusterURI());
    nonRestartableCacheManager.close();
    nonRestartableCacheManager.destroy();
    PersistentCacheManager restartableCacheManager = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
    restartableCacheManager.close();
    restartableCacheManager.destroy();
  }

  @Test
  public void testDestroyWithRestartableCacheManagersWithSameIdentifier() throws Exception {
    PersistentCacheManager restartableCacheManager1 = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
    Cache<Long, String> cache1 = restartableCacheManager1.getCache("some-cache", Long.class, String.class);
    cache1.put(1L, "testValue");
    assertThat(cache1.get(1L), is("testValue"));
    restartableCacheManager1.close();
    restartableCacheManager1.destroy();
    PersistentCacheManager restartableCacheManager2 = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, FULL, null);
    Cache<Long, String> cache2 = restartableCacheManager2.getCache("some-cache", Long.class, String.class);
    assertThat(cache2.get(1L), nullValue());
    restartableCacheManager2.close();
    restartableCacheManager2.destroy();
  }

  @Test
  public void testNonExistingRoot() throws Exception {
    try {
      createRestartableCacheManager(clusterURI(), "bad-root", FULL, null);
      fail("Unexpected succeeded to create cache manager with a bad root");
    } catch (StateTransitionException e) {
      Throwable rootCause = getRootCause(e);
      assertThat(rootCause, instanceOf(ServerSideExceptionWrapper.class));
      assertThat(rootCause.getMessage(), containsString("bad-root"));
    }
    // now create a normal cache manager and it should still work..verifies pass thru
    // is still stable
    PersistentCacheManager cacheManager = createRestartableCacheManager(clusterURI(), DATA_ROOT_ID,
        FULL, null);
    Cache<Long, String> cache1 = cacheManager.getCache("some-cache", Long.class, String.class);
    cache1.put(1L, "testValue");
    assertThat(cache1.get(1L), is("testValue"));
    cacheManager.close();
  }

  @Test
  public void testWrongDataPercentForHybrid() throws Exception {
    try {
      createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, 100, false, PARTIAL);
      fail("Must not succeed with invalid data percent");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("0 and 99"));
    }
  }

  @Test
  public void testWrongDataPercentForFull() throws Exception {
    try {
      createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, 0, false, FULL);
      fail("Must not succeed with invalid data percent. Data Percent is not allowed for full");
    } catch (StateTransitionException ignored) {
    }
  }

  @Test
  public void testMismatchedPoolForHybrid() throws Exception {
    try {
      createRestartableCacheManager(clusterURI(), DATA_ROOT_ID, 90, true, PARTIAL);
      fail("Must not succeed with a restartable shared pool");
    } catch (StateTransitionException ignored) {
    }
  }

  @Test
  public void testPoolAllocationCombinations() throws Exception {

    ClusteredResourcePool[] resourcePools = new ClusteredResourcePool[12];
    resourcePools[0] =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);
    resourcePools[1] =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_SECONDARY_RESOURCE, 4, MemoryUnit.MB);
    resourcePools[2] =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 2, MemoryUnit.MB);
    resourcePools[3] =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(PRIMARY_POOL);
    resourcePools[4] =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SECONDARY_POOL);
    resourcePools[5] =
        ClusteredResourcePoolBuilder.clusteredDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);
    resourcePools[6] =
        ClusteredResourcePoolBuilder.clusteredDedicated(DEFAULT_SECONDARY_RESOURCE, 4, MemoryUnit.MB);
    resourcePools[7] =
        ClusteredResourcePoolBuilder.clusteredDedicated(DEFAULT_PRIMARY_RESOURCE, 2, MemoryUnit.MB);
    resourcePools[8] =
        ClusteredResourcePoolBuilder.clusteredShared(PRIMARY_POOL);
    resourcePools[9] =
        ClusteredResourcePoolBuilder.clusteredShared(SECONDARY_POOL);

    EnterpriseServerSideConfigurationBuilder ssBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterURI()).autoCreate()
        .restartable(DATA_ROOT_ID).withRestartableOffHeapMode(FULL)
        .resourcePool(PRIMARY_POOL, 4, MemoryUnit.MB, DEFAULT_PRIMARY_RESOURCE)
        .resourcePool(SECONDARY_POOL, 2, MemoryUnit.MB, DEFAULT_SECONDARY_RESOURCE);
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ssBuilder);

    PersistentCacheManager cacheManager1 = clusteredCacheManagerBuilder.build(true);
    PersistentCacheManager cacheManager2 = clusteredCacheManagerBuilder.build(true);

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        IllegalStateException exception;
        if (i == j) {
          testPoolAllocationCombinations(i + ":" + j, resourcePools[i], resourcePools[j], cacheManager1, cacheManager2);  // should not fail
        } else {
          try {
            testPoolAllocationCombinations(i + ":" + j, resourcePools[i], resourcePools[j], cacheManager1, cacheManager2);  // should fail
            fail("Creating the cache with " + resourcePools[i] + " and validating it with " + resourcePools[j] + " should have failed");
          } catch (IllegalStateException ise) {
            exception = ise;
            assertThat(exception.getCause().getCause().getCause().getCause().getCause().getMessage(),
                containsString("Existing ServerStore configuration is not compatible with the desired configuration"));
          }
        }
      }
    }

    cacheManager1.close();
    cacheManager2.close();
  }

  private void testPoolAllocationCombinations(String cacheAliasSuffix, ClusteredResourcePool pool1, ClusteredResourcePool pool2, CacheManager cacheManager1, CacheManager cacheManager2) throws Exception {
    CacheConfigurationBuilder<Long, String> cacheConfiguration1 =
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool1));

    CacheConfigurationBuilder<Long, String> cacheConfiguration2 =
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool2));


    String alias = "cache-" + cacheAliasSuffix;
    cacheManager1.createCache(alias, cacheConfiguration1);
    cacheManager2.createCache(alias, cacheConfiguration2);
  }

  private PersistentCacheManager createNormalCacheManager(URI clusterUri) {
    ServerSideConfigurationBuilder ossBuilder = ClusteringServiceConfigurationBuilder.
        cluster(clusterUri).autoCreate();

    ClusteredResourcePool normalPool =
        ClusteredResourcePoolBuilder.clusteredDedicated(DEFAULT_PRIMARY_RESOURCE,
            2, MemoryUnit.MB);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ossBuilder)
            .withCache("some-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(normalPool)));
    return clusteredCacheManagerBuilder.build(true);
  }

  private PersistentCacheManager createRestartableCacheManager(URI clusterUri, String rootId,
                                                               RestartableOffHeapMode mode, String frsId) {
    final ClusteredResourcePool restartablePool =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE,
            2, MemoryUnit.MB);
    EnterpriseServerSideConfigurationBuilder.RestartableServerSideConfigurationBuilder ssBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterUri).autoCreate().restartable(rootId).withRestartableOffHeapMode(mode);
    if (frsId != null) {
      ssBuilder = ssBuilder.withRestartIdentifier(frsId);
    }

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ssBuilder)
            .withCache("some-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool)));

    return clusteredCacheManagerBuilder.build(true);
  }

  private PersistentCacheManager createRestartableCacheManager(URI clusterUri, String rootId, int dataPercent,
                                                               boolean sharedPool,
                                                               RestartableOffHeapMode mode) {
    final ClusteredResourcePool restartablePool = (sharedPool) ?
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared("pool-a") : (dataPercent < 0) ?
            ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE,
                2, MemoryUnit.MB) :
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE,
            2, MemoryUnit.MB, dataPercent);
    EnterpriseServerSideConfigurationBuilder ssBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterUri).autoCreate().restartable(rootId)
        .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
        .resourcePool("pool-a", 28, MemoryUnit.MB, DEFAULT_PRIMARY_RESOURCE)
        .restartable(DATA_ROOT_ID)
        .withRestartableOffHeapMode(mode);

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ssBuilder)
            .withCache("some-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool)));

    return clusteredCacheManagerBuilder.build(true);
  }

  private static Throwable getRootCause(Throwable e) {
    Throwable current = e;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }
}
