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

import com.terracottatech.config.data_roots.DataDirectoriesConfig;
import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.internal.EnterpriseClusterTierClientEntityService;
import com.terracottatech.ehcache.clustered.client.internal.EnterpriseEhcacheClientEntityService;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateServiceProvider;
import com.terracottatech.persistence.frs.FRSPersistenceServiceProvider;
import com.terracottatech.persistence.frs.config.FRSPersistenceServiceProviderConfiguration;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;

public abstract class BaseEnterpriseClusteredTest extends BasicAbstractEnterprisePassthroughTest {
  private EnterpriseServerSideConfigurationBuilder serverSideConfigBuilder;
  private PersistentCacheManager cacheManager;

  private static final String BASE_ROOT_NAME = "root";
  private static final String STRIPENAME = "stripe";

  static final String CACHE = "clustered-cache";
  static final String DATA_ROOT_ID = "root1";
  static final String SHARED_POOL_A = "resource-pool-a";
  static final String SHARED_POOL_B = "resource-pool-b";
  static final String DEFAULT_PRIMARY_RESOURCE = "primary-server-resource";
  static final String SECONDARY_RESOURCE = "secondary-server-resource";
  static final String STRIPE_URI = "passthrough://" + STRIPENAME;
  static final String EHCACHE_STORAGE_SPACE = "ehcache" + File.separator + "frs";
  static final String FRS_CONTAINER = "default-frs-container";
  static final String METADATA_DIR_NAME = "metadata";

  //Server name is created by appending the index to stripe name in createMultiServerStripe() of PassthroughTestHelpers
  static final String STRIPE0 = STRIPENAME + "_0";

  static final Set<URI> clusterURIs = new HashSet<>();

  @After
  public void cleanup() throws Exception {
    for (URI uri : clusterURIs) {
      try {
        newCacheManagerBuilder()
            .with(EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(uri).expecting())
            .build().destroy();
      } catch (Exception e) {
        // do nothing
      }
    }
    clusterURIs.clear();
  }

  private URI clusterURI(String cacheManagerName) {
    URI uri = URI.create(buildClusterUri().toString() + "/" + cacheManagerName);
    clusterURIs.add(uri);
    return uri;
  }

  public DataDirectoriesConfig dataRootConfig() {
    try {
      return PassthroughTestHelper.addDataRoots(BASE_ROOT_NAME, folder, 3);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    OffheapResourcesType offHeapResources = PassthroughTestHelper
        .getOffheapResources(64, org.terracotta.offheapresource.config.MemoryUnit.MB, DEFAULT_PRIMARY_RESOURCE, SECONDARY_RESOURCE);

    return server -> {
      server.registerServerEntityService(new EnterpriseEhcacheServerEntityService());
      server.registerClientEntityService(new EnterpriseEhcacheClientEntityService());
      server.registerServerEntityService(new EnterpriseClusterTierServerEntityService());
      server.registerClientEntityService(new EnterpriseClusterTierClientEntityService());
      server.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
      server.registerClientEntityService(new VoltronReadWriteLockEntityClientService());
      server.registerExtendedConfiguration(new OffHeapResourcesProvider(offHeapResources));
      server.registerExtendedConfiguration(dataRootConfig());
      server.registerServiceProvider(new FRSPersistenceServiceProvider(), new FRSPersistenceServiceProviderConfiguration(BASE_ROOT_NAME + 1));
      server.registerOverrideServiceProvider(new EnterpriseEhcacheStateServiceProvider(), () -> EnterpriseEhcacheStateServiceProvider.class);
    };
  }

  @After
  public void tearDown() {
    PassthroughTestHelper.clearDataDirectories();
  }

  void restartActive() throws Exception {
    restartActives();
  }

  PersistentCacheManager createRestartableCacheManager() {
    return createRestartableCacheManager("restartableCacheManager", false);
  }

  PersistentCacheManager createRestartableCacheManager(String cacheManagerName, boolean isHybrid) {
    RestartableOffHeapMode restartableOffHeapMode;
    if (!isHybrid) {
      restartableOffHeapMode = RestartableOffHeapMode.FULL;
    } else {
      restartableOffHeapMode = RestartableOffHeapMode.PARTIAL;
    }

    serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterURI(cacheManagerName))
        .autoCreate()
        .restartable(DATA_ROOT_ID)
        .withRestartableOffHeapMode(restartableOffHeapMode);

    cacheManager = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(serverSideConfigBuilder)
        .build(true);

    return cacheManager;
  }

  PersistentCacheManager createNonRestartableCacheManager() {
    return createNonRestartableCacheManager("nonRestartableCacheManager");
  }

  PersistentCacheManager createNonRestartableCacheManager(String cacheManagerName) {
    ServerSideConfigurationBuilder ossBuilder = ClusteringServiceConfigurationBuilder.
        cluster(clusterURI(cacheManagerName))
        .autoCreate();

    cacheManager = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(ossBuilder)
        .build(true);

    return cacheManager;
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCache(Class<K> keyClass,
                                                                                  Class<V> valueClass,
                                                                                  boolean isHybrid) {
    return createRestartableCacheManagerWithRestartableCache("cacheManagerWithCache", keyClass, valueClass, isHybrid, 1);
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCache(String cacheManagerName,
                                                                                  Class<K> keyClass,
                                                                                  Class<V> valueClass,
                                                                                  boolean isHybrid) {
    return createRestartableCacheManagerWithRestartableCache(cacheManagerName, keyClass, valueClass, isHybrid, 1);
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCache(String cacheManagerName,
                                                                                  Class<K> keyClass,
                                                                                  Class<V> valueClass,
                                                                                  boolean isHybrid,
                                                                                  int rootNo) {
    return createRestartableCacheManagerWithRestartableCache(cacheManagerName, keyClass, valueClass, isHybrid, rootNo, null);
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCache(String cacheManagerName,
                                                                                  Class<K> keyClass,
                                                                                  Class<V> valueClass,
                                                                                  boolean isHybrid,
                                                                                  int rootNo,
                                                                                  String frsIdentifier) {
    RestartableOffHeapMode restartableOffHeapMode;
    ClusteredResourcePool restartablePool;
    if (!isHybrid) {
      restartableOffHeapMode = RestartableOffHeapMode.FULL;
      restartablePool = ClusteredRestartableResourcePoolBuilder
          .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB);
    } else {
      restartableOffHeapMode = RestartableOffHeapMode.PARTIAL;
      restartablePool = ClusteredRestartableResourcePoolBuilder
          .clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, 4, MemoryUnit.MB, 50);
    }

    serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterURI(cacheManagerName))
        .autoCreate()
        .restartable(BASE_ROOT_NAME + rootNo)
        .withRestartableOffHeapMode(restartableOffHeapMode);

    if (frsIdentifier != null) {
      serverSideConfigBuilder = ((EnterpriseServerSideConfigurationBuilder.RestartableServerSideConfigurationBuilder) serverSideConfigBuilder)
          .withRestartIdentifier(frsIdentifier);
    }

    cacheManager = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(serverSideConfigBuilder)
        .withCache(CACHE, CacheConfigurationBuilder
            .newCacheConfigurationBuilder(keyClass, valueClass,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool)))
        .build(true);

    return cacheManager;
  }
}
