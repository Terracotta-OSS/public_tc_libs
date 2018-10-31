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
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.FULL;
import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.PARTIAL;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;

public abstract class AbstractClusteredTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClusteredTest.class);

  private EnterpriseServerSideConfigurationBuilder serverSideConfigBuilder;

  @Rule
  public TestName testName = new TestName();

  static final String SECONDARY_RESOURCE = "secondary-server-resource";
  static final String DATA_ROOT_ID = "root";
  static final String path = "build" + File.separator + "galvan";
  static final String SHARED_POOL_A = "resource-pool-a";
  static final String SHARED_POOL_B = "resource-pool-b";
  static final String[] DATA_DIRS = {"data", "dataroot"};

  protected static final String CACHE = "clustered-cache";
  protected static final String CACHE_MANAGER = "clustered-cm";
  protected static final String DEFAULT_PRIMARY_RESOURCE = "primary-server-resource";
  protected static final String RESOURCE_CONFIG =
          "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
                  + "<ohr:offheap-resources>"
                  + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "<ohr:resource name=\"secondary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "</ohr:offheap-resources>\n"
                  + "</config>\n"
                  + "<config>"
                  + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
                  + "<data:directory name=\"root\" use-for-platform=\"true\">../" + DATA_DIRS[0] + "</data:directory>\n"
                  + "<data:directory name=\"dataroot\">../" + DATA_DIRS[1] + "</data:directory>\n"
                  + "</data:data-directories>\n"
                  + "</config>\n";

  @Rule
  public CacheManagerTestWatcher watcher = new CacheManagerTestWatcher();

  protected static class CacheManagerTestWatcher extends TestWatcher {
    private final List<PersistentCacheManager> cacheManagersUnderTest = new ArrayList<>();

    @Override
    protected void succeeded(Description description) {
      cacheManagersUnderTest.clear();
    }

    @Override
    protected void failed(Throwable e, Description description) {
      cacheManagersUnderTest.forEach((p) -> {
        try {
          p.close();
          p.destroy();
        } catch (Exception ex) {
          LOGGER.info("Unable to close and destroy CM", ex);
        }
      });
      cacheManagersUnderTest.clear();
      try {
        FileUtils.moveDataDirs(new File(path), DATA_DIRS);
      } catch (IOException ignored) {
      }
    }

    public void addCacheManager(PersistentCacheManager cacheManager) {
      cacheManagersUnderTest.add(cacheManager);
    }
  }

  PersistentCacheManager createRestartableCacheManager(CacheManagerType cacheManagerType) {
    return createRestartableCacheManagerLocal("restartableCacheManager" + cleanedTestName(),
        cacheManagerType, DATA_ROOT_ID);
  }

  PersistentCacheManager createRestartableCacheManager(String cacheManagerName,
                                                       CacheManagerType cacheManagerType) {
    return createRestartableCacheManagerLocal(cacheManagerName, cacheManagerType, DATA_ROOT_ID);
  }

  PersistentCacheManager createRestartableCacheManager(String cacheManagerName,
                                                       CacheManagerType cacheManagerType,
                                                       String dataRootId) {
    return createRestartableCacheManagerLocal(cacheManagerName, cacheManagerType, dataRootId);
  }

  PersistentCacheManager createNonRestartableCacheManager(String cacheManagerName) {
    ServerSideConfigurationBuilder ossBuilder = ClusteringServiceConfigurationBuilder.
        cluster(this.getURI().resolve("/" + cacheManagerName))
        .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
        .autoCreate().defaultServerResource(DEFAULT_PRIMARY_RESOURCE);

    PersistentCacheManager cacheManager = CacheManagerBuilder
            .newCacheManagerBuilder()
            .with(ossBuilder)
            .build(true);
    watcher.addCacheManager(cacheManager);
    return cacheManager;
  }

  <K, V> PersistentCacheManager createNonRestartableCacheManagerWithCache(String cacheManagerName, int sizeInMB,
                                                                          Class<K> keyClass,
                                                                          Class<V> valueClass) {
    PersistentCacheManager cm = createNonRestartableCacheManager(cacheManagerName);
    ResourcePool pool = ClusteredResourcePoolBuilder.clusteredDedicated(DEFAULT_PRIMARY_RESOURCE, sizeInMB, MemoryUnit.MB);
    CacheConfigurationBuilder<K, V> cacheBuilder = CacheConfigurationBuilder
        .newCacheConfigurationBuilder(keyClass, valueClass,
            ResourcePoolsBuilder.newResourcePoolsBuilder().with(pool));
    cm.createCache(CACHE, cacheBuilder);
    return cm;
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCache(Class<K> keyClass,
                                                                                  Class<V> valueClass,
                                                                                  CacheManagerType cacheManagerType) {
    return createRestartableCacheManagerWithRestartableCacheLocal("restartableCacheManagerWithCache" + cleanedTestName(), keyClass,
        valueClass, cacheManagerType, DATA_ROOT_ID, 4);
  }

  protected void silentDestroyRestartableCacheManager() {
    try {
      destroyRestartableCacheManager();
    } catch (Exception e) {
      LOGGER.info("Cannot destroy Restartable CM ", e);
    }
  }

  protected void silentDestroyRestartableCacheManagerWithCache() {
    try {
      destroyRestartableCacheManagerWithCache();
    } catch (Exception e) {
      LOGGER.info("Cannot destroy Restartable CM With Cache ", e);
    }
  }

  protected void destroyRestartableCacheManager() throws Exception {
    destroyCacheManager("restartableCacheManager" + cleanedTestName());
  }

  protected void destroyRestartableCacheManagerWithCache() throws Exception {
    destroyCacheManager("restartableCacheManagerWithCache" + cleanedTestName());
  }

  protected void destroyCacheManager(String cacheManagerName) throws Exception {
    PersistentCacheManager cacheManager = newCacheManagerBuilder().with(
        EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(getURI().resolve("/" + cacheManagerName))
            .expecting()).build(false);
    cacheManager.destroy();
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCache(String cacheManagerName,
                                                                                  Class<K> keyClass,
                                                                                  Class<V> valueClass,
                                                                                  CacheManagerType cacheManagerType) {
    return createRestartableCacheManagerWithRestartableCacheLocal(cacheManagerName, keyClass, valueClass,
        cacheManagerType, DATA_ROOT_ID, 4);
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCacheWithoutResource(String cacheManagerName,
                                                                                                 Class<K> keyClass,
                                                                                                 Class<V> valueClass,
                                                                                                 CacheManagerType cacheManagerType) {
    return createRestartableCacheManagerWithRestartableCacheLocalWithoutResource(cacheManagerName, keyClass, valueClass,
        cacheManagerType, DATA_ROOT_ID, 4);
  }

  <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCache(String cacheManagerName,
                                                                                  Class<K> keyClass,
                                                                                  Class<V> valueClass,
                                                                                  int sizeInMB) {
    return createRestartableCacheManagerWithRestartableCacheLocal(cacheManagerName, keyClass, valueClass,
        CacheManagerType.FULL, DATA_ROOT_ID, sizeInMB);
  }

  protected abstract URI getURI();

  protected String cleanedTestName() {
    return  testName.getMethodName().split("\\[")[0];
  }

  private <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCacheLocal(
      String cacheManagerName, Class<K> keyClass, Class<V> valueClass, CacheManagerType cacheManagerType,
      String dataRootId, int sizeInMB) {
    RestartableOffHeapMode restartableOffHeapMode;
    ClusteredResourcePool restartablePool;

    if (cacheManagerType == CacheManagerType.FULL) {
      restartableOffHeapMode = FULL;
      restartablePool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, sizeInMB, MemoryUnit.MB);
    } else {
      restartableOffHeapMode = PARTIAL;
      restartablePool = cacheManagerType.equals(CacheManagerType.HYBRID) ?
          ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, sizeInMB, MemoryUnit.MB, 50) :
          ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE, sizeInMB, MemoryUnit.MB, 0);
    }

    serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
            .enterpriseCluster(this.getURI().resolve("/" + cacheManagerName))
            .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
            .autoCreate()
            .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
            .restartable(dataRootId)
            .withRestartableOffHeapMode(restartableOffHeapMode);

    PersistentCacheManager cacheManager = CacheManagerBuilder
            .newCacheManagerBuilder()
            .with(serverSideConfigBuilder)
            .withCache(CACHE, CacheConfigurationBuilder
                    .newCacheConfigurationBuilder(keyClass, valueClass,
                            ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool)))
            .build(true);
    watcher.addCacheManager(cacheManager);
    return cacheManager;
  }

  private <K, V> PersistentCacheManager createRestartableCacheManagerWithRestartableCacheLocalWithoutResource(
      String cacheManagerName, Class<K> keyClass, Class<V> valueClass, CacheManagerType cacheManagerType,
      String dataRootId, int sizeInMB) {
    RestartableOffHeapMode restartableOffHeapMode;
    ClusteredResourcePool restartablePool;

    if (cacheManagerType == CacheManagerType.FULL) {
      restartableOffHeapMode = FULL;
      restartablePool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(sizeInMB, MemoryUnit.MB);
    } else {
      restartableOffHeapMode = PARTIAL;
      restartablePool = cacheManagerType.equals(CacheManagerType.HYBRID) ?
          ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(sizeInMB, MemoryUnit.MB, 50) :
          ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(sizeInMB, MemoryUnit.MB, 0);
    }

    serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(this.getURI().resolve("/" + cacheManagerName))
        .autoCreate()
        .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
        .restartable(dataRootId)
        .withRestartableOffHeapMode(restartableOffHeapMode);

    PersistentCacheManager cacheManager = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(serverSideConfigBuilder)
        .withCache(CACHE, CacheConfigurationBuilder
            .newCacheConfigurationBuilder(keyClass, valueClass,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool)))
        .build(true);
    watcher.addCacheManager(cacheManager);
    return cacheManager;
  }

  private PersistentCacheManager createRestartableCacheManagerLocal(final String cacheManagerName,
                                                                    final CacheManagerType cacheManagerType,
                                                                    final String dataRootId) {
    RestartableOffHeapMode restartableOffHeapMode;

    if (cacheManagerType == CacheManagerType.FULL) {
      restartableOffHeapMode = FULL;
    } else {
      restartableOffHeapMode = PARTIAL;
    }
    serverSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(this.getURI().resolve("/" + cacheManagerName))
        .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
        .autoCreate()
        .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
        .restartable(dataRootId)
        .withRestartableOffHeapMode(restartableOffHeapMode);

    PersistentCacheManager cacheManager = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(serverSideConfigBuilder)
        .build(true);
    watcher.addCacheManager(cacheManager);
    return cacheManager;
  }
}
