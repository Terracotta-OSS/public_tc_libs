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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateServiceProvider;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static com.terracottatech.ehcache.clustered.common.RestartConfiguration.DEFAULT_CONTAINER_NAME;
import static com.terracottatech.ehcache.clustered.common.RestartConfiguration.DEFAULT_DATA_LOG_NAME_HYBRID;
import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.FULL;
import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.PARTIAL;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EnterpriseClusteredCacheTest extends BasicAbstractEnterprisePassthroughTest {
  private static final String DEFAULT_PRIMARY_RESOURCE = "primary-server-resource";
  private static final String SECONDARY_RESOURCE = "secondary-server-resource";
  private static final String SHARED_POOL_A = "resource-pool-a";
  private static final String SHARED_POOL_B = "resource-pool-b";
  private static final String DATA_ROOT_ID = "root1";
  private static final String EHCACHE_STORAGE_SPACE = "ehcache" + File.separator + "frs";
  private static final String METADATA_DIR_NAME = "metadata";
  private String SERVER_NAME;
  private String DATA_ROOT_DIR;
  private final Random rand = new Random();

  @After
  public void cleanup() throws Exception {
    newCacheManagerBuilder()
        .with(EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(clusterURI()).expecting())
        .build().destroy();
  }

  private URI clusterURI() {
    return URI.create(buildClusterUri().toString() + "/my-application");
  }

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    DATA_ROOT_DIR = createDataRootDir();
    SERVER_NAME = getServerName();
    return new ServerInitializerBuilder()
        .resource("primary-server-resource", 64, MemoryUnit.MB)
        .resource("secondary-server-resource", 64, MemoryUnit.MB)
        .dataRoot(DATA_ROOT_ID, DATA_ROOT_DIR)
        .dataRoot("root2", createDataRootDir())
        .dataRoot("root3", createDataRootDir())
        .serviceProvider(new FRSPersistenceServiceProvider(), new FRSPersistenceServiceProviderConfiguration("root2"))
        .overrideServiceProvider(new EnterpriseEhcacheStateServiceProvider(), () -> EnterpriseEhcacheStateServiceProvider.class)
        .build();
  }

  @Test
  public void testClusteredRestartableDedicatedCacheWithKeysAndValuesOffHeapMode() {
    testSimpleCacheManagerWithRestartConfig("root1", null, FULL, Long.class, String.class, this::assertBasicCacheFunctionality);
  }

  @Test
  public void testClusteredRestartableDedicatedAndSharedCachesWithKeysAndValuesOffHeapMode() {
    final RestartConfiguration restartConfigKeysAndValues = new RestartConfiguration("root1", FULL, "test");
    testMultipleWithRestartConfig(restartConfigKeysAndValues, Long.class, String.class, this::assertBasicCacheFunctionality);
  }

  @Test
  public void testClusteredRestartableAndNonRestartableDedicatedAndSharedCachesWithKeysAndValuesOffHeapMode() {
    final RestartConfiguration restartConfigKeysAndValues = new RestartConfiguration("root1", FULL, "test");
    testMixedWithRestartConfig(restartConfigKeysAndValues, Long.class, String.class, this::assertBasicCacheFunctionality);
  }

  @Test
  public void testClusteredRestartableSharedAndDedicatedCachesWithKeysOnlyOffHeapMode() throws Exception {
    final RestartConfiguration restartConfigKeysOnly = new RestartConfiguration("root1", PARTIAL, "test");
    testMultipleWithRestartConfig(restartConfigKeysOnly, Long.class, String.class, this::assertBasicCacheFunctionality);
    assertTrue(Files.exists(Paths.get(DATA_ROOT_DIR, SERVER_NAME, EHCACHE_STORAGE_SPACE, DEFAULT_CONTAINER_NAME, DEFAULT_DATA_LOG_NAME_HYBRID)));
  }

  @Test
  public void testClusteredRestartableSharedAndDedicatedCachesWithKeysWithPartialValuesOffHeapMode() throws Exception {
    final RestartConfiguration restartConfigPartialHybrid = new RestartConfiguration("root1", FULL, "test");
    testMultipleWithRestartConfig(restartConfigPartialHybrid, Long.class, String.class, this::assertBasicCacheFunctionality);
  }

  @Test
  public void testMetadataPersistence() throws Exception {
    testSimpleCacheManagerWithRestartConfig(DATA_ROOT_ID, null, FULL, Long.class, String.class,
        this::assertBasicCacheFunctionality);

    assertTrue(Files.exists(Paths.get(DATA_ROOT_DIR, SERVER_NAME, EHCACHE_STORAGE_SPACE, DEFAULT_CONTAINER_NAME, METADATA_DIR_NAME)));
    assertEquals(1L, countDirEntries(DATA_ROOT_DIR));
  }

  private long countDirEntries(String dirName) throws IOException {
    try (Stream<Path> list = Files.list(Paths.get(dirName))) {
      return list.count();
    }
  }

  @Test
  public void testMetadataPersistenceWithStateRepositoryConfig() throws Exception {
    final RestartConfiguration restartConfig = new RestartConfiguration(DATA_ROOT_ID, FULL, "test");
    testMixedWithRestartConfig(restartConfig, Long.class, CustomValue.class, this::assertCustomCacheFunctionality);
  }

  private <K, V> void testMultipleWithRestartConfig(RestartConfiguration restartConfiguration, Class<K> keyClazz, Class<V> valueClazz,
                                                 Consumer<Cache<K, V>> cacheConsumer) {
    final String testCacheName1 = "dedicated-cache";
    final String testCacheName2 = "shared-cache-1";
    final String testCacheName3 = "shared-cache-2";

    final ClusteredResourcePool sharedPool1;
    final ClusteredResourcePool sharedPool2;
    final ClusteredResourcePool dedicatedPool;
    if (restartConfiguration.isHybrid()) {
      dedicatedPool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(32, MemoryUnit.MB, 50);
      sharedPool1 = ClusteredResourcePoolBuilder.clusteredShared(SHARED_POOL_A);
      sharedPool2 = ClusteredResourcePoolBuilder.clusteredShared(SHARED_POOL_B);
    } else {
      dedicatedPool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(32, MemoryUnit.MB);
      sharedPool1 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_A);
      sharedPool2 = ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_B);
    }
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(enterpriseCluster(clusterURI()).autoCreate()
                .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
                .resourcePool(SHARED_POOL_A, 28, MemoryUnit.MB, SECONDARY_RESOURCE)
                .resourcePool(SHARED_POOL_B, 32, MemoryUnit.MB)
                .restartable(restartConfiguration.getRestartableLogRoot())
                .withRestartableOffHeapMode(restartConfiguration.getOffHeapMode())
                .withRestartIdentifier(restartConfiguration.getFrsIdentifier()))
            .withCache(testCacheName1, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClazz, valueClazz,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(dedicatedPool)))
            .withCache(testCacheName2, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClazz, valueClazz,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(sharedPool1)))
            .withCache(testCacheName3, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClazz, valueClazz,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(sharedPool2)));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<K, V> cache1 = cacheManager.getCache(testCacheName1, keyClazz, valueClazz);
    final Cache<K, V> cache2 = cacheManager.getCache(testCacheName2, keyClazz, valueClazz);
    final Cache<K, V> cache3 = cacheManager.getCache(testCacheName3, keyClazz, valueClazz);

    cacheConsumer.accept(cache1);
    cacheConsumer.accept(cache2);
    cacheConsumer.accept(cache3);

    cacheManager.close();
  }

  private <K, V> void testMixedWithRestartConfig(RestartConfiguration restartConfiguration, Class<K> keyClazz, Class<V> valueClazz,
                                                    Consumer<Cache<K, V>> cacheConsumer) {
    final String testCacheName1 = "dedicated-cache";
    final String testCacheName2 = "shared-cache-1";
    final String testCacheName3 = "shared-cache-2";

    DedicatedClusteredResourcePool dedicatedRestartablePool =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(32, MemoryUnit.MB);
    SharedClusteredResourcePool sharedRestartablePool =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(SHARED_POOL_A);

    assertThat(dedicatedRestartablePool.toString(), containsString("(restartable)"));
    assertThat(sharedRestartablePool.toString(), containsString("(restartable)"));

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(enterpriseCluster(clusterURI()).autoCreate()
                .defaultServerResource(DEFAULT_PRIMARY_RESOURCE)
                .resourcePool(SHARED_POOL_A, 28, MemoryUnit.MB, SECONDARY_RESOURCE)
                .resourcePool(SHARED_POOL_B, 32, MemoryUnit.MB)
                .restartable(restartConfiguration.getRestartableLogRoot())
                .withRestartableOffHeapMode(restartConfiguration.getOffHeapMode())
                .withRestartIdentifier(restartConfiguration.getFrsIdentifier()))
            .withCache(testCacheName1, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClazz, valueClazz,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(dedicatedRestartablePool)))
            .withCache(testCacheName2, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClazz, valueClazz,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(sharedRestartablePool)))
            .withCache(testCacheName3, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClazz, valueClazz,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredShared(SHARED_POOL_A))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<K, V> cache1 = cacheManager.getCache(testCacheName1, keyClazz, valueClazz);
    final Cache<K, V> cache2 = cacheManager.getCache(testCacheName2, keyClazz, valueClazz);
    final Cache<K, V> cache3 = cacheManager.getCache(testCacheName3, keyClazz, valueClazz);

    cacheConsumer.accept(cache1);
    cacheConsumer.accept(cache2);
    cacheConsumer.accept(cache3);

    cacheManager.close();
  }

  protected PersistentCacheManager createCacheManager(URI uri, boolean autoCreate, boolean restartable) {
    EnterpriseServerSideConfigurationBuilder enterpriseServerSideConfigBuilder;
    if (autoCreate) {
      enterpriseServerSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
          .enterpriseCluster(uri)
          .autoCreate();
    } else {
      enterpriseServerSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
          .enterpriseCluster(uri)
          .expecting();
    }
    enterpriseServerSideConfigBuilder = enterpriseServerSideConfigBuilder.defaultServerResource("primary-server-resource");
    if (restartable) {
      enterpriseServerSideConfigBuilder = enterpriseServerSideConfigBuilder.restartable("root1")
          .withRestartableOffHeapMode(RestartableOffHeapMode.FULL);
    }

    ClusteredResourcePool pool;
    if (restartable) {
      pool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(
          "primary-server-resource", 50, MemoryUnit.MB);
    } else {
      pool = ClusteredResourcePoolBuilder.clusteredDedicated(
          "primary-server-resource", 50, MemoryUnit.MB);
    }

    CacheManagerBuilder<PersistentCacheManager> builder = CacheManagerBuilder
        .newCacheManagerBuilder()
        .with(enterpriseServerSideConfigBuilder);
    builder = builder.withCache("cache-0", CacheConfigurationBuilder
        .newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(pool))
        .add(new ClusteredStoreConfiguration(Consistency.STRONG)));
    return builder.build(true);
  }

  @Test
  public void testRemove() throws Exception {
    //create cache manager
    PersistentCacheManager cacheManager = createCacheManager(clusterURI(), true, true);
    Cache<Long, String> cache = cacheManager.getCache("cache-0", Long.class, String.class);

    int n=100;
    for (int i=0; i<3; i++) {
      //perform put operations
      for (long key = i * n; key < (i+1) * n; ++key) {
        cache.put(key, "**************" + key);
      }

      // perform remove operations
      for (long key = i * n; key < (i+1) * n; key += 2) {
        cache.remove(key);
        if (cache.get(key) != null)
          throw new AssertionError("Found unexpected key after remove" + key);
      }
    }

    cacheManager.close();
    cacheManager.destroy();
  }

  private <K, V> void testSimpleCacheManagerWithRestartConfig(String logRoot, String frsId,
                                                              RestartableOffHeapMode offHeapMode,
                                                              Class<K> keyClazz, Class<V> valueClazz,
                                                              Consumer<Cache<K, V>> cacheConsumer) {
    ClusteredResourcePool restartablePool =
        ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(DEFAULT_PRIMARY_RESOURCE,
            2, MemoryUnit.MB);
    EnterpriseServerSideConfigurationBuilder.RestartableServerSideConfigurationBuilder ssBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterURI()).autoCreate().restartable(logRoot);
    if (offHeapMode != null) {
      ssBuilder = ssBuilder.withRestartableOffHeapMode(offHeapMode);
    }
    if (frsId != null) {
      ssBuilder = ssBuilder.withRestartIdentifier(frsId);
    }
    final String testCacheName = "clustered-cache";

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ssBuilder)
            .withCache(testCacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(keyClazz, valueClazz,
                ResourcePoolsBuilder.newResourcePoolsBuilder().with(restartablePool)));

    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<K, V> cache = cacheManager.getCache(testCacheName, keyClazz, valueClazz);

    cacheConsumer.accept(cache);

    cacheManager.close();
  }

  private void assertBasicCacheFunctionality(Cache<Long, String> cache) {
    long key = rand.nextLong();
    String value = "Value" + rand.nextInt(100);

    cache.put(key, value);
    assertThat(cache.get(key), is(value));
  }

  private void assertCustomCacheFunctionality(Cache<Long, CustomValue> cache) {
    long key = rand.nextLong();
    CustomValue value = new CustomValue(key+100);

    cache.put(key, value);
    assertThat(cache.get(key), is(new CustomValue(key+100)));
  }

  private static class CustomValue implements Serializable {
    private static final long serialVersionUID = 4140324786288069084L;
    private final long value;

    private CustomValue(long value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomValue that = (CustomValue)o;

      return value == that.value;

    }

    @Override
    public int hashCode() {
      return (int)(value ^ (value >>> 32));
    }
  }
}
