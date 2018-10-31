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
import org.ehcache.Status;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.terracotta.passthrough.IClientTestEnvironment;
import org.terracotta.passthrough.IClusterControl;
import org.terracotta.passthrough.ICommonTest;
import org.terracotta.testing.api.ITestMaster;

import com.tc.util.Assert;
import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.testing.api.ExtendedTestClusterConfiguration;
import com.terracottatech.testing.support.ExtendedHarnessTest;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BasicCacheOpsMultiClientIT extends ExtendedHarnessTest implements ICommonTest {

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>\n"
      + "</config>\n"
      + "<config>"
      + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
      + "<data:directory name=\"root\">../data</data:directory>\n"
      + "</data:data-directories>\n"
      + "</config>\n";

  private static final String CLUSTERED_CACHE_NAME    = "clustered-cache";
  private static final String SYN_CACHE_NAME = "syn-cache";
  private static final String PRIMARY_SERVER_RESOURCE_NAME = "primary-server-resource";
  private static final String DATA_ROOT               = "root";
  private static final String CACHE_MANAGER_NAME = "/restart-crud-cm";
  private static final int PRIMARY_SERVER_RESOURCE_SIZE = 4; //MB
  private static final int NUM_CLIENTS = 2;


  @Override
  public void runSetup(IClientTestEnvironment iClientTestEnvironment, IClusterControl iClusterControl) throws Throwable {
    try (PersistentCacheManager cacheManager = createCacheManager(URI.create(iClientTestEnvironment.getClusterUri()),
        true, true)) {
      assertThat(cacheManager.getStatus(), is(Status.AVAILABLE));
    }
    // close the cache manager to avoid hitting TDB-3894. While this does not fail the test itself, it is
    // unnecessarily throwing an exception when the client JVM doing the setup shuts down
  }

  @Override
  public void runDestroy(IClientTestEnvironment iClientTestEnvironment, IClusterControl iClusterControl) throws Throwable {
    PersistentCacheManager persistentCacheManager = createCacheManager(URI.create(iClientTestEnvironment.getClusterUri()), false, false);
    persistentCacheManager.destroy();
  }

  @Override
  public void runTest(IClientTestEnvironment iClientTestEnvironment, IClusterControl iClusterControl) throws Throwable {
    boolean secondClient = false;
    CustomValue customValue = new CustomValue("value");
    try (PersistentCacheManager persistentCacheManager = createCacheManager(
        URI.create(iClientTestEnvironment.getClusterUri()), false, true)) {
      final Cache<String, Boolean> synCache = persistentCacheManager.getCache(SYN_CACHE_NAME, String.class, Boolean.class);
      final String firstClientStartKey = "first_client_start", firstClientEndKey = "first_client_end";
      if (synCache.putIfAbsent(firstClientStartKey, true) == null) {
        final Cache<Long, CustomValue> customValueCache = persistentCacheManager.getCache(CLUSTERED_CACHE_NAME, Long.class, CustomValue.class);
        customValueCache.put(1L, customValue);
        assertThat(customValueCache.get(1L), is(customValue));
        synCache.put(firstClientEndKey, true);
      } else {
        //wait for the first client to finish
        int retry = 0, maxRetryCount = 60;
        while (++retry <= maxRetryCount && synCache.get(firstClientEndKey) == null) {
          Thread.sleep(1000L);
        }

        if (retry > maxRetryCount) {
          Assert.fail("Couldn't find " + firstClientEndKey + " in synCache after " + maxRetryCount + " retries!");
        }

        Cache<Long, CustomValue> customValueCache = persistentCacheManager.getCache(CLUSTERED_CACHE_NAME, Long.class, CustomValue.class);
        assertThat(customValueCache.get(1L), is(customValue));
        secondClient = true;
      }
    }

    if (secondClient) {
      // let only one client verify restart
      iClusterControl.terminateActive();
      iClusterControl.startAllServers();
      iClusterControl.waitForActive();

      try (PersistentCacheManager persistentCacheManager = createCacheManager(URI.create(iClientTestEnvironment.getClusterUri()), false, true)) {
        Cache<Long, CustomValue> customValueCache = persistentCacheManager.getCache(CLUSTERED_CACHE_NAME, Long.class, CustomValue.class);
        assertThat(customValueCache.get(1L), is(customValue));
      }
    }
  }

  @Override
  public ITestMaster<ExtendedTestClusterConfiguration> getTestMaster() {
    return new ITestMaster<ExtendedTestClusterConfiguration>() {
      @Override
      public String getConfigNamespaceSnippet() {
        return "";
      }

      @Override
      public String getServiceConfigXMLSnippet() {
        return RESOURCE_CONFIG;
      }

      @Override
      public List<String> getExtraServerJarPaths() {
        return Collections.emptyList();
      }

      @Override
      public String getTestClassName() {
        return BasicCacheOpsMultiClientIT.class.getCanonicalName();
      }

      @Override
      public String getClientErrorHandlerClassName() {
        return null;
      }

      @Override
      public int getClientsToStart() {
        return NUM_CLIENTS;
      }

      @Override
      public List<ExtendedTestClusterConfiguration> getRunConfigurations() {
        return Collections.singletonList(new ExtendedTestClusterConfiguration("test-cluster", 1, 1));
      }
    };
  }

  private static PersistentCacheManager createCacheManager(URI clusterURI, boolean shouldAutoCreate, boolean shouldInit) {
    EnterpriseServerSideConfigurationBuilder enterpriseServerSideConfigBuilder;
    if(shouldAutoCreate) {
      enterpriseServerSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterURI.resolve(CACHE_MANAGER_NAME))
          .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
        .autoCreate()
        .defaultServerResource(PRIMARY_SERVER_RESOURCE_NAME)
        .restartable(DATA_ROOT)
        .withRestartableOffHeapMode(RestartableOffHeapMode.FULL);
    } else {
      enterpriseServerSideConfigBuilder = EnterpriseClusteringServiceConfigurationBuilder
        .enterpriseCluster(clusterURI.resolve(CACHE_MANAGER_NAME))
          .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
        .expecting()
        .defaultServerResource(PRIMARY_SERVER_RESOURCE_NAME)
        .restartable(DATA_ROOT)
        .withRestartableOffHeapMode(RestartableOffHeapMode.FULL);
    }

    ClusteredResourcePool restartablePool = ClusteredRestartableResourcePoolBuilder
      .clusteredRestartableDedicated(PRIMARY_SERVER_RESOURCE_NAME, PRIMARY_SERVER_RESOURCE_SIZE, MemoryUnit.MB);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = CacheManagerBuilder
      .newCacheManagerBuilder()
      .with(enterpriseServerSideConfigBuilder)
      .withCache(CLUSTERED_CACHE_NAME,
        CacheConfigurationBuilder
          .newCacheConfigurationBuilder(Long.class, CustomValue.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
              .with(restartablePool))
          .add(new ClusteredStoreConfiguration(Consistency.STRONG)))
      .withCache(SYN_CACHE_NAME,
        CacheConfigurationBuilder
          .newCacheConfigurationBuilder(String.class, Boolean.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
              .with(restartablePool))
          .add(new ClusteredStoreConfiguration(Consistency.STRONG)));
    return clusteredCacheManagerBuilder.build(shouldInit);
  }

  private static class CustomValue implements Serializable {
    private static final long serialVersionUID = 2224523518556700622L;
    private final String value;

    private CustomValue(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomValue that = (CustomValue) o;
      return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }
}
