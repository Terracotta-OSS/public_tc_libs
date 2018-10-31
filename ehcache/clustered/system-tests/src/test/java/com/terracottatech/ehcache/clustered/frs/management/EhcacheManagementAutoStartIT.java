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

package com.terracottatech.ehcache.clustered.frs.management;

import com.terracottatech.testing.rules.EnterpriseCluster;
import org.ehcache.CacheManager;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.management.entity.nms.NmsConfig;
import org.terracotta.management.entity.nms.client.DefaultNmsService;
import org.terracotta.management.entity.nms.client.NmsEntity;
import org.terracotta.management.entity.nms.client.NmsEntityFactory;
import org.terracotta.management.entity.nms.client.NmsService;
import org.terracotta.management.model.cluster.Client;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated;
import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class EhcacheManagementAutoStartIT {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
          + "<ohr:resource name=\"secondary-server-resource\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>\n"
          + "</config>\n"
          + "<config>"
          + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
          + "<data:directory name=\"root\" use-for-platform=\"true\">data</data:directory>\n"
          + "<data:directory name=\"dataroot\">dataroot</data:directory>\n"
          + "</data:data-directories>\n"
          + "</config>\n";

  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  private Connection managementConnection;
  private NmsService nmsService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Before
  public void startNms() throws Exception {
    managementConnection = ConnectionFactory.connect(CLUSTER.getStripeConnectionURI(0), new Properties());
    NmsEntityFactory entityFactory = new NmsEntityFactory(managementConnection, EhcacheManagementAutoStartIT.class.getName());
    NmsEntity nmsEntity = entityFactory.retrieveOrCreate(new NmsConfig());
    nmsService = new DefaultNmsService(nmsEntity);
    nmsService.setOperationTimeout(5, TimeUnit.SECONDS);
  }

  @After
  public void stopNms() throws IOException {
    if (managementConnection != null) {
      managementConnection.close();
    }
  }

  @SuppressWarnings({"try", "unused"})
  @Test(timeout = 30_000)
  public void test_default_management_config_is_there() throws Exception {
    try (CacheManager cm = newCacheManagerBuilder()
        // cluster config
        .with(enterpriseCluster(CLUSTER.getConnectionURI().resolve("/my-server-entity-1"))
            .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
            .autoCreate()
            .defaultServerResource("primary-server-resource")
            .resourcePool("resource-pool-a", 28, MemoryUnit.MB, "secondary-server-resource")
            .restartable("dataroot")) // will take from primary-server-resource
        .withCache("dedicated-cache-1", newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .with(clusteredRestartableDedicated("primary-server-resource", 4, MemoryUnit.MB)))
            .build())
        .build(true)) {

      while (!Thread.currentThread().isInterrupted() && !nmsService.readTopology()
          .clientStream()
          .filter(client -> client.getName().contains("Ehcache"))
          .findFirst()
          .flatMap(Client::getManagementRegistry)
          .isPresent()) {
        Thread.sleep(10_000);
      }

      assertThat(Thread.currentThread().isInterrupted()).isFalse();
      Collection<String> client = nmsService.readTopology().clientStream().filter(c -> c.getName().contains("Ehcache")).findFirst().get().getTags();
      assertThat(client).containsExactly("ehcache-ee");
    }
  }

  @SuppressWarnings({"try", "unused"})
  @Test(timeout = 60_000)
  public void test_custom_management_config_is_there() throws Exception {
    try (CacheManager cm = newCacheManagerBuilder()
        // cluster config
        .with(enterpriseCluster(CLUSTER.getConnectionURI().resolve("/my-server-entity-1"))
            .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
            .autoCreate()
            .defaultServerResource("primary-server-resource")
            .resourcePool("resource-pool-a", 28, MemoryUnit.MB, "secondary-server-resource")
            .restartable("dataroot")) // will take from primary-server-resource
        .using(new DefaultManagementRegistryConfiguration()
            .addTags("webapp-1", "server-node-1")
            .setCacheManagerAlias("my-super-cache-manager"))
        .withCache("dedicated-cache-1", newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .with(clusteredRestartableDedicated("primary-server-resource", 4, MemoryUnit.MB)))
            .build())
        .build(true)) {

      while (!Thread.currentThread().isInterrupted() && !nmsService.readTopology()
          .clientStream()
          .filter(client -> client.getName().contains("Ehcache"))
          .findFirst()
          .flatMap(Client::getManagementRegistry)
          .isPresent()) {
        Thread.sleep(10_000);
      }

      assertThat(Thread.currentThread().isInterrupted()).isFalse();
      Collection<String> client = nmsService.readTopology().clientStream().filter(c -> c.getName().contains("Ehcache")).findFirst().get().getTags();
      assertThat(client).contains("webapp-1", "server-node-1");
    }
  }

}
