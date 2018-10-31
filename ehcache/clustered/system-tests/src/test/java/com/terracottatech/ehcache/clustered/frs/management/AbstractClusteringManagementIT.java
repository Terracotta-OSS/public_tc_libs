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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.terracottatech.testing.rules.EnterpriseCluster;

import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.management.entity.nms.NmsConfig;
import org.terracotta.management.entity.nms.client.DefaultNmsService;
import org.terracotta.management.entity.nms.client.NmsEntity;
import org.terracotta.management.entity.nms.client.NmsEntityFactory;
import org.terracotta.management.entity.nms.client.NmsService;
import org.terracotta.management.model.cluster.Client;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.cluster.ServerEntityIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated;
import static com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared;
import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static java.util.Collections.emptyList;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class AbstractClusteringManagementIT {

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

  protected static CacheManager cacheManager;
  protected static ClientIdentifier ehcacheClientIdentifier;
  protected static ServerEntityIdentifier clusterTierManagerEntityIdentifier;
  protected static ObjectMapper mapper = new ObjectMapper();

  protected static NmsService nmsService;
  protected static ServerEntityIdentifier tmsServerEntityIdentifier;
  protected static Connection managementConnection;

  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void beforeClass() throws Exception {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    CLUSTER.getClusterControl().waitForActive();

    // simulate a TMS client
    managementConnection = ConnectionFactory.connect(CLUSTER.getStripeConnectionURI(0), new Properties());
    NmsEntityFactory entityFactory = new NmsEntityFactory(managementConnection, AbstractClusteringManagementIT.class.getName());
    NmsEntity nmsEntity = entityFactory.retrieveOrCreate(new NmsConfig());
    nmsService = new DefaultNmsService(nmsEntity);
    nmsService.setOperationTimeout(5, TimeUnit.SECONDS);

    tmsServerEntityIdentifier = readTopology()
        .activeServerEntityStream()
        .filter(serverEntity -> serverEntity.getType().equals(NmsConfig.ENTITY_TYPE))
        .findFirst()
        .get() // throws if not found
        .getServerEntityIdentifier();

    cacheManager = newCacheManagerBuilder()
        // cluster config
        .with(enterpriseCluster(CLUSTER.getConnectionURI().resolve("/my-server-entity-1"))
            .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
            .autoCreate()
            .defaultServerResource("primary-server-resource")
            .resourcePool("resource-pool-a", 28, MemoryUnit.MB, "secondary-server-resource") // <2>
            .resourcePool("resource-pool-b", 16, MemoryUnit.MB)
            .restartable("dataroot")) // will take from primary-server-resource
        // management config
        .using(new DefaultManagementRegistryConfiguration()
            .addTags("webapp-1", "server-node-1")
            .setCacheManagerAlias("my-super-cache-manager"))
        // cache config
        .withCache("dedicated-cache-1", newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .with(clusteredRestartableDedicated("primary-server-resource", 4, MemoryUnit.MB)))
            .build())
        .withCache("shared-cache-2", newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .with(clusteredRestartableShared("resource-pool-a")))
            .build())
        .withCache("shared-cache-3", newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .with(clusteredRestartableShared("resource-pool-b")))
            .build())
        .build(true);

    // ensure the CM is running and get its client id
    assertThat(cacheManager.getStatus(), equalTo(Status.AVAILABLE));
    ehcacheClientIdentifier = readTopology().getClients().values()
        .stream()
        .filter(client -> client.getName().equals("Ehcache:my-server-entity-1"))
        .findFirst()
        .map(Client::getClientIdentifier)
        .get();

    clusterTierManagerEntityIdentifier = readTopology()
        .activeServerEntityStream()
        .filter(serverEntity -> serverEntity.getName().equals("my-server-entity-1"))
        .findFirst()
        .get() // throws if not found
        .getServerEntityIdentifier();

    // test_notifs_sent_at_CM_init
    waitForAllNotifications(
        "CLIENT_CONNECTED", "CLIENT_CONNECTED",
        "CLIENT_DISCONNECTED",
        "CLIENT_REGISTRY_AVAILABLE",
        "CLIENT_TAGS_UPDATED",
        "EHCACHE_RESOURCE_POOLS_CONFIGURED",
        "EHCACHE_SERVER_STORE_CREATED", "EHCACHE_SERVER_STORE_CREATED", "EHCACHE_SERVER_STORE_CREATED",
        "EHCACHE_RESTART_STORE_CREATED",
        "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE",
        "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED",
        "SERVER_ENTITY_DESTROYED",
        "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED",
        "SERVER_ENTITY_UNFETCHED", "SERVER_ENTITY_UNFETCHED");

    sendManagementCallOnEntityToCollectStats();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (cacheManager != null && cacheManager.getStatus() == Status.AVAILABLE) {

      if (nmsService != null) {
        Context ehcacheClient = readTopology().getClient(ehcacheClientIdentifier).get().getContext().with("cacheManagerName", "my-super-cache-manager");
        nmsService.stopStatisticCollector(ehcacheClient).waitForReturn();
      }

      cacheManager.close();
    }

    if (nmsService != null) {
      Context context = readTopology().getSingleStripe().getActiveServerEntity(tmsServerEntityIdentifier).get().getContext();
      nmsService.stopStatisticCollector(context);

      managementConnection.close();
    }
  }

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(60);

  protected static org.terracotta.management.model.cluster.Cluster readTopology() throws Exception {
    return nmsService.readTopology();
  }

  protected static void sendManagementCallOnClientToCollectStats() throws Exception {
    Context ehcacheClient = readTopology().getClient(ehcacheClientIdentifier).get().getContext()
        .with("cacheManagerName", "my-super-cache-manager");
    nmsService.startStatisticCollector(ehcacheClient, 1, TimeUnit.SECONDS).waitForReturn();
  }

  protected static List<ContextualStatistics> waitForNextStats() throws Exception {
    // uses the monitoring consumer entity to get the content of the stat buffer when some stats are collected
    while (!Thread.currentThread().isInterrupted()) {
      List<ContextualStatistics> messages = Stream.of(nmsService.waitForMessage())
          .filter(message -> message.getType().equals("STATISTICS"))
          .flatMap(message -> message.unwrap(ContextualStatistics.class).stream())
          .collect(Collectors.toList());
      if (messages.isEmpty()) {
        Thread.yield();
      } else {
        return messages;
      }
    }
    return emptyList();
  }

  protected static List<String> notificationTypes(List<Message> messages) {
    return messages
        .stream()
        .filter(message -> "NOTIFICATION".equals(message.getType()))
        .flatMap(message -> message.unwrap(ContextualNotification.class).stream())
        .map(ContextualNotification::getType)
        .collect(Collectors.toList());
  }

  protected static String read(String path) {
    try (Scanner scanner = new Scanner(AbstractClusteringManagementIT.class.getResourceAsStream(path), "UTF-8")) {
      return scanner.useDelimiter("\\A").next();
    }
  }

  protected static String normalizeForLineEndings(String stringToNormalize) {
    return stringToNormalize.replace("\r\n", "\n").replace("\r", "\n");
  }

  private static void sendManagementCallOnEntityToCollectStats() throws Exception {
    Context context = readTopology().getSingleStripe().getActiveServerEntity(tmsServerEntityIdentifier).get().getContext();
    nmsService.startStatisticCollector(context, 1, TimeUnit.SECONDS).waitForReturn();
  }

  protected static void waitForAllNotifications(String... notificationTypes) throws InterruptedException {
    List<String> waitingFor = new ArrayList<>(Arrays.asList(notificationTypes));
    List<ContextualNotification> missingOnes = new ArrayList<>();

    Thread t = new Thread(() -> {
      try {
        nmsService.waitForMessage(message -> {
          if (message.getType().equals("NOTIFICATION")) {
            for (ContextualNotification notification : message.unwrap(ContextualNotification.class)) {
              if(!waitingFor.remove(notification.getType())) {
                missingOnes.add(notification);
              }
            }
          }
          return waitingFor.isEmpty();
        });
      } catch (InterruptedException e) {
        // Get out
      }
    });

    t.start();
    t.join(30_000); // should be way enough to receive all messages
    t.interrupt(); // we interrupt the thread that is waiting on the message queue

    assertTrue("Still waiting for: " + waitingFor, waitingFor.isEmpty());
    assertTrue("Unexpected notification: " + missingOnes, missingOnes.isEmpty());
  }
}
