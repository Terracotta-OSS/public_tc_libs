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
package com.terracottatech.tools.tests;


import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.testing.rules.EnterpriseCluster;
import com.terracottatech.tools.clustertool.ClusterTool;
import com.terracottatech.tools.clustertool.managers.DefaultDiagnosticManager;
import com.terracottatech.tools.clustertool.managers.DiagnosticManager;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.terracottatech.tools.clustertool.managers.DefaultDiagnosticManager.ConnectionCloseableDiagnosticsEntity;
import static com.terracottatech.utilities.InetSocketAddressConvertor.getInetSocketAddress;

public class BackupTestHelper {

  private static final String RESOURCE_CONFIG =
      "    <config\n" +
          "      xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>\n" +
          "      <ohr:offheap-resources>\n" +
          "        <ohr:resource name=\"primary-server-resource\" unit=\"GB\">1</ohr:resource>\n" +
          "      </ohr:offheap-resources>\n" +
          "    </config>\n" +
          "    <config>\n" +
          "      <data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n" +
          "        <data:directory name=\"root\">../data</data:directory>\n" +
          "      </data:data-directories>\n" +
          "    </config>\n" +
          "    <service xmlns:persistence=\"http://www.terracottatech.com/config/platform-persistence\">\n" +
          "      <persistence:platform-persistence data-directory-id=\"root\"/>\n" +
          "    </service>\n" +
          "    <service>\n" +
          "      <backup-restore xmlns=\"http://www.terracottatech.com/config/backup-restore\">\n" +
          "        <backup-location path=\"BACKUP_PATH_TO_BE_REPLACED\" />\n" +
          "      </backup-restore>\n" +
          "    </service>";

  private static DiagnosticManager diagnosticManager = new DefaultDiagnosticManager();

  public static String resourceConfigWithBackupPath(String backupPath) {
    return RESOURCE_CONFIG.replace("BACKUP_PATH_TO_BE_REPLACED", backupPath);
  }

  public static void backupViaClusterTool(EnterpriseCluster cluster) throws IOException {
    backupViaClusterTool(cluster, false);
  }

  public static void backupViaClusterTool(EnterpriseCluster cluster, boolean explicitServerOption) throws IOException {
    String[] args;
    if (explicitServerOption) {
      args = new String[]{"backup", "-n", "primary", "-s", getActiveServerHostPort(cluster)};
    } else {
      args = new String[]{"backup", "-n", "primary", getActiveServerHostPort(cluster)};
    }
    ClusterTool.main(args);
  }

  public static String getActiveServerHostPort(EnterpriseCluster cluster) throws IOException {
    URI connectionURI = cluster.getConnectionURI();
    String[] hostPorts = connectionURI.toString().replace("terracotta://", "").split(",");

    for (String hostPort : hostPorts) {
      try (ConnectionCloseableDiagnosticsEntity closeableDiagnosticsEntity = diagnosticManager.getEntity(getInetSocketAddress(hostPort), null)) {
        if (closeableDiagnosticsEntity.getDiagnostics().getState().contains("ACTIVE")) {
          return hostPort;
        }
      } catch (RuntimeException e) {
        if (contains(e, TimeoutException.class)) {
          continue;
        }
        throw e;
      }
    }

    throw new IllegalStateException("Unable to find valid active host port.");
  }

  private static <T> boolean contains(Throwable e, Class<T> cls) {
    while (true) {
      if (e == null) {
        return false;
      }
      if (e.getClass() == cls) {
        return true;
      }
      e = e.getCause();
    }
  }

  public static void populateEhCache(EnterpriseCluster cluster) {
    operationOnCache(cluster, cache -> {
      for (long i = 0 ; i < 100; i++) {
        cache.put(i, "test" + i);
      }
    });
  }

  public static void populateTCStore(EnterpriseCluster cluster) throws Exception {
    operationOnTcstore(cluster, writerReader -> {
      for (long i = 0; i < 100; i++) {
        writerReader.add(i, CellDefinition.defineString("stringCell").newCell("test" + i));
      }
    });
  }

  private static void operationOnCache(EnterpriseCluster cluster, Consumer<Cache<Long, String>> cacheConsumer) {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(cluster.getConnectionURI())
                .timeouts(TimeoutsBuilder.timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(30)))
                .autoCreate()
                .restartable("root"))
            .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredRestartableResourcePoolBuilder
                        .clusteredRestartableDedicated("primary-server-resource", 100, MemoryUnit.MB))));

    try (PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true)) {
      Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
      cacheConsumer.accept(cache);
    }
  }

  private static void operationOnTcstore(EnterpriseCluster cluster, Consumer<DatasetWriterReader<Long>> datasetWriterReaderConsumer) throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(cluster.getConnectionURI()).build()) {
      datasetManager.newDataset("longData", Type.LONG, datasetManager.datasetConfiguration().offheap("primary-server-resource").disk("root").build());
      try (Dataset<Long> dataset = datasetManager.getDataset("longData", Type.LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        datasetWriterReaderConsumer.accept(writerReader);
      }
    }
  }

}
