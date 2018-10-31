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

import org.ehcache.Cache;
import org.junit.Test;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class EnterpriseClusteringManagementServiceIT extends AbstractClusteringManagementIT {
  @Test
  public void test_D_server_capabilities_exposed() throws Exception {
    Capability[] capabilities = readTopology().getSingleStripe()
        .getActiveServerEntity(clusterTierManagerEntityIdentifier)
        .get()
        .getManagementRegistry()
        .get()
        .getCapabilities()
        .toArray(new Capability[0]);

    assertThat(capabilities.length).isEqualTo(3);

    assertThat(capabilities[0].getName()).isEqualTo("ClusterTierManagerSettings");
    assertThat(capabilities[1].getName()).isEqualTo("PoolSettings");
    assertThat(capabilities[2].getName()).isEqualTo("PoolStatistics");

    assertThat(capabilities[0].getDescriptors()).hasSize(1);
    Settings settings = (Settings) capabilities[0].getDescriptors().iterator().next();
    assertThat(settings.get("type")).isEqualTo("ClusterTierManager");
    assertThat(settings.get("clusterTierManager")).isNotNull();
    assertThat(settings.get("defaultServerResource")).isEqualTo("primary-server-resource");
    assertThat(settings.get("restartableStoreId")).isEqualTo("dataroot#default-frs-container#default-cachedata");
    assertThat(settings.get("restartableStoreRoot")).isEqualTo("dataroot");
    assertThat(settings.get("restartableStoreContainer")).isEqualTo("default-frs-container");
    assertThat(settings.get("restartableStoreName")).isEqualTo("default-cachedata");
    assertThat(settings.get("offheapMode")).isEqualTo("FULL");

  }

  @Test
  public void test_G_disk_usage_server_stats_exposed() throws Exception {
    sendManagementCallOnClientToCollectStats();
    Cache<String, String> cache1 = cacheManager.getCache("dedicated-cache-1", String.class, String.class);

    cache1.put("key1", fillString(1024, 'A'));

    long oldDiskUsage = assertMinDiskUsage(1);

    // now add
    cache1.put("key2", fillString(2048, 'B'));
    cache1.put("key3", fillString(2048, 'C'));

    assertMinDiskUsage(oldDiskUsage + 4096);
  }

  private long assertMinDiskUsage(long minExpected) throws Exception {
    long diskUsage;
    List<ContextualStatistics> serverStats;
    do {
      serverStats = getServerStats();
      diskUsage = getDiskUsage(serverStats, "dataroot");
    } while (diskUsage < minExpected && !Thread.currentThread().isInterrupted());
    assertThat(getDiskUsage(serverStats, "root")).isGreaterThan(0L);
    assertThat(getDiskUsage(serverStats, "dataroot")).isGreaterThanOrEqualTo(minExpected);
    return diskUsage;
  }

  private String fillString(int size, char val) {
    return Stream.generate(() -> String.valueOf(val)).limit(size).collect(Collectors.joining());
  }

  private List<ContextualStatistics> getServerStats() throws Exception {
    List<ContextualStatistics> allStats = new ArrayList<>();

    // wait until we have some stats coming from the server entity
    while (!Thread.currentThread().isInterrupted() && !allStats.stream()
        .filter(statistics -> statistics.getContext().contains("consumerId"))
        .findFirst()
        .isPresent()) {
      allStats.addAll(waitForNextStats());
    }
    return allStats.stream()
        .filter(statistics -> statistics.getContext().contains("consumerId"))
        .collect(Collectors.toList());
  }

  private long getDiskUsage(List<ContextualStatistics> serverStats, String dataRootName) {
    List<ContextualStatistics> dataRootStats = serverStats.stream()
        .filter(statistics -> statistics.getCapability().equals("DataRootStatistics"))
        .collect(Collectors.toList());
    for (ContextualStatistics stat : dataRootStats) {
      if (stat.getContext().get("alias").equals(dataRootName)) {
        return stat.<Long>getLatestSampleValue("DataRoot:TotalDiskUsage").get();
      }
    }
    return 0;
  }
}
