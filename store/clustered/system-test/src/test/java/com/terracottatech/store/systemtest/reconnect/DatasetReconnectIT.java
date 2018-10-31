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
package com.terracottatech.store.systemtest.reconnect;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreOperationAbandonedException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.systemtest.BaseSystemTest;
import com.terracottatech.test.data.Animals;
import com.terracottatech.testing.delay.ClusterDelay;
import com.terracottatech.testing.delay.DelayConnectionService;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;

public class DatasetReconnectIT extends BaseSystemTest {
  public static final String RESOURCE_CONFIG =
          "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
                  + "<ohr:offheap-resources>"
                  + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "</ohr:offheap-resources>"
                  + "</config>\n"
                  + "<config>"
                  + "<data:data-directories xmlns:data='http://www.terracottatech.com/config/data-roots'>"
                  + "<data:directory name='cluster-disk-resource'>disk</data:directory>"
                  + "</data:data-directories>"
                  + "</config>"
                  + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
                  + "<lease:connection-leasing>"
                  + "<lease:lease-length unit='seconds'>5</lease:lease-length>"
                  + "</lease:connection-leasing>"
                  + "</service>";

  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  protected URI getClusterURI() {
    return CLUSTER.getConnectionURI();
  }

  protected ClusterDelay getDelay() {
    return new ClusterDelay("datasetDelay", 1);
  }

  @Test
  public void runTest() throws Exception {
    testReconnectWithCRUD((time, delay) -> delay.setDefaultMessageDelay(time));
  }

  public void testReconnectWithCRUD(BiConsumer<Long, ClusterDelay> delayConsumer) throws Exception {

    URI clusterURI = getClusterURI();
    loadAnimals(clusterURI);

    ClusterDelay clusterDelay = getDelay();
    DelayConnectionService.setDelay(clusterDelay);

    try (DatasetManager manager = DatasetManager.clustered(clusterURI)
            .withConnectionTimeout(2L, TimeUnit.MINUTES).build()) {

      try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {
        DatasetReader<String> reader = animals.reader();

        delayConsumer.accept(6000L, clusterDelay);
        Thread.sleep(10000);

        delayConsumer.accept(0L, clusterDelay);
        try {
          Animals.ANIMALS.forEach(x -> reader.get(x.getName()));
          Assert.fail("StoreOperationAbandonedException expected");
        } catch (StoreOperationAbandonedException e) {
          Animals.ANIMALS.forEach(x -> reader.get(x.getName()));
        }
      }
    }
    DelayConnectionService.setDelay(null);
  }

  public static void loadAnimals(URI connectionURI) throws com.terracottatech.store.StoreException {
    try (DatasetManager manager = DatasetManager.clustered(connectionURI)
            .withConnectionTimeout(1L, TimeUnit.MINUTES).build()) {
      DatasetConfiguration configuration = manager.datasetConfiguration()
              .offheap(CLUSTER_OFFHEAP_RESOURCE)
              .disk(CLUSTER_DISK_RESOURCE)
              .index(TAXONOMIC_CLASS, IndexSettings.btree())
              .index(Animals.Schema.STATUS, IndexSettings.btree())
              .build();
      manager.newDataset("animals", Type.STRING, configuration);

      try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {
        DatasetWriterReader<String> writer = animals.writerReader();
        Animals.ANIMALS.forEach(animal -> animal.addTo(writer::add));
      }
    }
  }
}
