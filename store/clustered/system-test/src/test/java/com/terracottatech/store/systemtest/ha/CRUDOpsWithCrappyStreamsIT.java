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
package com.terracottatech.store.systemtest.ha;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.systemtest.BaseSystemTest;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Ignore("Long running test")
public class CRUDOpsWithCrappyStreamsIT extends BaseSystemTest {

  private static final Properties SERVER_PROPERTIES;
  static {
    Properties serverProperties = new Properties();
    serverProperties.setProperty("com.terracottatech.store.mutative-pipelines.enable-all", "true");
    SERVER_PROPERTIES = serverProperties;
  }

  @ClassRule
  public static EnterpriseCluster CLUSTER =
      initClusterWithPassive("OffheapAndFRS.xmlfrag", 2, () -> SERVER_PROPERTIES);

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);

  private DatasetWriterReader<Integer> writerReader;

  @Test
  public void testCRUDWithLongRunningStreams() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    DatasetManager clusteredDatasetManager = DatasetManager.clustered(CLUSTER.getStripeConnectionURI(0)).build();

    AdvancedDatasetConfigurationBuilder builder = (AdvancedDatasetConfigurationBuilder) clusteredDatasetManager.datasetConfiguration();
    DatasetConfiguration datasetConfiguration = builder.concurrencyHint(2)
        .offheap("primary-server-resource")
        .disk("cluster-disk-resource")
        .index(nameCell, IndexSettings.BTREE)
        .build();

    clusteredDatasetManager.newDataset("passiveSync", Type.INT, datasetConfiguration);
    Dataset<Integer> dataset = clusteredDatasetManager.getDataset("passiveSync", Type.INT);

    writerReader = dataset.writerReader();

    Random random = new Random();

    Set<Integer> added  = new HashSet<>();

    random.ints(10000).forEach( x -> {
      added.add(x);
      writerReader.add(x, nameCell.newCell("Name" + x), phoneCell.newCell(x));
    });

    ExecutorService service = Executors.newFixedThreadPool(8);
    CompletableFuture<?>[] updaters = new CompletableFuture<?>[8];

    for (int i = 0; i < 4; i++) {
      int temp = i;
      updaters[i] = CompletableFuture.runAsync(() -> writerReader.records()
          .filter(record -> (Math.abs(record.getKey()) % 4) == temp )
          .forEach(cells -> {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }), service);
    }

    for (int i = 4; i < 8; i++) {
      int temp = i - 4;
      updaters[i] = CompletableFuture.runAsync(() -> added.forEach(x -> {
        if ((Math.abs(x) % 4) == temp) {
          Assert.assertTrue(writerReader.update(x, UpdateOperation.write(nameCell.newCell("Updated" + x + "by id " + temp))));
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }), service);
    }

    CompletableFuture.allOf(updaters).get();

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    writerReader.records().forEach(x -> {
      Assert.assertTrue(added.contains(x.getKey()));
      int id = (Math.abs(x.getKey()) % 4);
      Assert.assertTrue(x.get(nameCell).get().endsWith(Integer.toString(id)));
    });
  }
}
