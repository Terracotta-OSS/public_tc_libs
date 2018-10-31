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
package com.terracottatech.store.systemtest;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class IndexReplicationIT extends BaseSystemTest {
  @ClassRule
  public static EnterpriseCluster CLUSTER = initClusterWithPassive("OffheapAndFRS.xmlfrag", 2);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static final StringCellDefinition CELL = CellDefinition.defineString("cell");

  @Before
  public void before() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @After
  public void after() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
  }

  @Test
  public void createIndexAndFailThenRestart() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();
    try (DatasetManager manager = DatasetManager.clustered(connectionURI).build()) {

      DatasetConfiguration configuration = manager.datasetConfiguration()
              .offheap(CLUSTER_OFFHEAP_RESOURCE)
              .disk(CLUSTER_DISK_RESOURCE)
              .build();
      manager.newDataset("store-0", Type.INT, configuration);

      try (Dataset<Integer> dataset = manager.getDataset("store-0", Type.INT)) {
        dataset.getIndexing().createIndex(CELL, IndexSettings.BTREE).get();
      }
    }

    CLUSTER.getClusterControl().terminateAllServers();
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    try (DatasetManager manager = DatasetManager.clustered(connectionURI).build()) {
      try (Dataset<Integer> dataset = manager.getDataset("store-0", Type.INT)) {
        Index<?> index = dataset.getIndexing().getLiveIndexes().stream().findFirst().get();
        assertEquals(CELL, index.on());
      }
    }
  }

  @Test
  public void duplicateIndex() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();
    try (DatasetManager manager = DatasetManager.clustered(connectionURI).build()) {

      DatasetConfiguration configuration = manager.datasetConfiguration()
              .offheap(CLUSTER_OFFHEAP_RESOURCE)
              .build();
      manager.newDataset("store-1", Type.INT, configuration);
      try (Dataset<Integer> dataset = manager.getDataset("store-1", Type.INT)) {
        dataset.getIndexing().createIndex(CELL, IndexSettings.BTREE).get();
        exception.expectCause(instanceOf(IllegalArgumentException.class));
        dataset.getIndexing().createIndex(CELL, IndexSettings.BTREE).get();
      }
    }
  }
}
