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

import org.junit.Test;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;

import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * TransientDatasetInPersistentServerIT
 */
public class TransientDatasetInPersistentServerIT extends BaseClusterWithOffheapAndFRSTest {
  @Test
  public void clusteredDatasetManagerOffheapOnlyTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration)).isTrue();
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> writerReader = dataset.writerReader();
        writerReader.add(10, defineString("name").newCell("John"), defineInt("salary").newCell(100_000));
      }
    }

    CLUSTER.getClusterControl().terminateAllServers();
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForActive();

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", Type.INT)) {
        DatasetWriterReader<Integer> writerReader = dataset.writerReader();
        assertThat(writerReader.add(10, defineString("name").newCell("John"), defineInt("salary").newCell(100_000))).isTrue();
      }
    }

  }

}
