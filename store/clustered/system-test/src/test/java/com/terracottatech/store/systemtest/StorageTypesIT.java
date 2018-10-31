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

import org.junit.After;
import org.junit.Test;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.manager.DatasetManager;

import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * StorageTypesIT
 */
public class StorageTypesIT extends BaseClusterWithOffheapAndFRSTest {
  private static final int NUM_ITERATIONS = 100;
  private static final int NUM_DESTROYS = 6;
  private static final String DATASET_NAME1 = "employee1";
  private static final String DATASET_NAME2 = "employee2";

  @After
  public void destroy() {
    silentDestroyDatasets(DATASET_NAME2, DATASET_NAME1);
  }

  @Test
  public void testHybridStorageType() throws Exception {
    runStorageTypeTest(PersistentStorageEngine.HYBRID);
  }

  @Test
  public void testFRSStorageType() throws Exception {
    runStorageTypeTest(PersistentStorageEngine.FRS);
  }

  @Test
  public void testMultiStorageTypeDestroyLoop() throws Exception {
    for (int i = 0; i < NUM_DESTROYS; i++) {
      createAndLoadDataset((i % 2 == 0) ? PersistentStorageEngine.FRS : PersistentStorageEngine.HYBRID,
          DATASET_NAME1, DATASET_NAME2 );
      verifyDatasets(DATASET_NAME1, DATASET_NAME2);
      silentDestroyDatasets(DATASET_NAME2, DATASET_NAME1);
    }
  }

  @Test
  public void testConflictingStorageTypesForSameDiskResource() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration1 = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE, PersistentStorageEngine.FRS)
          .durabilityEveryMutation()
          .build();

      DatasetConfiguration datasetConfiguration2 = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE, PersistentStorageEngine.HYBRID)
          .durabilityEveryMutation()
          .build();
      assertThat(datasetManager.newDataset(DATASET_NAME1, INT, datasetConfiguration1)).isTrue();
      try {
        datasetManager.newDataset(DATASET_NAME2, INT, datasetConfiguration2);
        fail("Not expected to create a new dataset with different storage type for same disk resource");
      } catch (StoreException e) {
        assertThat(e.getMessage()).contains("Incompatible Storage Types");
      }
      assertThat(datasetManager.newDataset(DATASET_NAME2, INT, datasetConfiguration1)).isTrue();

      try (Dataset<Integer> dataset = datasetManager.getDataset(DATASET_NAME2, INT)) {
        DatasetWriterReader<Integer> writerReader = dataset.writerReader();
        for (int i = 0; i < NUM_ITERATIONS; i++) {
          writerReader.add(i, defineString("name").newCell("John" + " i"),
              defineInt("salary").newCell(10_000 * i));
        }
      }
    }
  }

  @Test
  public void testDifferentStorageTypesForDifferentDiskResource() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration1 = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE, PersistentStorageEngine.FRS)
          .durabilityEveryMutation()
          .build();

      DatasetConfiguration datasetConfiguration2 = datasetManager.datasetConfiguration()
          .offheap(SECONDARY_OFFHEAP_RESOURCE)
          .disk(SECONDARY_DISK_RESOURCE, PersistentStorageEngine.HYBRID)
          .durabilityEveryMutation()
          .build();
      assertThat(datasetManager.newDataset(DATASET_NAME1, INT, datasetConfiguration1)).isTrue();
      assertThat(datasetManager.newDataset(DATASET_NAME2, INT, datasetConfiguration2)).isTrue();

      // write to any one
      try (Dataset<Integer> dataset = datasetManager.getDataset(DATASET_NAME2, INT)) {
        DatasetWriterReader<Integer> writerReader = dataset.writerReader();
        for (int i = 0; i < NUM_ITERATIONS; i++) {
          writerReader.add(i, defineString("name").newCell("John" + " i"),
              defineInt("salary").newCell(10_000 * i));
        }
      }
    }
  }

  @Test
  public void testConflictingStorageTypesOnClose() throws Exception {
    createAndLoadDataset(PersistentStorageEngine.FRS, DATASET_NAME1);
    try {
      createAndLoadDataset(PersistentStorageEngine.HYBRID, DATASET_NAME2);
      fail("Not expected that two datasets using same disk resource uses different storage types");
    } catch (StoreException e) {
      assertThat(e.getMessage()).contains("Incompatible Storage Types");
    }
    // same storage type should be fine
    createAndLoadDataset(PersistentStorageEngine.FRS, DATASET_NAME2);
    verifyDatasets(DATASET_NAME1, DATASET_NAME2);
  }

  private void runStorageTypeTest(PersistentStorageType storageType) throws Exception {
    createAndLoadDataset(storageType, DATASET_NAME1, DATASET_NAME2 );

    CLUSTER.getClusterControl().terminateAllServers();
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForActive();

    verifyDatasets(DATASET_NAME1, DATASET_NAME2);
  }

  private void createAndLoadDataset(PersistentStorageType storageType, String... datasetNames) throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE, storageType)
          .durabilityEveryMutation()
          .build();
      for (String datasetName : datasetNames) {
        assertThat(datasetManager.newDataset(datasetName, INT, datasetConfiguration)).isTrue();
        try (Dataset<Integer> dataset = datasetManager.getDataset(datasetName, INT)) {
          DatasetWriterReader<Integer> writerReader = dataset.writerReader();
          for (int i = 0; i < NUM_ITERATIONS; i++) {
            writerReader.add(i, defineString("name").newCell("John" + " i"),
                defineInt("salary").newCell(10_000 * i));
          }
        }
      }
    }
  }

  private void verifyDatasets(String ...datasetNames) throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      for (String datasetName : datasetNames) {
        try (Dataset<Integer> dataset = datasetManager.getDataset(datasetName, Type.INT)) {
          DatasetWriterReader<Integer> writerReader = dataset.writerReader();
          assertThat(writerReader.records().count()).isEqualTo(NUM_ITERATIONS);
        }
      }
    }
  }

  private void silentDestroyDatasets(String ...datasetNames) {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      for (String datasetName : datasetNames) {
        try {
          datasetManager.destroyDataset(datasetName);
        } catch (Exception ignored) {
        }
      }
    } catch (Exception ignored) {
    }
  }
}