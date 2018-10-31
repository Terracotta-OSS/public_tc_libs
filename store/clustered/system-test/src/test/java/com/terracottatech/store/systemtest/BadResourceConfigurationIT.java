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

import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BadResourceConfigurationIT extends BaseClusterWithOffheapAndFRSTest {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void clusteredDatasetManagerNonExistingOffHeapTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
              .offheap("unknown-offheap")
              .build();

      exception.expect(StoreException.class);
      exception.expectMessage("Unknown offheap resource: unknown-offheap");

      datasetManager.newDataset("address", Type.INT, datasetConfiguration);
    }
  }

  @Test
  public void clusteredDatasetManagerNonExistingDiskTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
              .offheap(CLUSTER_OFFHEAP_RESOURCE)
              .disk("unknown-disk")
              .build();

      exception.expect(StoreException.class);
      exception.expectMessage("Unknown disk resource: unknown-disk");

      datasetManager.newDataset("address", Type.INT, datasetConfiguration);
    }
  }
}
