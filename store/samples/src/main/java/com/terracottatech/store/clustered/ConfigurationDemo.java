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
package com.terracottatech.store.clustered;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.Type;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.DatasetManager;

import java.net.URI;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

// The formatting in this class is influenced by the inclusion of fragments from this class in documentation
@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
public class ConfigurationDemo {

  public static void main(String[] args) throws Exception {
    ConfigurationDemo demo =
        new ConfigurationDemo(new URI("terracotta://server1"), "offheap-resource-name", "disk-resource-name");
    demo.fullExample();
    demo.fullExampleWithEngine();
    demo.brokenConfiguration();
    demo.notBrokenConfiguration();
    demo.fluentConfiguration();
    demo.datasetWithIndex();
  }

  private final URI clusterUri;
  private final String offHeapResourceId;
  private final String diskResourceId;

  public ConfigurationDemo(URI clusterUri, String offHeapResourceId, String diskResourceId) {
    this.clusterUri = clusterUri;
    this.offHeapResourceId = offHeapResourceId;
    this.diskResourceId = diskResourceId;
  }

  @SuppressWarnings("try")
  public void fullExample() throws Exception {
    // tag::fullExample[]
    try (DatasetManager datasetManager = DatasetManager.clustered(clusterUri) // <1>
            .build()) {  // <2>
      DatasetConfiguration ordersConfig = datasetManager.datasetConfiguration()  // <3>
              .offheap(offHeapResourceId)  // <4>
              .disk(diskResourceId)  // <5>
              .build();  // <6>
      datasetManager.newDataset("orders", Type.LONG, ordersConfig); // <7>
      try (Dataset<?> orders = datasetManager.getDataset("orders", Type.LONG)) {  // <8>
        // Use the Dataset
      }
    }
    // end::fullExample[]
  }

  @SuppressWarnings("try")
  public void fullExampleWithEngine() throws Exception {
    // tag::fullExampleWithEngine[]
    try (DatasetManager datasetManager = DatasetManager.clustered(clusterUri) // <1>
        .build()) {  // <2>
      DatasetConfiguration employeesConfig = datasetManager.datasetConfiguration()  // <3>
          .offheap(offHeapResourceId)  // <4>
          .disk(diskResourceId, PersistentStorageEngine.FRS)  // <5>
          .build();  // <6>
      datasetManager.newDataset("employees", Type.LONG, employeesConfig); // <7>
      try (Dataset<?> orders = datasetManager.getDataset("employees", Type.LONG)) {  // <8>
        // Use the Dataset
      }
    }
    // end::fullExampleWithEngine[]
  }

  public void brokenConfiguration() throws Exception {
    // tag::brokenConfiguration[]
    ClusteredDatasetManagerBuilder builder = DatasetManager.clustered(clusterUri);
    builder.withConnectionTimeout(30, TimeUnit.SECONDS);
    DatasetManager datasetManager = builder.build();
    // end::brokenConfiguration[]

    datasetManager.close();
  }

  public void notBrokenConfiguration() throws Exception {
    // tag::notBrokenConfiguration[]
    ClusteredDatasetManagerBuilder builder = DatasetManager.clustered(clusterUri);
    ClusteredDatasetManagerBuilder configuredBuilder = builder.withConnectionTimeout(30, TimeUnit.SECONDS);
    DatasetManager datasetManager = configuredBuilder.build();
    // end::notBrokenConfiguration[]

    datasetManager.close();
  }

  public void fluentConfiguration() throws Exception {
    // tag::fluentConfiguration[]
    DatasetManager datasetManager = DatasetManager.clustered(clusterUri)
            .withConnectionTimeout(30, TimeUnit.SECONDS)
            .build();
    // end::fluentConfiguration[]

    datasetManager.close();
  }

  public void datasetWithIndex() throws Exception {

    // tag::creatingIndexes[]
    DatasetManager datasetManager = DatasetManager.clustered(clusterUri).build();

    DatasetConfiguration configuration = datasetManager.datasetConfiguration()
      .offheap(offHeapResourceId)
      .index(CellDefinition.define("orderId", Type.STRING), IndexSettings.BTREE) // <1>
      .build();

    datasetManager.newDataset("indexedOrders", Type.LONG, configuration);
    Dataset<Long> dataset = datasetManager.getDataset("indexedOrders", Type.LONG);

    Indexing indexing = dataset.getIndexing(); // <2>

    Operation<Index<Integer>> indexOperation = indexing.createIndex(
        CellDefinition.define("invoiceId", Type.INT), IndexSettings.BTREE); // <3>

    Index<Integer> invoiceIdIndex = indexOperation.get(); // <4>
    // end::creatingIndexes[]

    // tag::indexStatus[]
    Collection<Index<?>> allIndexes = indexing.getAllIndexes(); // <1>
    Collection<Index<?>> liveIndexes = indexing.getLiveIndexes(); // <2>
    // end::indexStatus[]

    // tag::destroyIndex[]
    indexing.destroyIndex(invoiceIdIndex); // <1>
    // end::destroyIndex[]

  }
}
