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

import com.terracottatech.store.CellSet;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.systemtest.IndexingIT.IndexKeyAndType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.terracotta.connection.ConnectionException;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static com.terracottatech.store.systemtest.EmbeddedDatasetManagerIT.fundamentalDatasetCreationRetrievalChecks;
import static com.terracottatech.tool.Diagnostics.getAllThreads;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.collection.IsIn.isOneOf;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusteredDatasetManagerIT extends BaseClusterWithOffheapAndFRSTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void clusteredDatasetManagerNullURITest() throws Exception {
    exception.expect(NullPointerException.class);
    DatasetManager.clustered((URI) null).build();
  }

  @Test
  public void clusteredDatasetManagerNonExistingOffHeapCorrectionRecoveryTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      String datasetName = "student";
      final String OFFHEAP_RESOURCE = "Non Existing OffHeap Resource";
      DatasetConfiguration invalidDatasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE)
          .build();

      try {
        datasetManager.newDataset(datasetName, INT, invalidDatasetConfiguration);
        fail("Dataset creation was expected to throw");
      } catch (StoreException se) {
        // Expected
      }

      DatasetConfiguration validDatasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE)
          .build();
      datasetManager.newDataset(datasetName, INT, validDatasetConfiguration);

      datasetManager.destroyDataset(datasetName);
    }
  }

  @Test
  public void clusteredDatasetManagerIllegalURITest() throws Exception {
    // Trying to connect to anything other than terracotta server should raise exception
    try {
      DatasetManager.clustered(URI.create("http://localhost:8080")).build();
      fail("Should have failed with IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    try {
      DatasetManager.clustered(URI.create("ftp://localhost:21")).build();
      fail("Should have failed with IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void clusteredDatasetManagerNonExitingServerURITest() {
    try {
      DatasetManager.clustered(URI.create("terracotta://example.com:22")).withConnectionTimeout(500, TimeUnit.MILLISECONDS).build();
      fail("Expected StoreException");
    } catch (StoreException e) {
      assertThat(e.getCause(), instanceOf(ConnectionException.class));
      assertThat(e.getCause().getCause(), instanceOf(TimeoutException.class));
    }
  }

  @Test
  public void clusteredDatasetManagerNonExistingOffHeapTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      // Create dataset configuration with non existing Offheap resource at server-side
      final String OFFHEAP_RESOURCE = "Non Existing OffHeap Resource";
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(OFFHEAP_RESOURCE)
          .build();

      exception.expect(StoreException.class);
      datasetManager.newDataset("student", INT, datasetConfiguration);
    }
  }

  @Test
  public void diskOnlyClusteredDatasetManagerTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      exception.expect(IllegalStateException.class);
      datasetManager.datasetConfiguration().disk("cluster-disk-resource").build();
    }
  }

  @Test
  public void clusteredDatasetManagerNonExistingDiskTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk("NonExistingDisk")
          .build();

      exception.expect(StoreException.class);
      datasetManager.newDataset("employee", INT, datasetConfiguration);
    }
  }

  @Test
  public void clusteredDatasetManagerOffheapOnlyTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .build();

      // Basic checks on Clustered Dataset manager
      fundamentalDatasetCreationRetrievalChecks(datasetManager, datasetConfiguration);
    }
  }

  @Test
  public void testCreateDatasetTooSmall() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration configuration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_TINY_OFFHEAP_RESOURCE)
          .build();
      exception.expect(StoreException.class);
      datasetManager.newDataset("test", Type.STRING, configuration);
    }
  }

  @Test
  public void clusteredDatasetManagerListDatasets() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .build();
      assertThat(datasetManager.listDatasets(), is(Collections.emptyMap()));

      datasetManager.newDataset("employee", INT, datasetConfiguration);
      assertThat(datasetManager.listDatasets().size(), is(1));
      assertThat(datasetManager.listDatasets().get("employee"), is(INT));

      datasetManager.newDataset("item", STRING, datasetConfiguration);
      assertThat(datasetManager.listDatasets().size(), is(2));
      assertThat(datasetManager.listDatasets().get("employee"), is(INT));
      assertThat(datasetManager.listDatasets().get("item"), is(STRING));

      assertThat(datasetManager.destroyDataset("employee"), is(true));
      assertThat(datasetManager.listDatasets().size(), is(1));
      assertThat(datasetManager.listDatasets().get("item"), is(STRING));

      assertThat(datasetManager.destroyDataset("item"), is(true));
      assertThat(datasetManager.listDatasets(), is(Collections.emptyMap()));
    }
  }

  @Test
  public void clusteredDatasetManagerWithDiskTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk("cluster-disk-resource")
          .build();

      // Basic checks on Clustered Dataset manager
      fundamentalDatasetCreationRetrievalChecks(datasetManager, datasetConfiguration);
    }
  }

  @Test
  public void offheapNotTiedToDiskAfterDatasetDestroy() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk("cluster-disk-resource")
          .build();

      String datasetName = "test";
      datasetManager.newDataset(datasetName, INT, datasetConfiguration);
      datasetManager.destroyDataset(datasetName);

      datasetManager.newDataset(datasetName, INT, datasetConfiguration);
      datasetManager.destroyDataset(datasetName);

      DatasetConfiguration anotherDatasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(SECONDARY_OFFHEAP_RESOURCE)
          .disk("cluster-disk-resource")
          .build();

      datasetManager.newDataset(datasetName, INT, anotherDatasetConfiguration);
      datasetManager.destroyDataset(datasetName);
    }
  }

  @Test
  public void clusteredDatasetManagerDataAfterRebootTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();

    int empID = generateUniqueEmpID();
    CellSet cellSet = CellSet.of(Employee.NAME.newCell("Amitabh Bachchan"), Employee.CITY_ADDRESS.newCell("Mumbai"));

    DatasetConfiguration datasetConfiguration;

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk("cluster-disk-resource")
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();
        assertTrue(datasetWriterReader.add(empID, cellSet));
      }
    }

    CLUSTER.getClusterControl().terminateAllServers();
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      try (Dataset<Integer> retrievedDataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = retrievedDataset.writerReader();
        assertTrue(datasetWriterReader.get(empID).isPresent());
        assertThat(new Employee(datasetWriterReader.get(empID).get()).getCellSet(), equalTo(cellSet));
      }

      datasetManager.destroyDataset("employee");
    }
  }

  @Test
  public void clusteredDatasetManagerIndexesAfterRebootTest() throws Exception {
    CLUSTER.getClusterControl().waitForActive();

    int empID = generateUniqueEmpID();
    String name = "Nelson Mandela";
    CellSet cellSet = CellSet.of(Employee.NAME.newCell(name), Employee.CITY_ADDRESS.newCell("Johannesburg"));

    DatasetConfiguration datasetConfiguration;
    List<IndexKeyAndType> beforeRebootIndexKeyAndTypeList;

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk("cluster-disk-resource")
          .index(Employee.NAME, IndexSettings.BTREE)
          .index(Employee.CELL_NUMBER, IndexSettings.BTREE)
          .build();
      datasetManager.newDataset("employee", INT, datasetConfiguration);

      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();
        assertTrue(datasetWriterReader.add(empID, cellSet));
        beforeRebootIndexKeyAndTypeList = dataset.getIndexing().getAllIndexes().stream()
            .map(IndexKeyAndType::new)
            .collect(toList());
      }
    }

    CLUSTER.getClusterControl().terminateAllServers();
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      try (Dataset<Integer> retrievedDataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = retrievedDataset.writerReader();
        assertTrue(datasetWriterReader.records().filter(Employee.NAME.valueOrFail().is(name)).count() == 1);
        List<IndexKeyAndType> afterRebootIndexKeyAndTypeList = retrievedDataset.getIndexing().getAllIndexes().stream()
            .map(IndexKeyAndType::new)
            .collect(toList());
        assertThat(afterRebootIndexKeyAndTypeList, containsInAnyOrder(beforeRebootIndexKeyAndTypeList.toArray()));
      }

      datasetManager.destroyDataset("employee");
    }
  }

  @SuppressWarnings("try")
  @Test
  public void emptyClusteredDatasetManagerShutdownWhenOffline() throws Exception {
    try {
      CLUSTER.getClusterControl().waitForActive();
      Thread[] initialThreads = getAllThreads();
      try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
        CLUSTER.getClusterControl().terminateAllServers();
      }
      assertThatEventually(() -> {
        //DefinitionInterner Cleaner Thread
        System.gc();
        return asList(getAllThreads());
      }, everyItem(either(isOneOf(initialThreads)).or(hasProperty("name", containsString("process reaper"))))).within(Duration.ofSeconds(10));
    } finally {
      CLUSTER.getClusterControl().startAllServers();
      CLUSTER.getClusterControl().waitForActive();
    }
  }

  @Test
  public void populatedClusteredDatasetManagerShutdownWhenOffline() throws Exception {
    try {
      CLUSTER.getClusterControl().waitForActive();
      Thread[] initialThreads = getAllThreads();
      try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
        DatasetConfiguration config = datasetManager.datasetConfiguration()
                .offheap(CLUSTER_OFFHEAP_RESOURCE)
                .disk("cluster-disk-resource").build();
        datasetManager.newDataset("emptyClusteredDatasetManagerShutdownWhenOffline", Type.INT, config);
        Dataset<Integer> dataset = datasetManager.getDataset("emptyClusteredDatasetManagerShutdownWhenOffline", Type.INT);
        DatasetWriterReader<Integer> writer = dataset.writerReader();
        range(0, 100).forEach(i -> writer.add(i));
        CLUSTER.getClusterControl().terminateAllServers();
      }
      assertThatEventually(() -> {
        //DefinitionInterner Cleaner Thread
        System.gc();
        return asList(getAllThreads());
      }, everyItem(either(isOneOf(initialThreads)).or(hasProperty("name", containsString("process reaper"))))).within(Duration.ofSeconds(10));
    } finally {
      CLUSTER.getClusterControl().startAllServers();
      CLUSTER.getClusterControl().waitForActive();
      try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
        datasetManager.destroyDataset("populatedClusteredDatasetManagerShutdownWhenOffline");
      }
    }
  }

  @Test
  public void activeClusteredDatasetManagerShutdownWhenOffline() throws Exception {
    try {
      Thread t;
      CLUSTER.getClusterControl().waitForActive();
      Thread[] initialThreads = getAllThreads();
      try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
        DatasetConfiguration config = datasetManager.datasetConfiguration()
                .offheap(CLUSTER_OFFHEAP_RESOURCE)
                .disk("cluster-disk-resource").build();
        datasetManager.newDataset("activeClusteredDatasetManagerShutdownWhenOffline", Type.INT, config);
        Dataset<Integer> dataset = datasetManager.getDataset("activeClusteredDatasetManagerShutdownWhenOffline", Type.INT);
        Runnable task = () -> {
          DatasetWriterReader<Integer> writer = dataset.writerReader();
          LongCellDefinition counter = CellDefinition.defineLong("counter");
          writer.add(0);
          while (true) {
            writer.update(0, UpdateOperation.write(counter).longResultOf(counter.longValueOr(0L).increment()));
          }
        };
        t = new Thread(task);
        t.start();
        CLUSTER.getClusterControl().terminateAllServers();
      }
      t.join();
      assertThatEventually(() -> {
        //DefinitionInterner Cleaner Thread
        System.gc();
        return asList(getAllThreads());
      }, everyItem(either(isOneOf(initialThreads)).or(hasProperty("name", containsString("process reaper"))))).within(Duration.ofSeconds(10));
    } finally {
      CLUSTER.getClusterControl().startAllServers();
      CLUSTER.getClusterControl().waitForActive();
      try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
        datasetManager.destroyDataset("activeClusteredDatasetManagerShutdownWhenOffline");
      }
    }
  }

  @Test
  public void activeClusteredDatasetManagerWithListenerShutdownWhenOffline() throws Exception {
    try {
      Thread t;
      CLUSTER.getClusterControl().waitForActive();
      Thread[] initialThreads = getAllThreads();
      try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
        DatasetConfiguration config = datasetManager.datasetConfiguration()
                .offheap(CLUSTER_OFFHEAP_RESOURCE)
                .disk("cluster-disk-resource").build();
        datasetManager.newDataset("activeClusteredDatasetManagerWithListenerShutdownWhenOffline", Type.INT, config);
        Dataset<Integer> dataset = datasetManager.getDataset("activeClusteredDatasetManagerWithListenerShutdownWhenOffline", Type.INT);
        Runnable task = () -> {
          DatasetWriterReader<Integer> writer = dataset.writerReader();
          writer.registerChangeListener(new ChangeListener<Integer>() {
            @Override
            public void onChange(Integer key, ChangeType changeType) {
              System.out.println(changeType + " " + key);
            }

            @Override
            public void missedEvents() {
              System.out.println("MISSED EVENTS");
            }
          });
          LongCellDefinition counter = CellDefinition.defineLong("counter");
          writer.add(0);
          while (true) {
            writer.update(0, UpdateOperation.write(counter).longResultOf(counter.longValueOr(0L).increment()));
          }
        };
        t = new Thread(task);
        t.start();
        CLUSTER.getClusterControl().terminateAllServers();
      }
      t.join();

      assertThatEventually(() -> {
        //DefinitionInterner Cleaner Thread
        System.gc();
        return asList(getAllThreads());
      }, everyItem(either(isOneOf(initialThreads)).or(hasProperty("name", containsString("process reaper"))))).within(Duration.ofSeconds(10));
    } finally {
      CLUSTER.getClusterControl().startAllServers();
      CLUSTER.getClusterControl().waitForActive();
      try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
        datasetManager.destroyDataset("activeClusteredDatasetManagerWithListenerShutdownWhenOffline");
      }
    }
  }
}
