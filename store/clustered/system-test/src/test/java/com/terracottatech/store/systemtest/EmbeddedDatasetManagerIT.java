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

import com.terracottatech.sovereign.common.utils.FileUtils;
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.Employees;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import com.terracottatech.store.systemtest.IndexingIT.IndexKeyAndType;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.common.test.Employee.CELL_NUMBER;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employees.addNEmployeeRecords;
import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EmbeddedDatasetManagerIT {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test(expected = IllegalStateException.class)
  public void embeddedDatasetManagerCreationWithoutAnyResourceTest() throws Exception {
    DatasetManager.embedded().build();
  }

  @Test
  public void embeddedDatasetCreationOnNonExistingOffheapTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .build()) {

      // Dataset creation using DatasetManager with non-exiting offheap resource should throw exception
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("OffHeap2").build();
      exception.expect(IllegalArgumentException.class);
      datasetManager.newDataset("student", INT, datasetConfiguration);
    }
  }

  @Test
  public void testCreateDatasetTooSmall() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("offheap", 1, MemoryUnit.B)
        .build()) {
      DatasetConfiguration configuration = datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build();
      exception.expect(StoreException.class);
      datasetManager.newDataset("test", Type.STRING, configuration);
    }
  }

  @Test
  public void transientEmbeddedDatasetManagerTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .build()) {

      // Dataset creation on Dataset Manager with only offheap resource should be allowed
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("OffHeap1").build();
      fundamentalDatasetCreationRetrievalChecks(datasetManager, datasetConfiguration);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void diskOnlyEmbeddedDatasetManagerCreationTest() throws Exception {
    // DatasetManager with disk only resource is not allowed
    DatasetManager datasetManager = DatasetManager.embedded()
        .disk("disk1", temporaryFolder.newFolder().toPath(),
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
        .build();
  }

  @Test
  public void diskOnlyEmbeddedDatasetCreationTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", temporaryFolder.newFolder().toPath(),
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
        .build()) {

      // Dataset creation with only disk resource should not be allowed
      exception.expect(StoreRuntimeException.class);
      DatasetConfiguration datasetConfiguration1 = datasetManager.datasetConfiguration().disk("disk1").build();
    }
  }

  @Test
  public void persistentEmbeddedDatasetCreationOnNonExistingDiskTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", temporaryFolder.newFolder().toPath(),
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
        .build()) {

      // Dataset creation using DatasetManager with non-exiting disk resource should throw exception
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("OffHeap1").disk("disk2").build();
      exception.expect(IllegalArgumentException.class);
      datasetManager.newDataset("student", INT, datasetConfiguration);
    }
  }

  @Test
  public void persistentEmbeddedDatasetManagerTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", temporaryFolder.newFolder().toPath(),
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
        .build()) {

      // Dataset creation on Dataset Manager with offheap and disk resource work
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("OffHeap1").disk("disk1").build();
      fundamentalDatasetCreationRetrievalChecks(datasetManager, datasetConfiguration);
    }
  }

  @Test
  public void diskUpdateAsPartOfAddToDatasetTest() throws Exception {
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.HYBRID,
            EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
        .build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap("OffHeap1")
          .disk("disk1")
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();

        // insert and initial record to enable creation of data file(s) in the file system
        datasetWriterReader.add(generateUniqueEmpID(), NAME.newCell("John Paul"));

        // Check that the add operation writes onto the file system
        Map<Path, FileTime> pathFileTimeMapBefore = getFileToLastModifiedTimeMapInDirectory(diskResourcePath);

        // In Linux, the granularity of LastModifiedTime in only in seconds, so waiting for at least a second to get the time difference
        Thread.sleep(1_000);

        datasetWriterReader.add(generateUniqueEmpID(), NAME.newCell("John Chen"));
        Map<Path, FileTime> pathFileTimeMapAfter = getFileToLastModifiedTimeMapInDirectory(diskResourcePath);
        assertThat(pathFileTimeMapBefore, not(equalTo(pathFileTimeMapAfter)));
      }
    }
  }

  @Test
  public void diskUpdateAsPartOfCreateDatasetTest() throws Exception {
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
        .build()) {


      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("OffHeap1").disk("disk1").build();

      // Create the first dataset so that intial files are all created
      assertThat(datasetManager.newDataset("student", INT, datasetConfiguration), is(true));

      // Check that a dataset create operation writes onto the file system
      Map<Path, FileTime> pathFileTimeMapBefore = getFileToLastModifiedTimeMapInDirectory(diskResourcePath);

      // In Linux, the granularity of LastModifiedTime in only in seconds, so waiting for atleast a second to get the time difference
      Thread.sleep(1_000);

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      Map<Path, FileTime> pathFileTimeMapAfter = getFileToLastModifiedTimeMapInDirectory(diskResourcePath);
      assertThat(pathFileTimeMapBefore, not(equalTo(pathFileTimeMapAfter)));
    }
  }

  @SuppressWarnings("try")
  @Test
  public void newFileModeTest() throws Exception {
    // DatasetManager creation with disk resource with FileMode=NEW should be successful
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.NEW)
        .build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("OffHeap1").disk("disk1").build();

      // create a dataset with this datasetManager and then close both the dataset and datasetManager
      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
    }

    // DatasetManager creation with disk resource with FileMode=NEW in already used folder should fail with Exception
    try {
      try (DatasetManager datasetManager2 = DatasetManager.embedded()
          .offheap("OffHeap1", 100, MemoryUnit.MB)
          .disk("disk1", diskResourcePath,
              EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
              EmbeddedDatasetManagerBuilder.FileMode.NEW)
          .build()) {
      }
      fail("StoreException was expected to be thrown here");
    } catch (StoreException e) {
    }

    // But after deleting the exiting directory, it should work
    // Attempt to permit Windows to release memory-mapped FRS datasets
    for (int i = 0; i < 10; i++) {
      System.gc();
      System.runFinalization();
      Thread.yield();
    }
    // Windows has an issue with simply deleting the directory (recursively) -- the deletion "completes"
    // successfully but the directory remains extant until the bookkeeping is all done.  Renaming the
    // directory first gets around that delay.
    Path target = diskResourcePath.resolveSibling(diskResourcePath.getName(diskResourcePath.getNameCount() - 1) + "-DELETED");
    Files.move(diskResourcePath, target);
    FileUtils.deleteRecursively(target.toFile());

    try (DatasetManager datasetManager3 = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.NEW)
        .build()) {
      DatasetConfiguration datasetConfiguration3 = datasetManager3.datasetConfiguration().offheap("OffHeap1").disk("disk1").build();
      assertThat(datasetManager3.newDataset("employee", INT, datasetConfiguration3), is(true));
    }
  }

  @Test
  public void reopenFileModeTest() throws Exception {
    // Disk resource with FileMode=Reopen with newly created directory should fail with exception
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    try {
      DatasetManager datasetManager = DatasetManager.embedded()
          .offheap("OffHeap1", 100, MemoryUnit.MB)
          .disk("disk1", diskResourcePath,
              EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
              EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
          .build();
      fail("Expected a StoreException to be thrown");
    } catch (StoreException e) {
    }

    // Create a new datasetManager with Filemode=NEW and then close it to reopen again
    // Also add a record which should be present after reopening
    Employee employeeBeforeClosing;
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.NEW)
        .build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap("OffHeap1")
          .disk("disk1")
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();
        int empID = generateUniqueEmpID();
        datasetWriterReader.add(empID, NAME.newCell("Usain Bolt"));
        employeeBeforeClosing = new Employee(datasetWriterReader.get(empID).get());
      }
    }

    // Reopen using the same filePath to find the employee dataset and record in place
    try (DatasetManager reopenedDatasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
        .build()) {
      Set<Employee> employeeSet = reopenedDatasetManager.getDataset("employee", INT)
          .reader()
          .records()
          .map(Employee::new)
          .collect(toSet());

      assertTrue(employeeSet.size() == 1);
      assertTrue(employeeSet.contains(employeeBeforeClosing));
    }
  }

  @Test
  public void overwriteFileModeTest() throws Exception {
    // Create a new datasetManager with Filemode=NEW and then close it to reopen with overwrite Filemode
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.NEW)
        .build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap("OffHeap1")
          .disk("disk1")
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();
        int empID = generateUniqueEmpID();
        datasetWriterReader.add(empID, NAME.newCell("Michael Phelps"));
      }
    }

    // Now overwrite it, no data should be found in it that was written earlier
    try (DatasetManager overwrittenDatasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
        .build()) {
      try {
        overwrittenDatasetManager.getDataset("employee", INT);
        fail("Expected a StoreException to be thrown");
      } catch (StoreException e) {
      }

      // Employee dataset can be created again on it
      DatasetConfiguration overwrittenDatasetConfiguration = overwrittenDatasetManager.datasetConfiguration().offheap("OffHeap1").disk("disk1").build();
      assertThat(overwrittenDatasetManager.newDataset("employee", INT, overwrittenDatasetConfiguration), is(true));
    }
  }

  @Test
  public void reopenOrNewFileModeTest() throws Exception {
    // Create a new datasetManager with Filemode=REOPEN_OR_NEW and then close it to reopen with the same Filemode
    Employee employeeBeforeClose;
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap("OffHeap1")
          .disk("disk1")
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();
        int empID = generateUniqueEmpID();
        datasetWriterReader.add(empID, NAME.newCell("Roger Federer"));
        employeeBeforeClose = new Employee(datasetWriterReader.get(empID).get());
      }
    }

    // New DatasetManager instance on same disk resource with REOPEN_OR_NEW will find the data intact
    try (DatasetManager reopenedDatasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .build()) {
      Set<Employee> employeeSet = reopenedDatasetManager.getDataset("employee", INT)
          .reader()
          .records()
          .map(Employee::new)
          .collect(toSet());

      assertTrue(employeeSet.size() == 1);
      assertTrue(employeeSet.contains(employeeBeforeClose));
    }
  }

  @Test
  public void indexesAfterReopenTest() throws Exception {
    // Create a new datasetManager with Filemode=REOPEN_OR_NEW and then close it to reopen with the same Filemode
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    String name = "Roger Federer";
    List<IndexKeyAndType> beforeReopenIndexKeyAndTypeList;
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap("OffHeap1")
          .disk("disk1")
          .index(NAME, IndexSettings.BTREE)
          .index(CELL_NUMBER, IndexSettings.BTREE)
          .build();

      datasetManager.newDataset("employee", INT, datasetConfiguration);

      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();
        int empID = generateUniqueEmpID();
        datasetWriterReader.add(empID, NAME.newCell(name));
        beforeReopenIndexKeyAndTypeList = dataset.getIndexing().getAllIndexes().stream()
            .map(IndexKeyAndType::new)
            .collect(toList());
      }
    }

    // New DatasetManager instance on same disk resource with REOPEN_OR_NEW will find the indexes intact
    try (DatasetManager reopenedDatasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 100, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .build()) {
      try (Dataset<Integer> reopenedDataset = reopenedDatasetManager.getDataset("employee", INT)) {
        DatasetReader<Integer> datasetReader = reopenedDataset.reader();

        assertTrue(datasetReader.records().filter(NAME.valueOrFail().is(name)).count() == 1);
        List<IndexKeyAndType> afterReopenIndexKeyAndTypeList = reopenedDataset.getIndexing().getAllIndexes().stream()
            .map(IndexKeyAndType::new)
            .collect(toList());
        assertThat(afterReopenIndexKeyAndTypeList, containsInAnyOrder(beforeReopenIndexKeyAndTypeList.toArray()));
      }
    }
  }

  @Test
  public void hybridPersistentDatasetManagetTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", 5, MemoryUnit.MB)
        .disk("disk1", temporaryFolder.newFolder().toPath(),
            EmbeddedDatasetManagerBuilder.PersistenceMode.HYBRID,
            EmbeddedDatasetManagerBuilder.FileMode.NEW)
        .build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("OffHeap1").disk("disk1").build();

      // Basic checks on Hybrid Dataset manager
      fundamentalDatasetCreationRetrievalChecks(datasetManager, datasetConfiguration);
    }
  }

  @Test
  public void hybridPersistentModeTest() throws Exception {
    int numEmployeesToInduceOutOfMemory = 25000;
    int additionalEmployeesHybridCanTake = 4000;
    int memorySizeInMB = 8;

    CellSet cellSet = CellSet.of(NAME.newCell("Muhammad Ali"),
        CELL_NUMBER.newCell(911L),
        Employee.STREET_ADDRESS.newCell("Madison Square Garden"),
        Employee.CITY_ADDRESS.newCell("New York"),
        COUNTRY_ADDRESS.newCell("USA"));

    // Create a new datasetManager with INMEMORY PersistentMode and one offheap and a disk resource
    Path diskResourcePath = temporaryFolder.newFolder().toPath();
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("OffHeap1", memorySizeInMB, MemoryUnit.MB)
        .disk("disk1", diskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .build()) {
      DatasetConfigurationBuilder configurationBuilder = datasetManager.datasetConfiguration()
          .offheap("OffHeap1")
          .disk("disk1");
      configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
      DatasetConfiguration datasetConfiguration = configurationBuilder.build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();

        // Hybrid mode should be able to accomodate more data as compared to INMEMORY persistence mode DatasetManager
        // with same amount of Offheap memory
        try {
          addNEmployeeRecords(datasetWriterReader, cellSet, numEmployeesToInduceOutOfMemory);
          fail("Increase numEmployeesToInduceOutOfMemory if it fails here");
        } catch (SovereignExtinctionException e) {
        }
      }
    }

    // Now Hybrid mode datasetManager will be able to accommodate much more rows
    Path hybridDiskResourcePath = temporaryFolder.newFolder().toPath();
    try (DatasetManager hybridDatasetManager = DatasetManager.embedded()
        .offheap("OffHeap2", memorySizeInMB, MemoryUnit.MB)
        .disk("disk2", hybridDiskResourcePath,
            EmbeddedDatasetManagerBuilder.PersistenceMode.HYBRID,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .build()) {
      DatasetConfigurationBuilder configurationBuilder = hybridDatasetManager.datasetConfiguration().offheap("OffHeap2").disk("disk2");
      configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
      DatasetConfiguration hybridDatasetConfiguration = configurationBuilder.build();

      assertThat(hybridDatasetManager.newDataset("employee", INT, hybridDatasetConfiguration), is(true));
      try (Dataset<Integer> hybridDataset = hybridDatasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> hybridDatasetWriterReader = hybridDataset.writerReader();

        try {
          addNEmployeeRecords(hybridDatasetWriterReader, cellSet, numEmployeesToInduceOutOfMemory + additionalEmployeesHybridCanTake);
        } catch (SovereignExtinctionException e) {
          fail("Try to decrease numEmployeesToInduceOutOfMemory or additionalEmployeesHybridCanTake if it fails here, do not make it zero");
        }

        assertEquals(hybridDatasetWriterReader.records().count(), numEmployeesToInduceOutOfMemory + additionalEmployeesHybridCanTake);

        Set<CellSet> setOfCellSets = hybridDatasetWriterReader.records()
            .map(Employee::new)
            .map(Employee::getCellSet)
            .collect(toSet());

        // Cellset for each record is same
        assertTrue(setOfCellSets.size() == 1);
        assertTrue(setOfCellSets.contains(cellSet));
      }
    }
  }

  public static void fundamentalDatasetCreationRetrievalChecks(DatasetManager datasetManager,
                                                               DatasetConfiguration datasetConfiguration) throws Exception {
    assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
    Dataset<Integer> createdDataset;
    assertThat(createdDataset = datasetManager.getDataset("employee", INT), notNullValue());
    assertTrue(createdDataset.writerReader()
        .add(generateUniqueEmpID(), NAME.newCell("Ramesh"),
            Employee.CITY_ADDRESS.newCell("Bengaluru")));

    assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(false));

    // getDataset should get us the same dataset created earlier
    Dataset<Integer> retrivedDataset;
    assertNotNull(retrivedDataset = datasetManager.getDataset("employee", INT));
    assertTrue(new Employees(createdDataset).equals(retrivedDataset));

    // Exception should be thrown if dataset key does not match
    try {
      datasetManager.getDataset("employee", STRING);
      fail("Expected a StoreException to be thrown");
    } catch (DatasetKeyTypeMismatchException e) {
    }

    // Exception should be thrown if trying to retrieve a non-existing dataset
    try {
      datasetManager.getDataset("student", STRING);
      fail("Expected a StoreException to be thrown");
    } catch (DatasetMissingException e) {
    }

    // Datasets with same name but different keys not allowed with same DatasetManager
    try {
      datasetManager.newDataset("employee", STRING, datasetConfiguration);
      fail("Expected a DatasetKeyTypeMismatchException to be thrown");
    } catch (DatasetKeyTypeMismatchException e) {
    }

    // Destroy on in-Use Dataset should throw Exception
    try {
      datasetManager.destroyDataset("employee");
      fail("Expected a StoreException to be thrown");
    } catch (StoreException e) {
    }

    createdDataset.close();
    // Again, calling close on one instance on dataset is not sufficient, it should be done for all the instances
    try {
      datasetManager.destroyDataset("employee");
      fail("Expected a StoreException to be thrown");
    } catch (StoreException e) {
    }

    retrivedDataset.close();
    // Finally the dataset can now be destroyed
    assertTrue(datasetManager.destroyDataset("employee"));
    try {
      datasetManager.getDataset("employee", INT);
      fail("Expected a StoreException to be thrown");
    } catch (DatasetMissingException e) {
    }

    // False should be returned if destroying a non-Existing dataset should throw exception
    assertFalse(datasetManager.destroyDataset("employee"));
  }

  private Map<Path, FileTime> getFileToLastModifiedTimeMapInDirectory(Path directoryPath) throws Exception {
    Map<Path, FileTime> fileLastModifiedTimeMap = new HashMap<>();

    try (Stream<Path> pathStream = Files.walk(directoryPath)) {
      pathStream.filter(p -> !Files.isDirectory(p)).forEach(path -> {
        try {
          fileLastModifiedTimeMap.put(path, Files.getLastModifiedTime(path));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    return fileLastModifiedTimeMap;
  }
}
