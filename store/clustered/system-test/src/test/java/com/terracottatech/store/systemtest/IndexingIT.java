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
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.RecordStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.indexing.IndexSettings.BTREE;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.STREET_ADDRESS;
import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexingIT extends CRUDTests {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  // In future, if more index types are supported, they can be added to this list to get all the below test enabled for them
  private final List<IndexSettings> indexTypes = Arrays.asList(BTREE);

  @Before
  public void initializeTest() throws Exception {
  }

  @Test
  public void createIndexAllCombinationsOnEmptyDataset() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {

      ArrayList<Operation<Index<?>>> indexingOperations = new ArrayList<>();
      ArrayList<IndexKeyAndType> expectedIndexKeyAndTypeList = new ArrayList<>();
      for (IndexSettings indexType : indexTypes) {
        for (CellDefinition<?> cellDefinition : Employee.employeeCellDefinitions) {
          indexingOperations.add(createIndex(dataset, indexType, cellDefinition));
          expectedIndexKeyAndTypeList.add(new IndexKeyAndType(cellDefinition, indexType));
        }
      }

      // Wait for index creation and also gather the live indexes on the dataset
      for (Operation<Index<?>> indexingOperation : indexingOperations) {
        indexingOperation.get(10, SECONDS);
      }

      /*
       * Index definitions aren't guaranteed to be created until the indexing operation is complete.
       */
      List<IndexKeyAndType> allIndexKeyAndTypeList = dataset.getIndexing().getAllIndexes().stream()
          .map(IndexKeyAndType::new)
          .collect(toList());
      assertThat(allIndexKeyAndTypeList, containsInAnyOrder(expectedIndexKeyAndTypeList.toArray()));

      List<IndexKeyAndType> liveIndexKeyAndTypeList = dataset.getIndexing().getLiveIndexes().stream()
          .map(IndexKeyAndType::new)
          .collect(toList());
      assertThat(liveIndexKeyAndTypeList, containsInAnyOrder(expectedIndexKeyAndTypeList.toArray()));
    }
  }

  @Test
  public void createIndexAllCombinationsOnNonEmptyDataset() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // Read Employee.csv and initialize List of Employee i.e. employeeList
      initializeEmployeeList();

      // Fill in data to employee Dataset using the employeeList
      employeeList.stream()
          .forEach(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));

      // For every celldefinition, gather the set of values for it in the dataset before creating indexes
      // Same would be done after indexes are created, and then the maps would be compared for equality
      Map<CellDefinition<?>, Set<Object>> cellValueMapBeforeIndexing = new HashMap<>();
      for (CellDefinition<?> cellDefinition : Employee.employeeCellDefinitions) {
        cellValueMapBeforeIndexing.put(cellDefinition,
                employeeReader.records()
                        .filter(cellDefinition.exists())
                        .map(cellDefinition.value())
                        .collect(toSet()));
      }

      // create all the indexes
      ArrayList<Operation<Index<?>>> indexingOperations = new ArrayList<>();
      ArrayList<IndexKeyAndType> expectedIndexKeyAndTypeList = new ArrayList<>();
      for (IndexSettings indexType : indexTypes) {
        for (CellDefinition<?> cellDefinition : Employee.employeeCellDefinitions) {
          indexingOperations.add(createIndex(dataset, indexType, cellDefinition));
          expectedIndexKeyAndTypeList.add(new IndexKeyAndType(cellDefinition, indexType));
        }
      }

      // Wait for index creation and also gather the live indexes on the dataset
      for (Operation<Index<?>> indexingOperation : indexingOperations) {
        indexingOperation.get(10, SECONDS);
      }

      /*
       * Index definitions aren't guaranteed to be created until the indexing operation is complete.
       */
      List<IndexKeyAndType> allIndexKeyAndTypeList = dataset.getIndexing().getAllIndexes().stream()
          .map(IndexKeyAndType::new)
          .collect(toList());
      assertThat(allIndexKeyAndTypeList, containsInAnyOrder(expectedIndexKeyAndTypeList.toArray()));

      List<IndexKeyAndType> liveIndexKeyAndTypeList = dataset.getIndexing().getLiveIndexes().stream()
          .map(IndexKeyAndType::new)
          .collect(toList());
      assertThat(liveIndexKeyAndTypeList, containsInAnyOrder(expectedIndexKeyAndTypeList.toArray()));

      Map<CellDefinition<?>, Set<Object>> cellValueMapAfterIndexing = new HashMap<>();
      for (CellDefinition<?> cellDefinition : Employee.employeeCellDefinitions) {
        cellValueMapAfterIndexing.put(cellDefinition,
                employeeReader.records()
                        .filter(cellDefinition.exists())
                        .map(cellDefinition.value())
                        .collect(toSet()));
      }

      assertThat(cellValueMapAfterIndexing, equalTo(cellValueMapBeforeIndexing));
    }
  }

  @Test
  public void duplicateIndexingTest() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      dataset.getIndexing().createIndex(NAME, BTREE).get();
      exception.expectCause(instanceOf(IllegalArgumentException.class));
      dataset.getIndexing().createIndex(NAME, BTREE).get();
    }
  }

  @Test
  public void indexOnDestroyedDatasetTest() throws Exception {
    DatasetManager datasetManager = datasetManagerType.getDatasetManager();
    datasetManager.newDataset("student", INT, datasetManagerType.getDatasetConfiguration());
    Dataset<Integer> studentDataset = datasetManager.getDataset("student", INT);
    studentDataset.close();

    // Index creation on closed dataset should throw exception
    try {
      studentDataset.getIndexing().createIndex(NAME, BTREE);
      fail("StoreRuntimeException was expected to be thrown here");
    } catch (StoreRuntimeException e) {
    } finally {
      datasetManager.destroyDataset("student");
    }
  }

  @Test
  public void datasetCreationTimeIndexingTest() throws Exception {

    DatasetManager datasetManager = datasetManagerType.getDatasetManager();
    DatasetConfiguration datasetConfiguration = datasetManagerType.getDatasetConfiguration();
    String offHeapResource = datasetConfiguration.getOffheapResource();
    String diskResource = datasetConfiguration.getDiskResource().orElse(null);

    DatasetConfigurationBuilder studentDatasetConfigurationBuilder = datasetManager.datasetConfiguration()
        .offheap(offHeapResource);
    studentDatasetConfigurationBuilder = ((AdvancedDatasetConfigurationBuilder) studentDatasetConfigurationBuilder).concurrencyHint(2);

    if (diskResource != null) {
      studentDatasetConfigurationBuilder.disk(diskResource);
    }

    ArrayList<IndexKeyAndType> expectedIndexKeyAndTypeList = new ArrayList<>();
    for (IndexSettings indexType : indexTypes) {
      for (CellDefinition<?> cellDefinition : Employee.employeeCellDefinitions) {
        studentDatasetConfigurationBuilder = studentDatasetConfigurationBuilder.index(cellDefinition, indexType);
        expectedIndexKeyAndTypeList.add(new IndexKeyAndType(cellDefinition, indexType));
      }
    }

    Dataset<Integer> studentDataset;
    assertTrue(datasetManager.newDataset("student", INT, studentDatasetConfigurationBuilder.build()));
    assertNotNull(studentDataset = datasetManager.getDataset("student", INT));

    List<IndexKeyAndType> allIndexKeyAndTypeList = studentDataset.getIndexing().getAllIndexes().stream()
        .map(IndexKeyAndType::new)
        .collect(toList());

    assertThat(allIndexKeyAndTypeList, containsInAnyOrder(expectedIndexKeyAndTypeList.toArray()));

    // Now, check by insert some data
    DatasetWriterReader<Integer> studentDatasetWriterReader = studentDataset
        .writerReader();

    CellSet cellSet[] = {CellSet.of(NAME.newCell("Brad Pitt"), STREET_ADDRESS.newCell("Troy")),
        CellSet.of(NAME.newCell("Matt Damon"), STREET_ADDRESS.newCell("Bourne Identity"))};

    studentDatasetWriterReader.add(generateUniqueEmpID(), cellSet[0]);
    studentDatasetWriterReader.add(generateUniqueEmpID(), cellSet[1]);

    List<String> namesUsingStreamWithNameMap = studentDatasetWriterReader.records()
        .map(NAME.value())
        .map(Optional::get)
        .collect(toList());

    List<String> namesAfterRetrievingAllRecords = studentDatasetWriterReader.records()
        .map(Employee::new)
        .collect(toList())
        .stream()
        .map(Employee::getName)
        .collect(toList());

    assertThat(namesUsingStreamWithNameMap, containsInAnyOrder(namesAfterRetrievingAllRecords.toArray()));
    studentDataset.close();
    datasetManager.destroyDataset("student");
  }

  @Test
  public void destroyIndexTest() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      Operation<Index<String>> indexingOperation = dataset.getIndexing().createIndex(NAME, BTREE);
      Index<?> index = indexingOperation.get(10, SECONDS);
      assertTrue(dataset.getIndexing().getAllIndexes().size() == 1);
      assertTrue(dataset.getIndexing().getLiveIndexes().size() == 1);

      dataset.getIndexing().destroyIndex(index);
      assertTrue(dataset.getIndexing().getAllIndexes().size() == 0);
      assertTrue(dataset.getIndexing().getLiveIndexes().size() == 0);

      // destroying same index again should throw exception
      try {
        dataset.getIndexing().destroyIndex(index);
        fail("Exception should have been thrown");
      } catch (StoreIndexNotFoundException e) {
        // Expected
      }

      // recreation of same index should be possible
      indexingOperation = dataset.getIndexing().createIndex(NAME, BTREE);
      index = indexingOperation.get(10, SECONDS);

      assertTrue(dataset.getIndexing().getAllIndexes().size() == 1);
      assertTrue(dataset.getIndexing().getLiveIndexes().size() == 1);
      assertThat(new IndexKeyAndType(index), equalTo(new IndexKeyAndType(dataset.getIndexing().getAllIndexes().stream().findAny().get())));
      assertThat(new IndexKeyAndType(index), equalTo(new IndexKeyAndType(dataset.getIndexing().getLiveIndexes().stream().findAny().get())));
    }
  }

  @SuppressWarnings("unchecked")
  private <K extends Comparable<K>> Operation<Index<?>> createIndex(
          Dataset<Integer> dataset, IndexSettings indexType, CellDefinition<?> cellDefinition) {
    CellDefinition<K> comparableDefinition = (CellDefinition<K>) cellDefinition;
    Operation<Index<K>> index = dataset.getIndexing().createIndex(comparableDefinition, indexType);
    return (Operation<Index<?>>) (Operation) index;
  }

  static class IndexKeyAndType {
    private final CellDefinition<?> key;
    private final IndexSettings type;

    public IndexKeyAndType(CellDefinition<?> key, IndexSettings type) {
      this.key = key;
      this.type = type;
    }

    public IndexKeyAndType(Index<?> index) {
      this.key = index.on();
      this.type = index.definition();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IndexKeyAndType)) {
        return false;
      }

      IndexKeyAndType that = (IndexKeyAndType) o;
      return key.equals(that.key) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + type.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "IndexKeyAndType{" +
          "key=" + key +
          ", type=" + type +
          '}';
    }
  }
}
