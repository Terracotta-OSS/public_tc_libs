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
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.Employees;
import com.terracottatech.store.internal.InternalDatasetManager;
import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.OptionalAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.statistics.DatasetOutcomes;
import com.terracottatech.store.statistics.DatasetStatistics;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class StatisticsIT extends CRUDTests {

  private static final int KEY_NOT_FOUND_1 = 100_000_000;
  private static final int KEY_NOT_FOUND_2 = 100_000_001;

  private final Logger log = LoggerFactory.getLogger(getClass());

  private long getCount = 0;
  private long createCount = 0;
  private long updateCount = 0;
  private long deleteCount = 0;
  private long streamCount = 0;

  private final List<Employee> employeesInStore = new ArrayList<>();

  @Rule
  public TestWatcher watchman = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      log.error("Counters at time of assertion: " + counters(), e);
      String ids = employeesInStore.stream()
          .map(Employee::getEmpID)
          .map(Object::toString)
          .collect(Collectors.joining(","));
      log.error("Employee ids at beginning of test: " + ids);
    }
  };

  @Before
  public void setup() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      initializeEmployeeList();

      // Insert only the 20 firsts to have some to fetch but also some still to add in my tests
      employeeList.stream()
          .sorted()
          .limit(20)
          .forEachOrdered(employee -> {
            employeesInStore.add(employee);
            employeeWriterReader.add(employee.getEmpID(), employee.getCellSet());
          });

    }

    getCount = 0;
    createCount = 0;
    updateCount = 0;
    deleteCount = 0;
    streamCount = 0;
  }

  @Test
  public void get() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetReader<Integer> employeeReader = dataset.reader();
      Integer empID = id(0);

      // get
      expect(employeeReader.get(Integer.MAX_VALUE)).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeReader.get(empID)).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      // get on
      expect(employeeReader.on(empID).read(Employee.NAME.value())).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      expect(employeeReader.on(Integer.MAX_VALUE).read(Employee.NAME.value())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // get async
      expect(employeeReader.async().get(empID).get()).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      expect(employeeReader.async().get(Integer.MAX_VALUE).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // get async on
      expect(employeeReader.async().on(empID).read(Employee.NAME.value()).get()).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      expect(employeeReader.async().on(Integer.MAX_VALUE).read(Employee.NAME.value()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // read iff
      expect(employeeReader.on(empID).iff(r -> true).read()).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      expect(employeeReader.on(empID).iff(r -> false).read()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeReader.on(empID).iff(r -> true).read(Employee.NAME.value())).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      expect(employeeReader.on(empID).iff(r -> false).read(Employee.NAME.value())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // read iff async
      expect(employeeReader.async().on(empID).iff(r -> true).read().get()).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      expect(employeeReader.async().on(empID).iff(r -> false).read().get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeReader.async().on(empID).iff(r -> true).read(Employee.NAME.value()).get()).isPresent();
      changesOf(datasetStatistics, 1, 0, 0, 0, 0);

      expect(employeeReader.async().on(empID).iff(r -> false).read(Employee.NAME.value()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);
    }
  }

  @Test
  public void create() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      int key = Employees.generateUniqueEmpID();

      // add
      expect(employeeWriterReader.add(key, cellSet(20))).isTrue();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.add(Employees.generateUniqueEmpID(), cellSetArray(20))).isTrue();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.add(key, cellSet(20))).isFalse();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // add on
      expect(employeeWriterReader.on(Employees.generateUniqueEmpID()).add(cellSet(22))).isNotPresent();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.on(Employees.generateUniqueEmpID()).add(cellSetArray(23))).isNotPresent();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.on(key).add(cellSet(20))).isPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // add on function
      expect(employeeWriterReader.on(Employees.generateUniqueEmpID()).add(Employee.NAME.value(), cellSet(22))).isNotPresent();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.on(Employees.generateUniqueEmpID()).add(Employee.NAME.value(), cellSetArray(22))).isNotPresent();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.on(key).add(Employee.NAME.value(), cellSet(20))).isPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // async add
      expect((boolean) employeeWriterReader.async().add(Employees.generateUniqueEmpID(), cellSet(24)).get()).isTrue();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect((boolean) employeeWriterReader.async().add(Employees.generateUniqueEmpID(), cellSetArray(25)).get()).isTrue();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect((boolean) employeeWriterReader.async().add(key, cellSet(20)).get()).isFalse();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // async add on
      expect(employeeWriterReader.async().on(Employees.generateUniqueEmpID()).add(cellSet(26)).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.async().on(Employees.generateUniqueEmpID()).add(cellSetArray(27)).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      expect(employeeWriterReader.async().on(key).add(cellSet(20)).get()).isPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // upsert
      employeeWriterReader.on(Employees.generateUniqueEmpID()).upsert(cellSet(28));
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      employeeWriterReader.on(Employees.generateUniqueEmpID()).upsert(cellSetArray(29));
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      // async upsert
      employeeWriterReader.async().on(Employees.generateUniqueEmpID()).upsert(cellSet(30)).get();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);

      employeeWriterReader.async().on(Employees.generateUniqueEmpID()).upsert(cellSetArray(31)).get();
      changesOf(datasetStatistics, 0, 1, 0, 0, 0);
    }
  }

  @Test
  public void stream() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // stream
      employeeReader.records();
      changesOf(datasetStatistics, 0, 0, 0, 0, 1);

      employeeWriterReader.records();
      changesOf(datasetStatistics, 0, 0, 0, 0, 1);

      // async stream
      employeeReader.async().records();
      changesOf(datasetStatistics, 0, 0, 0, 0, 1);

      employeeWriterReader.async().records();
      changesOf(datasetStatistics, 0, 0, 0, 0, 1);
    }
  }

  @Test
  public void delete() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      // delete
      expect(employeeWriterReader.delete(id(0))).isTrue();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      assertThat(employeeWriterReader.get(id(0))).isNotPresent();

      expect(employeeWriterReader.delete(id(0))).isFalse();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete on
      expect(employeeWriterReader.on(id(1)).delete()).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.on(id(1)).delete()).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete on map
      expect(employeeWriterReader.on(id(8)).delete(Employee.NAME.value())).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.on(id(8)).delete(Employee.NAME.value())).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete async
      expect((boolean) employeeWriterReader.async().delete(id(2)).get()).isTrue();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect((boolean) employeeWriterReader.async().delete(id(2)).get()).isFalse();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete async on
      expect(employeeWriterReader.async().on(id(3)).delete().get()).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.async().on(id(3)).delete().get()).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete async on map
      expect(employeeWriterReader.async().on(id(9)).delete(Employee.NAME.value()).get()).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.async().on(id(9)).delete(Employee.NAME.value()).get()).isNotNull();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete iff
      expect(employeeWriterReader.on(id(4)).iff(r -> false).delete()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeWriterReader.on(id(4)).iff(r -> true).delete()).isPresent();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.on(id(4)).iff(r -> true).delete()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete async iff
      expect(employeeWriterReader.async().on(id(11)).iff(r -> false).delete().get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeWriterReader.async().on(id(11)).iff(r -> true).delete().get()).isPresent();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.async().on(id(11)).iff(r -> true).delete().get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete iff map
      expect(employeeWriterReader.on(id(5)).iff(r -> false).delete(Employee.NAME.value())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeWriterReader.on(id(5)).iff(r -> true).delete(Employee.NAME.value())).isPresent();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.on(id(5)).iff(r -> true).delete(Employee.NAME.value())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete async iff map
      expect(employeeWriterReader.async().on(id(10)).iff(r -> false).delete(Employee.NAME.value()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeWriterReader.async().on(id(10)).iff(r -> true).delete(Employee.NAME.value()).get()).isPresent();
      changesOf(datasetStatistics, 0, 0, 0, 1, 0);

      expect(employeeWriterReader.async().on(id(10)).iff(r -> true).delete(Employee.NAME.value()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // delete stream TODO
      employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(id(4)).or(Record.<Integer>keyFunction().is(id(5))))
          .delete();
//    changesOf(datasetStatistics, 0, 0, 0, 2, 1);

      employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(id(4)).or(Record.<Integer>keyFunction().is(id(5))))
          .delete();
//    changesOf(datasetStatistics, 0, 0, 0, 0, 1);

      // deleteThen stream
      expect(employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(id(6)).or(Record.<Integer>keyFunction().is(id(7))))
          .deleteThen()
          .count()).isEqualTo(2L);
//    changesOf(datasetStatistics, 0, 0, 0, 2, 1);

      expect(employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(id(6)).or(Record.<Integer>keyFunction().is(id(7))))
          .deleteThen()
          .count()).isEqualTo(0L);
//    changesOf(datasetStatistics, 0, 0, 0, 0, 1);
    }
  }

  @Test
  public void update() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      // update
      expect(employeeWriterReader.update(id(1), updateOperation())).isTrue();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect(employeeWriterReader.update(KEY_NOT_FOUND_1, updateOperation())).isFalse();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // update on
      expect(employeeWriterReader.on(id(1)).update(updateOperation())).isPresent();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect(employeeWriterReader.on(KEY_NOT_FOUND_1).update(updateOperation())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // update on map
      expect(employeeWriterReader.on(id(1)).update(updateOperation(), updateMapper())).isPresent();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect(employeeWriterReader.on(KEY_NOT_FOUND_1).update(updateOperation(), updateMapper())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // update async
      expect((boolean) employeeWriterReader.async().update(id(1), updateOperation()).get()).isTrue();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect((boolean) employeeWriterReader.async().update(KEY_NOT_FOUND_1, updateOperation()).get()).isFalse();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // update async on
      expect(employeeWriterReader.async().on(id(1)).update(updateOperation()).get()).isPresent();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect(employeeWriterReader.async().on(KEY_NOT_FOUND_1).update(updateOperation()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // update async on map
      expect(employeeWriterReader.async().on(id(1)).update(updateOperation(), updateMapper()).get()).isPresent();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect(employeeWriterReader.async().on(KEY_NOT_FOUND_1).update(updateOperation(), updateMapper()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // upsert
      employeeWriterReader.on(id(1)).upsert(cellSet(1));
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      employeeWriterReader.on(id(1)).upsert(cellSetArray(1));
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      // upsert async
      employeeWriterReader.async().on(id(1)).upsert(cellSet(1)).get();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      employeeWriterReader.async().on(id(1)).upsert(cellSetArray(1)).get();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      // update iff
      expect(employeeWriterReader.on(id(1)).iff(r -> true).update(updateOperation())).isPresent();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect(employeeWriterReader.on(id(1)).iff(r -> false).update(updateOperation())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeWriterReader.on(KEY_NOT_FOUND_1).iff(r -> true).update(updateOperation())).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // update async iff
      expect(employeeWriterReader.async().on(id(1)).iff(r -> true).update(updateOperation()).get()).isPresent();
      changesOf(datasetStatistics, 0, 0, 1, 0, 0);

      expect(employeeWriterReader.async().on(id(1)).iff(r -> false).update(updateOperation()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      expect(employeeWriterReader.async().on(KEY_NOT_FOUND_1).iff(r -> true).update(updateOperation()).get()).isNotPresent();
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);

      // update stream TODO
      employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(id(4)).or(Record.<Integer>keyFunction().is(id(5))))
          .mutate(updateOperation());
//    changesOf(datasetStatistics, 0, 0, 2, 0, 1);

      employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(KEY_NOT_FOUND_1).or(Record.<Integer>keyFunction().is(KEY_NOT_FOUND_2)))
          .mutate(updateOperation());
//    changesOf(datasetStatistics, 0, 0, 0, 0, 1);

      // updateThen stream
      expect(employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(id(6)).or(Record.<Integer>keyFunction().is(id(7))))
          .mutateThen(updateOperation())
          .count()).isEqualTo(2L);
//    changesOf(datasetStatistics, 0, 0, 2, 0, 1);

      expect(employeeWriterReader.records()
          .filter(Record.<Integer>keyFunction().is(KEY_NOT_FOUND_1).or(Record.<Integer>keyFunction().is(KEY_NOT_FOUND_2)))
          .mutateThen(updateOperation())
          .count()).isEqualTo(0L);
//    changesOf(datasetStatistics, 0, 0, 0, 0, 1);
    }
  }

  @Test
  public void get_failure() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetReader<Integer> employeeReader = dataset.reader();
      long failures = datasetStatistics.get(DatasetOutcomes.GetOutcome.FAILURE);
      try {
        employeeReader.get(null);
        fail("Should throw an exception");
      } catch (RuntimeException e) {
      }
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);
      assertThat(datasetStatistics.get(DatasetOutcomes.GetOutcome.FAILURE)).isEqualTo(failures + 1);
    }
  }

  @Test
  public void create_failure() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      long failures = datasetStatistics.get(DatasetOutcomes.AddOutcome.FAILURE);
      try {
        employeeWriterReader.add(null, cellSetArray(20));
        fail("Should throw an exception");
      } catch (RuntimeException e) {
      }
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);
      assertThat(datasetStatistics.get(DatasetOutcomes.AddOutcome.FAILURE)).isEqualTo(failures + 1);
    }
  }

  @Test
  public void update_failure() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      long failures = datasetStatistics.get(DatasetOutcomes.UpdateOutcome.FAILURE);
      try {
        employeeWriterReader.update(null, updateOperation());
        fail("Should throw an exception");
      } catch (RuntimeException e) {
      }
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);
      assertThat(datasetStatistics.get(DatasetOutcomes.UpdateOutcome.FAILURE)).isEqualTo(failures + 1);
    }
  }

  @Test
  public void delete_failure() throws ExecutionException, InterruptedException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      long failures = datasetStatistics.get(DatasetOutcomes.DeleteOutcome.FAILURE);
      try {
        employeeWriterReader.delete(null);
        fail("Should throw an exception");
      } catch (RuntimeException e) {
      }
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);
      assertThat(datasetStatistics.get(DatasetOutcomes.DeleteOutcome.FAILURE)).isEqualTo(failures + 1);
    }
  }

  @Test
  public void stream_failure() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
      DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
          .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

      DatasetReader<Integer> employeeReader = dataset.reader();
      long failures = datasetStatistics.get(DatasetOutcomes.StreamOutcome.FAILURE);
      // Stream won't fail in general. So, the easiest here is just to set the underlying to null
      // That will for sure cause a NPE and we will have our stat
      DatasetReader<?> current = swapUnderlying(employeeReader, null);
      try {
        employeeReader.records();
        fail("Should throw an exception");
      } catch (RuntimeException e) {
      } finally {
        // Put it back to be nice citizen
        swapUnderlying(employeeReader, current);
      }
      changesOf(datasetStatistics, 0, 0, 0, 0, 0);
      assertThat(datasetStatistics.get(DatasetOutcomes.StreamOutcome.FAILURE)).isEqualTo(failures + 1);
    }
  }

  private DatasetReader<?> swapUnderlying(Object o, DatasetReader<?> reader) throws Exception {
    Field underlying = o.getClass().getDeclaredField("underlying");
    underlying.setAccessible(true);
    DatasetReader<?> current = (DatasetReader<?>) underlying.get(o);
    underlying.set(o, reader);
    return current;
  }

  private UpdateOperation.CellUpdateOperation<Integer, String> updateOperation() {
    return UpdateOperation.write(Employee.NAME).value("aaa");
  }

  private <T extends Comparable<T>> BiFunction<Record<T>, Record<T>, Record<T>> updateMapper() {
    return (r1, r2) -> r2;
  }

  /**
   * Make sure the stat moved only of the expected delta
   *
   * @param get how many hits should have happened
   * @param create how many misses should have happened
   * @param stream how many puts should have happened
   * @param delete how many removes should have happened
   * @param stream how many updates should have happened
   */
  protected void changesOf(DatasetStatistics datasetStatistics, long get, long create, long update, long delete, long stream) {
    assertThat(datasetStatistics.get(DatasetOutcomes.GetOutcome.SUCCESS) - getCount).as(datasetManagerType + " gets").isEqualTo(get);
    assertThat(datasetStatistics.get(DatasetOutcomes.AddOutcome.SUCCESS) - createCount).as(datasetManagerType + " creates").isEqualTo(create);
    assertThat(datasetStatistics.get(DatasetOutcomes.UpdateOutcome.SUCCESS) - updateCount).as(datasetManagerType + " updates").isEqualTo(update);
    assertThat(datasetStatistics.get(DatasetOutcomes.DeleteOutcome.SUCCESS) - deleteCount).as(datasetManagerType + " deletes").isEqualTo(delete);
    assertThat(datasetStatistics.get(DatasetOutcomes.StreamOutcome.SUCCESS) - streamCount).as(datasetManagerType + " streams").isEqualTo(stream);
    getCount += get;
    createCount += create;
    updateCount += update;
    deleteCount += delete;
    streamCount += stream;
  }

  protected String counters() {
    String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();
    DatasetStatistics datasetStatistics = ((InternalDatasetManager) datasetManagerType.getDatasetManager()).getStatisticsService()
        .getDatasetStatistics().stream().filter(stats -> datasetName.equals(stats.getDatasetName())).findAny().get();

    long gets    = datasetStatistics.get(DatasetOutcomes.GetOutcome.SUCCESS) - getCount;
    long creates = datasetStatistics.get(DatasetOutcomes.AddOutcome.SUCCESS) - createCount;
    long updates = datasetStatistics.get(DatasetOutcomes.UpdateOutcome.SUCCESS) - updateCount;
    long deletes = datasetStatistics.get(DatasetOutcomes.DeleteOutcome.SUCCESS) - deleteCount;
    long streams = datasetStatistics.get(DatasetOutcomes.StreamOutcome.SUCCESS) - streamCount;
    return String.format(datasetManagerType + " (G=%d C=%d U=%d D=%d S=%d)", gets, creates, updates, deletes, streams);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static <T> OptionalAssert<T> expect(Optional<T> actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static <ELEMENT> AbstractListAssert<?, List<? extends ELEMENT>, ELEMENT, ObjectAssert<ELEMENT>> expect(Stream<? extends ELEMENT> actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static AbstractBooleanAssert<?> expect(boolean actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static AbstractLongAssert<?> expect(long actual) {
    return assertThat(actual);
  }
}
