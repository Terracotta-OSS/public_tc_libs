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
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashSet;
import java.util.stream.Collectors;

import static com.terracottatech.store.function.Collectors.averagingDouble;
import static com.terracottatech.store.function.Collectors.averagingInt;
import static com.terracottatech.store.function.Collectors.averagingLong;
import static com.terracottatech.store.function.Collectors.composite;
import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.function.Collectors.filtering;
import static com.terracottatech.store.function.Collectors.groupingBy;
import static com.terracottatech.store.function.Collectors.groupingByConcurrent;
import static com.terracottatech.store.function.Collectors.mapping;
import static com.terracottatech.store.function.Collectors.maxBy;
import static com.terracottatech.store.function.Collectors.minBy;
import static com.terracottatech.store.function.Collectors.partitioningBy;
import static com.terracottatech.store.function.Collectors.summarizingDouble;
import static com.terracottatech.store.function.Collectors.summarizingInt;
import static com.terracottatech.store.function.Collectors.summarizingLong;
import static com.terracottatech.store.function.Collectors.summingDouble;
import static com.terracottatech.store.function.Collectors.summingInt;
import static com.terracottatech.store.function.Collectors.summingLong;
import static com.terracottatech.store.common.test.Employee.BIRTH_YEAR;
import static com.terracottatech.store.common.test.Employee.BONUS;
import static com.terracottatech.store.common.test.Employee.CELL_NUMBER;
import static com.terracottatech.store.common.test.Employee.CURRENT;
import static com.terracottatech.store.common.test.Employee.HOUSE_NUMBER_ADDRESS;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.TELEPHONE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CollectorsIT extends CRUDTests {

  @Before
  public void initializeMapAndDataset() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      // Read Employee.csv and initialize List of Employee i.e. employeeList
      initializeEmployeeList();

      // Fill in data to employee Dataset using the employeeList
      employeeList.stream()
          .forEach(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));
    }
  }

  @Test
  public void compositeCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(composite(averagingDouble(SALARY.doubleValueOr(0D)), averagingInt(BIRTH_YEAR.intValueOr(0)))),
          equalTo(asList(employeeList.stream().collect(Collectors.averagingDouble(Employee::getSalary)),
              employeeList.stream().collect(Collectors.averagingInt(Employee::getBirthYear)))));
    }
  }

  @Test
  public void filteringCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(filtering(CURRENT.isTrue(), averagingDouble(SALARY.doubleValueOr(0D)))),
          equalTo(employeeList.stream().filter(Employee::getCurrent).collect(Collectors.averagingDouble(Employee::getSalary))));
    }
  }

  @Test
  public void mappingCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(mapping(Employee::new, toSet())),
          equalTo(new HashSet<Employee>(employeeList)));
    }
  }


  @Test
  public void countingCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(counting()),
          equalTo(employeeList.stream().collect(Collectors.counting())));
    }
  }

  @Test
  public void minByCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(new Employee(employeeReader.records()
              .collect(minBy(Comparator.comparing(record -> record.get(SALARY).orElse(1_000_000_000D)))).get()),
          equalTo(employeeList.stream().collect(Collectors.minBy(Comparator.comparing(Employee::getSalary))).get()));
    }
  }

  @Test
  public void maxByCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(new Employee(employeeReader.records()
              .collect(maxBy(Comparator.comparing(record -> record.get(SALARY).orElse(0D)))).get()),
          equalTo(employeeList.stream().collect(Collectors.maxBy(Comparator.comparing(Employee::getSalary))).get()));
    }
  }

  @Test
  public void summingIntCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(summingInt(HOUSE_NUMBER_ADDRESS.intValueOr(0))),
          equalTo(employeeList.stream().collect(Collectors.summingInt(Employee::getHouseNumber))));
    }
  }

  @Test
  public void summingLongCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(summingLong(CELL_NUMBER.longValueOr(0L))),
          equalTo(employeeList.stream().collect(Collectors.summingLong(Employee::getCellNumber))));
    }
  }

  @Test
  public void summingDoubleCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(summingDouble(BONUS.doubleValueOr(0D))),
          equalTo(employeeList.stream().collect(Collectors.summingDouble(Employee::getBonus))));
    }
  }

  @Test
  public void averagingIntCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(averagingInt(BIRTH_YEAR.intValueOr(0))),
          equalTo(employeeList.stream().collect(Collectors.averagingInt(Employee::getBirthYear))));
    }
  }

  @Test
  public void averagingLongCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(averagingLong(TELEPHONE.longValueOr(0L))),
          equalTo(employeeList.stream().collect(Collectors.averagingLong(Employee::getTelephone))));
    }
  }

  @Test
  public void averagingDoubleCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(averagingDouble(BONUS.doubleValueOr(0D))),
          equalTo(employeeList.stream().collect(Collectors.averagingDouble(Employee::getBonus))));
    }
  }

  @Test
  public void groupingByCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(groupingBy(BIRTH_YEAR.valueOr(0), averagingDouble(SALARY.doubleValueOr(0D)))),
          equalTo(employeeList.stream().collect(Collectors.groupingBy(Employee::getBirthYear, Collectors.averagingDouble(Employee::getSalary)))));
    }
  }

  @Test
  public void groupingByConcurrentCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(groupingByConcurrent(BIRTH_YEAR.valueOr(0), averagingDouble(SALARY.doubleValueOr(0D)))),
          equalTo(employeeList.stream().collect(Collectors.groupingByConcurrent(Employee::getBirthYear, Collectors.averagingDouble(Employee::getSalary)))));
    }
  }

  @Test
  public void partitioningByCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(partitioningBy(CURRENT.valueOr(false).is(true), averagingDouble(SALARY.doubleValueOr(0D)))),
          equalTo(employeeList.stream().collect(Collectors.partitioningBy(Employee::getCurrent, Collectors.averagingDouble(Employee::getSalary)))));
    }
  }

  @Test
  public void summarizingIntCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(summarizingInt(BIRTH_YEAR.intValueOr(0))).toString(),
          equalTo(employeeList.stream().collect(Collectors.summarizingInt(Employee::getBirthYear)).toString()));
    }
  }

  @Test
  public void summarizingLongCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(summarizingLong(CELL_NUMBER.longValueOr(0))).toString(),
          equalTo(employeeList.stream().collect(Collectors.summarizingLong(Employee::getCellNumber)).toString()));
    }
  }

  @Test
  public void summarizingDoubleCollectorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertThat(employeeReader.records().collect(summarizingDouble(SALARY.doubleValueOr(0))).toString(),
          equalTo(employeeList.stream().collect(Collectors.summarizingDouble(Employee::getSalary)).toString()));
    }
  }
}
