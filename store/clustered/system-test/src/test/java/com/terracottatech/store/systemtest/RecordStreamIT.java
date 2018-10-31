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
import com.terracottatech.store.Record;
import com.terracottatech.store.common.test.Employee;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.store.common.test.Employee.BIRTH_DAY;
import static com.terracottatech.store.common.test.Employee.BIRTH_MONTH;
import static com.terracottatech.store.common.test.Employee.BIRTH_YEAR;
import static com.terracottatech.store.common.test.Employee.BONUS;
import static com.terracottatech.store.common.test.Employee.CELL_NUMBER;
import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.CURRENT;
import static com.terracottatech.store.common.test.Employee.GENDER;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.TELEPHONE;
import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for Record Stream operations
 */
public class RecordStreamIT extends CRUDTests {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void initializeMapAndDataset() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // Read Employee.csv and initialize List of Employee i.e. employeeList
      initializeEmployeeList();

      // Fill in data to employee Dataset using the employeeList
      employeeList.stream()
          .forEach(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));
    }
  }

  @Test
  public void filterTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // Integer Filter with allMatch()
      List<Employee> lambdaFilteredEmployees = employeeReader.records()
          .filter(rec -> rec.get(BIRTH_YEAR).get() > 1975)
          .map(Employee::new)
          .collect(toList());
      List<Employee> portableFilteredEmployees = employeeReader.records()
          .filter(BIRTH_YEAR.value().isGreaterThan(1975))
          .map(Employee::new)
          .collect(toList());
      List<Employee> filteredEmployeeList = employeeList.stream()
          .filter(e -> e.getBirthYear() > 1975)
          .collect(toList());
      assertThat(lambdaFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));
      assertThat(portableFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));

      // Char Filter with anyMatch()
      lambdaFilteredEmployees = employeeReader.records()
          .filter(rec -> rec.get(GENDER).get().equals('M'))
          .map(Employee::new)
          .collect(toList());
      portableFilteredEmployees = employeeReader.records()
          .filter(GENDER.value().is('M'))
          .map(Employee::new)
          .collect(toList());
      filteredEmployeeList = employeeList.stream()
          .filter(e -> e.getGender() == 'M')
          .collect(toList());
      assertThat(lambdaFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));
      assertThat(portableFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));

      // String Filter
      lambdaFilteredEmployees = employeeReader.records()
          .filter(rec -> rec.get(COUNTRY_ADDRESS).get().equals("USA"))
          .map(Employee::new)
          .collect(toList());
      portableFilteredEmployees = employeeReader.records()
          .filter(COUNTRY_ADDRESS.value().is("USA"))
          .map(Employee::new)
          .collect(toList());
      filteredEmployeeList = employeeList.stream()
          .filter(e -> e.getCountryAddress().equals("USA"))
          .collect(toList());
      assertThat(lambdaFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));
      assertThat(portableFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));

      // Boolean Filter
      lambdaFilteredEmployees = employeeReader.records()
          .filter(rec -> rec.get(CURRENT).get().equals(true))
          .map(Employee::new)
          .collect(toList());
      portableFilteredEmployees = employeeReader.records()
          .filter(CURRENT.value().is(true))
          .map(Employee::new)
          .collect(toList());
      filteredEmployeeList = employeeList.stream()
          .filter(e -> e.getCurrent().equals(true))
          .collect(toList());
      assertThat(lambdaFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));
      assertThat(portableFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));

      // Key Filter
      lambdaFilteredEmployees = employeeReader.records()
          .filter(rec -> rec.getKey() % 2 == 0)
          .map(Employee::new)
          .collect(toList());
      filteredEmployeeList = employeeList.stream()
          .filter(e -> e.getEmpID() % 2 == 0)
          .collect(toList());
      assertThat(lambdaFilteredEmployees, containsInAnyOrder(filteredEmployeeList.toArray()));
    }
  }

  @Test
  public void mapTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<String> recordNames = employeeReader.records()
          .map(NAME.value())
          .map(Optional::get)
          .collect(toList());
      List<String> listNames = employeeList.stream()
          .map(Employee::getName)
          .collect(toList());
      assertThat(recordNames, containsInAnyOrder(listNames.toArray()));

      // valueOrFail
      recordNames = employeeReader.records()
          .map(NAME.valueOrFail())
          .collect(toList());
      assertThat(recordNames, containsInAnyOrder(listNames.toArray()));

      List<String> recordDOBs = employeeReader.records()
          .map(record -> new String(record.get(BIRTH_DAY).get() + "/" +
              record.get(BIRTH_MONTH).get() + "/" +
              record.get(BIRTH_YEAR).get()))
          .collect(toList());
      List<String> listDOBs = employeeList.stream()
          .map(employee -> new String(employee.getBirthDay() + "/" +
              employee.getBirthMonth() + "/" +
              employee.getBirthYear()))
          .collect(toList());
      assertThat(recordDOBs, containsInAnyOrder(listDOBs.toArray()));
    }
  }

  @Test
  public void mapToIntTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int recordOldestMaleEmployeeDOY = employeeReader.records()
          .filter(GENDER.value().is('M'))
          .mapToInt(record -> record.get(BIRTH_YEAR).get())
          .min()
          .getAsInt();

      int listOldestMaleEmployeeDOY = employeeList.stream()
          .filter(employee -> employee.getGender() == 'M')
          .mapToInt(Employee::getBirthYear)
          .min()
          .getAsInt();

      assertEquals(recordOldestMaleEmployeeDOY, listOldestMaleEmployeeDOY);

      // intValueOrFail
      recordOldestMaleEmployeeDOY = employeeReader.records()
          .filter(GENDER.value().is('M'))
          .mapToInt(BIRTH_YEAR.intValueOrFail())
          .min()
          .getAsInt();

      assertEquals(recordOldestMaleEmployeeDOY, listOldestMaleEmployeeDOY);
    }
  }

  @Test
  public void mapToLongTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      long[] recordAllCellNumber = employeeReader.records()
          .mapToLong(record -> record.get(CELL_NUMBER).get())
          .toArray();

      long[] listAllCellNUmber = employeeList.stream()
          .mapToLong(Employee::getCellNumber)
          .toArray();

      assertThat(recordAllCellNumber, longArrayContainsInAnyOrder(listAllCellNUmber));

      // longValueOrFail
      recordAllCellNumber = employeeReader.records()
          .mapToLong(CELL_NUMBER.longValueOrFail())
          .toArray();

      assertThat(recordAllCellNumber, longArrayContainsInAnyOrder(listAllCellNUmber));
    }
  }

  @Test
  public void mapToDoubleTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      double recordHigherThanMillionBonus[] = employeeReader.records()
          .filter(record -> record.get(BONUS).get() > 1000000)
          .mapToDouble(record -> record.get(BONUS).get())
          .toArray();

      double listHigherThanMillionBonus[] = employeeList.stream()
          .filter(employee -> employee.getBonus() > 1000000)
          .mapToDouble(Employee::getBonus)
          .toArray();

      assertThat(recordHigherThanMillionBonus, doubleArrayContainsInAnyOrder(listHigherThanMillionBonus));

      recordHigherThanMillionBonus = employeeReader.records()
          .filter(record -> record.get(BONUS).get() > 1000000)
          .mapToDouble(BONUS.doubleValueOrFail())
          .toArray();

      assertThat(recordHigherThanMillionBonus, doubleArrayContainsInAnyOrder(listHigherThanMillionBonus));
    }
  }

  @Test
  public void flatMapTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<String> recordAllNamesOrCountryNames = employeeReader.records()
          .flatMap(record -> Stream.of(record.get(NAME).get(), record.get(COUNTRY_ADDRESS).get()))
          .collect(toList());

      List<String> listAllAllNamesOrCountryNames = employeeList.stream()
          .flatMap(employee -> Stream.of(employee.getName(), employee.getCountryAddress()))
          .collect(toList());

      assertThat(recordAllNamesOrCountryNames, containsInAnyOrder(listAllAllNamesOrCountryNames.toArray()));
    }
  }

  @Test
  public void flatMapToIntTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int recordFlatMapDOBs[] = employeeReader.records()
          .flatMapToInt(record -> IntStream.of(record.get(BIRTH_DAY).get(),
              record.get(BIRTH_MONTH).get(),
              record.get(BIRTH_YEAR).get()))
          .toArray();

      int listFlatMapDOBs[] = employeeList.stream()
          .flatMapToInt(employee -> IntStream.of(employee.getBirthDay(),
              employee.getBirthMonth(),
              employee.getBirthYear()))
          .toArray();


      assertThat(recordFlatMapDOBs, intArrayContainsInAnyOrder(listFlatMapDOBs));
    }
  }

  @Test
  public void flatMapToLongTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      long recordAllContactNumbers[] = employeeReader.records()
          .flatMapToLong(record -> LongStream.of(record.get(TELEPHONE).get(),
              record.get(CELL_NUMBER).get()))
          .toArray();

      long listAllContactNumbers[] = employeeList.stream()
          .flatMapToLong(employee -> LongStream.of(employee.getTelephone(),
              employee.getCellNumber()))
          .toArray();

      assertThat(recordAllContactNumbers, longArrayContainsInAnyOrder(listAllContactNumbers));
    }
  }

  @Test
  public void flatMapToDoubleTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      double recordAllCompensations[] = employeeReader.records()
          .flatMapToDouble(record -> DoubleStream.of(record.get(SALARY).get(),
              record.get(BONUS).get()))
          .toArray();

      double listAllCompensations[] = employeeList.stream()
          .flatMapToDouble(employee -> DoubleStream.of(employee.getSalary(),
              employee.getBonus()))
          .toArray();

      assertThat(recordAllCompensations, doubleArrayContainsInAnyOrder(listAllCompensations));
    }
  }

  @Test
  public void distinctTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Employee existingFemaleEmployee = new Employee(employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('F'))
          .findFirst()
          .get());

      // add duplicate CellSet with new empID
      assertTrue(employeeWriterReader.add(generateUniqueEmpID(), existingFemaleEmployee.getCellSet()));

      // distinct should not eliminate based on the cellset only
      assertEquals(employeeReader.records().distinct().count(),
          employeeReader.records().count());

      // but should still eliminate the duplicate cellset
      assertEquals(employeeReader.records().map(CellSet::new).distinct().count(),
          employeeReader.records().map(CellSet::new).count() - 1);
    }
  }

  @Test
  public void sortedTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      exception.expect(UnsupportedOperationException.class);
      exception.expectMessage("sorted() is not supported - Record is not Comparable, what you mean probably is sorted(keyFunction().asComparator())");
      employeeReader.records()
          .sorted()
          .forEach(r -> {
          });
    }
  }

  @Test
  public void sortedOnMutativeStreamTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      exception.expect(UnsupportedOperationException.class);
      exception.expectMessage("sorted() is not supported - Record is not Comparable, what you mean probably is sorted(keyFunction().asComparator())");
      employeeWriterReader.records()
          .sorted()
          .forEach(r -> {
          });
    }
  }

  @Test
  public void sortedWithComparatorTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<Employee> recordEmployees = employeeReader.records()
          .sorted(Comparator.comparing(record -> record.get(NAME).get()))
          .map(Employee::new)
          .collect(toList());

      List<Employee> listEmployees = employeeList.stream()
          .sorted(Comparator.comparing(Employee::getName))
          .collect(toList());

      assertTrue(recordEmployees.equals(listEmployees));

      recordEmployees = employeeReader.records()
          .sorted(NAME.valueOr("").asComparator())
          .map(Employee::new)
          .collect(toList());

      assertTrue(recordEmployees.equals(listEmployees));

      recordEmployees = employeeReader.records()
          .sorted(NAME.valueOr("").asComparator().reversed())
          .map(Employee::new)
          .collect(toList());

      Collections.reverse(listEmployees);
      assertTrue(recordEmployees.equals(listEmployees));
    }
  }

  @Test
  public void peekTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<Employee> peekRecordEmployees = new ArrayList<>(50);
      List<Employee> recordEmployees = employeeReader.records()
          .peek(record -> peekRecordEmployees.add(new Employee(record)))
          .map(Employee::new)
          .collect(toList());

      assertTrue(peekRecordEmployees.equals(recordEmployees));
    }
  }

  @Test
  public void limitTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<Employee> recordEmployees = employeeReader.records()
          .limit(5L)
          .map(Employee::new)
          .collect(toList());

      assertEquals(5, recordEmployees.size());

      // All records collected should be part of original set
      assertTrue(recordEmployees.stream()
          .allMatch(employee -> employee.getCellSet().containsAll(employeeReader.get(employee.getEmpID()).get())));

      // Max way more than the number of records
      recordEmployees = employeeReader.records()
          .limit(500000L)
          .map(Employee::new)
          .collect(toList());

      assertThat(recordEmployees, containsInAnyOrder(employeeList.toArray()));
    }
  }

  @Test
  public void limitOnSortedTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<Employee> recordEmployees = employeeReader.records()
          .sorted(Comparator.comparing(SALARY.valueOr(0D)))
          .limit(5L)
          .map(Employee::new)
          .collect(toList());

      assertThat(recordEmployees, containsInAnyOrder(employeeList.stream()
          .sorted(Comparator.comparing(Employee::getSalary))
          .limit(5L)
          .toArray()));
    }
  }

  @Test
  public void skipTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<Employee> skipRecordEmployees = employeeReader.records()
          .skip(3L)
          .map(Employee::new)
          .collect(toList());

      List<Employee> allRecordEmployees = employeeReader.records()
          .map(Employee::new)
          .collect(toList());

      assertEquals(skipRecordEmployees.size(), allRecordEmployees.size() - 3);

      // All records collected should be part of original set
      assertTrue(skipRecordEmployees.stream()
          .allMatch(employee -> employee.getCellSet().containsAll(employeeReader.get(employee.getEmpID()).get())));

      // Skip way more than the number of records
      assertTrue(employeeReader.records().skip(500000L).count() == 0);
    }
  }

  @Test
  public void forEachTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Map<String, Double> recordTotalCompensationMap = new HashMap<>(50);
      employeeReader.records()
          .forEach(record -> recordTotalCompensationMap.put(record.get(NAME).get(),
              record.get(SALARY).get() + record.get(BONUS).get()));

      Map<String, Double> listTotalCompensationMap = new HashMap<>(50);
      employeeList.stream()
          .forEach(employee -> listTotalCompensationMap.put(employee.getName(),
              employee.getSalary() + employee.getBonus()));

      assertTrue(recordTotalCompensationMap.equals(listTotalCompensationMap));
    }
  }

  @Test
  public void toArrayTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      @SuppressWarnings("unchecked")
      Record<Integer> recordObjects[] = dataset.reader()
              .records()
              .toArray(Record[]::new);
      List<Employee> recordEmployeeList = Arrays.stream(recordObjects)
          .map(Employee::new)
          .collect(toList());

      assertThat(recordEmployeeList, containsInAnyOrder(employeeList.toArray()));
    }
  }

  @Test
  public void toArrayIntFunctionTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      @SuppressWarnings("unchecked")
      List<Record<Integer>> records = asList(employeeReader.records().toArray(Record[]::new));

      Stream<Employee> employeeStream = records.stream().map(Employee::new);
      assertThat(employeeStream.collect(toList()), containsInAnyOrder(employeeList.toArray(new Employee[0])));
    }
  }

  @Test
  public void reduce1Test() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Record<Integer> recordHighestPaidEmployee = employeeReader.records()
          .reduce(employeeReader.records().findAny().get(),
              (record1, record2) -> (record1.get(SALARY).get() > record2.get(SALARY).get()) ? record1 : record2);

      Employee listHighestPaidEmployee = employeeList.stream()
          .reduce(employeeList.stream().findAny().get(),
              (employee1, employee2) -> (employee1.getSalary() > employee2.getSalary()) ? employee1 : employee2);

      assertTrue(new Employee(recordHighestPaidEmployee).equals(listHighestPaidEmployee));
    }
  }

  @Test
  public void reduce2Test() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Record<Integer> recordHighestPaidEmployee = employeeReader.records()
          .reduce((record1, record2) -> (record1.get(SALARY).get() > record2.get(SALARY).get()) ? record1 : record2)
          .get();

      Employee listHighestPaidEmployee = employeeList.stream()
          .reduce((employee1, employee2) -> (employee1.getSalary() > employee2.getSalary()) ? employee1 : employee2)
          .get();

      assertTrue(new Employee(recordHighestPaidEmployee).equals(listHighestPaidEmployee));
    }
  }

  @Test
  public void reduce3Test() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      double recordHighestSalary = employeeReader.records()
          .reduce(0D,
              (max, record) -> (max > record.get(SALARY).get()) ? max : record.get(SALARY).get(),
              (x, y) -> (x > y) ? x : y);

      double listHighestSalary = employeeList.stream()
          .reduce(0D,
              (max, employee) -> (max > employee.getSalary()) ? max : employee.getSalary(),
              (x, y) -> (x > y) ? x : y);

      assertEquals(recordHighestSalary, listHighestSalary, 1e-8);
    }
  }

  @Test
  public void collect1Test() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Map<String, List<Employee>> recordCountryWiseEmployees = employeeReader.records()
          .collect(Collectors.groupingBy(record -> record.get(COUNTRY_ADDRESS).get()))
          .entrySet()
          .stream()
          .collect(toMap(HashMap.Entry::getKey,
              entry -> entry.getValue().stream()
                  .map(Employee::new)
                  .collect(toList())));

      Map<String, List<Employee>> listCountryWiseEmployees = employeeList.stream()
          .collect(Collectors.groupingBy(Employee::getCountryAddress));

      assertThat(recordCountryWiseEmployees.keySet(), containsInAnyOrder(listCountryWiseEmployees.keySet().toArray()));
      for (Map.Entry<String, List<Employee>> entry : recordCountryWiseEmployees.entrySet()) {
        assertThat(entry.getValue(), containsInAnyOrder(listCountryWiseEmployees.get(entry.getKey()).toArray()));
      }
    }
  }

  @Test
  public void collect2Test() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      List<Record<Integer>> recordEmployees = employeeReader.records()
          .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

      List<Employee> recordEmployeeList =
          recordEmployees.stream()
              .map(Employee::new)
              .collect(toList());

      assertThat(recordEmployeeList, containsInAnyOrder(employeeList.toArray()));
    }
  }

  @Test
  public void minTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      String recordSmallestName = employeeReader.records()
          .min((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
          .get()
          .get(NAME)
          .get();

      String listSmallestName = employeeList.stream()
          .min((employee1, employee2) -> Integer.compare(employee1.getName().length(), employee2.getName().length()))
          .get()
          .getName();

      assertThat(recordSmallestName.length(), is(listSmallestName.length()));

      // min on empty stream returns empty optional
      assertFalse(employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('Z'))
          .min((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
          .isPresent());
    }
  }

  @Test
  public void maxTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      String recordLargestName = employeeReader.records()
          .max((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
          .get()
          .get(NAME)
          .get();

      String listLargestName = employeeList.stream()
          .max((employee1, employee2) -> Integer.compare(employee1.getName().length(), employee2.getName().length()))
          .get()
          .getName();

      assertThat(recordLargestName.length(), is(listLargestName.length()));

      // max on empty stream returns empty optional
      assertFalse(employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('Z'))
          .max((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
          .isPresent());
    }
  }

  @Test
  public void countTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertEquals(employeeReader.records().count(), employeeList.stream().count());
      assertEquals(employeeReader.records()
              .filter(GENDER.value().is('M'))
              .count(),
          employeeList.stream()
              .filter(employee -> employee.getGender() == 'M')
              .count());
    }
  }

  @Test
  public void anyMatchTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      assertTrue(employeeReader.records().anyMatch(record -> record.get(GENDER).get().equals('M')));
      assertFalse(employeeReader.records().anyMatch(record -> record.get(GENDER).get().equals('Z')));
      assertFalse(employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('Z'))
          .anyMatch(record -> record.get(GENDER).get().equals('M')));
    }
  }

  @Test
  public void allMatchTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // allMatch on Empty Input stream as a result of filter
      assertTrue(employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('Z'))
          .allMatch(record -> record.get(GENDER).get().equals('Z')));

      // positive case
      assertTrue(employeeReader.records()
          .allMatch(record -> record.get(GENDER).get().equals('M') || record.get(GENDER).get().equals('F')));

      // negative case
      assertFalse(employeeReader.records()
          .allMatch(record -> record.get(GENDER).get().equals('M')));
    }
  }

  @Test
  public void noneMatchTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // noneMatch on Empty Input stream as a result of filter even if true is the predicate
      assertTrue(employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('Z'))
          .noneMatch(record -> true));

      // positive case
      assertTrue(employeeReader.records()
          .noneMatch(record -> !(record.get(GENDER).get().equals('M') || record.get(GENDER).get().equals('F'))));

      // negative case
      assertFalse(employeeReader.records()
          .noneMatch(record -> record.get(GENDER).get().equals('M')));
    }
  }

  @Test
  public void findFirstTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // Existing Record Check
      Optional<Record<Integer>> optionalRecord = employeeReader.records()
          .filter(record -> record.get(COUNTRY_ADDRESS).get().equals("England"))
          .sorted(Comparator.comparing(NAME.valueOrFail()))
          .findFirst();
      assertTrue(optionalRecord.isPresent());
      assertTrue(optionalRecord.get().get(COUNTRY_ADDRESS).get().equals("England"));

      Employee recordEmployee = new Employee(optionalRecord.get());
      Employee listEmployee = employeeList.stream()
          .filter(employee -> employee.getCountryAddress().equals("England"))
          .sorted(Comparator.comparing(Employee::getName))
          .findFirst()
          .get();
      assertTrue(recordEmployee.equals(listEmployee));

      //Non-Existing Record Check
      optionalRecord = employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('Z'))
          .findFirst();
      assertFalse(optionalRecord.isPresent());
    }
  }

  @Test
  public void firstAnyTest() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // Existing Record Check
      Optional<Record<Integer>> optionalRecord = employeeReader.records()
          .filter(record -> record.get(CITY_ADDRESS).get().equals("Hyderabad"))
          .findAny();
      assertTrue(optionalRecord.isPresent());
      assertTrue(optionalRecord.get().get(CITY_ADDRESS).get().equals("Hyderabad"));

      //Non-Existing Record Check
      optionalRecord = employeeReader.records()
          .filter(record -> record.get(GENDER).get().equals('Z'))
          .findAny();
      assertFalse(optionalRecord.isPresent());
    }
  }

  private Matcher<int[]> intArrayContainsInAnyOrder(int[] matching) {
    final ArrayList<Matcher<Integer>> matchers = new ArrayList<>();
    for (Integer val : matching) {
      matchers.add(is(val));
    }
    return new TypeSafeMatcher<int[]>() {
      private final ArrayList<Matcher<Integer>> internalMatchers = new ArrayList<>(matchers);
      private double nonMatch;

      @Override
      public void describeTo(Description description) {
        description.appendList("[", ", ", "]", internalMatchers)
            .appendText(" in any order ");
      }

      @Override
      protected boolean matchesSafely(int[] longs) {
        if (longs.length != internalMatchers.size()) {
          return false;
        }
        for (int aLong : longs) {
          Matcher<Integer> matchingMatcher = null;
          for (Matcher<Integer> matcher : internalMatchers) {
            if (matcher.matches(aLong)) {
              matchingMatcher = matcher;
              break;
            }
          }
          if (matchingMatcher != null) {
            internalMatchers.remove(matchingMatcher);
          } else {
            nonMatch = aLong;
            return false;
          }
        }
        return internalMatchers.isEmpty();
      }
    };
  }

  private Matcher<long[]> longArrayContainsInAnyOrder(long[] matching) {
    final ArrayList<Matcher<Long>> matchers = new ArrayList<>();
    for (Long val : matching) {
      matchers.add(is(val));
    }
    return new TypeSafeMatcher<long[]>() {
      private final ArrayList<Matcher<Long>> internalMatchers = new ArrayList<>(matchers);
      private double nonMatch;

      @Override
      public void describeTo(Description description) {
        description.appendList("[", ", ", "]", internalMatchers)
            .appendText(" in any order ");
      }

      @Override
      protected boolean matchesSafely(long[] longs) {
        if (longs.length != internalMatchers.size()) {
          return false;
        }
        for (long aLong : longs) {
          Matcher<Long> matchingMatcher = null;
          for (Matcher<Long> matcher : internalMatchers) {
            if (matcher.matches(aLong)) {
              matchingMatcher = matcher;
              break;
            }
          }
          if (matchingMatcher != null) {
            internalMatchers.remove(matchingMatcher);
          } else {
            nonMatch = aLong;
            return false;
          }
        }
        return internalMatchers.isEmpty();
      }
    };
  }

  private Matcher<double[]> doubleArrayContainsInAnyOrder(double[] matching) {
    final ArrayList<Matcher<Double>> matchers = new ArrayList<>();
    for (Double val : matching) {
      matchers.add(is(val));
    }
    return new TypeSafeMatcher<double[]>() {
      private final ArrayList<Matcher<Double>> internalMatchers = new ArrayList<>(matchers);
      private double nonMatch;

      @Override
      public void describeTo(Description description) {
        description.appendList("[", ", ", "]", internalMatchers)
            .appendText(" in any order ");
      }

      @Override
      protected boolean matchesSafely(double[] longs) {
        if (longs.length != internalMatchers.size()) {
          return false;
        }
        for (double aLong : longs) {
          Matcher<Double> matchingMatcher = null;
          for (Matcher<Double> matcher : internalMatchers) {
            if (matcher.matches(aLong)) {
              matchingMatcher = matcher;
              break;
            }
          }
          if (matchingMatcher != null) {
            internalMatchers.remove(matchingMatcher);
          } else {
            nonMatch = aLong;
            return false;
          }
        }
        return internalMatchers.isEmpty();
      }
    };
  }

}
