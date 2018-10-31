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

package com.terracottatech.store.common.test;

import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toSet;

public class Employees {
  private final Set<Employee> employees;

  public static final String name = "Lalu Yadav******************************************************************************************";
  public static final Long cellNumber = 108L;
  public static final String street = "Taraiyaxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
  public static final String city = "Chapra!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
  public static final String country = "Bihar...........................................................................................................";

  public static final CellSet dummyCellSet = CellSet.of(Employee.NAME.newCell(name),
      Employee.CELL_NUMBER.newCell(cellNumber),
      Employee.STREET_ADDRESS.newCell(street),
      Employee.CITY_ADDRESS.newCell(city),
      Employee.COUNTRY_ADDRESS.newCell(country));

  public Employees(Dataset<Integer> employeeDataset) {
    employees = employeeDataset.reader()
        .records()
        .map(Employee::new)
        .collect(toSet());
  }

  public boolean equals(Dataset<Integer> employeeDataset) {
    return employees.equals(new Employees(employeeDataset).employees);
  }

  // Starting unique EmployeeID
  private static AtomicInteger employeeID = new AtomicInteger(0);
  public static int generateUniqueEmpID() {
    return employeeID.incrementAndGet();
  }

  public static void addNEmployeeRecords(DatasetWriterReader<Integer> datasetWriterReader, CellSet cellSet, int n) {
    for (int i = 0; i < n; i++) {
      datasetWriterReader.add(generateUniqueEmpID(), cellSet);
    }
  }

  public static void addNEmployeeRecords(DatasetWriterReader<Integer> datasetWriterReader, int n) {
    for (int i = 0; i < n; i++) {
      datasetWriterReader.add(generateUniqueEmpID(), dummyCellSet);
    }
  }
}
