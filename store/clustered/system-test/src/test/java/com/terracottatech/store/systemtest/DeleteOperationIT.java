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

import com.terracottatech.store.Cell;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;

import com.terracottatech.store.common.test.Employee;
import org.junit.Test;

import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Delete Operation Test cases
 */
public class DeleteOperationIT extends CRUDTests {

  @Test
  public void deleteNonExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.delete(empID));
    }
  }

  @Test
  public void deleteExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Grover Cleveland"),
          Employee.GENDER.newCell('M'),
          Employee.SALARY.newCell(100000.0D),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertTrue(employeeWriterReader.delete(empID));
      assertFalse(employeeWriterReader.get(empID).isPresent());
      assertFalse(employeeReader.get(empID).isPresent());
    }
  }

  @Test
  public void onDeleteNonExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.on(empID).delete().isPresent());
    }
  }

  @Test
  public void onDeleteExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Benjamin Harrison"),
          Employee.GENDER.newCell('M'),
          Employee.TELEPHONE.newCell(911L)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Record<Integer> rec = employeeWriterReader.on(empID).delete().get();
      assertThat(rec, containsInAnyOrder(cells));
      assertTrue(rec.getKey().equals(empID));
      assertFalse(employeeWriterReader.get(empID).isPresent());
      assertFalse(employeeReader.get(empID).isPresent());
    }
  }

  @Test
  public void onDeleteNonExistingMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.on(empID).delete(Employee.NAME.value()).isPresent());
      assertFalse(employeeWriterReader.on(empID).delete(rec -> rec.getKey()).isPresent());
    }
  }

  @Test
  public void onDeleteExistingMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("William McKinley"),
          Employee.SSN.newCell(978564),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertEquals("William McKinley", employeeWriterReader.on(empID).delete(Employee.NAME.value()).get().get());
    }
  }

  @Test
  public void onDeleteExistingMapperCellDefinitionAbsent() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("William McKinley Jr"),
          Employee.SSN.newCell(978564),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertFalse(employeeWriterReader.on(empID).delete(Employee.GENDER.value()).get().isPresent());
      assertFalse(employeeWriterReader.get(empID).isPresent());
      assertFalse(employeeReader.get(empID).isPresent());
    }
  }

  @Test
  public void onIffDeleteNonExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.on(empID).iff(rec -> true).delete().isPresent());
      assertFalse(employeeWriterReader.on(empID).iff(rec -> rec.getKey().equals(empID)).delete().isPresent());
    }
  }

  @Test
  public void onIffDeleteNonQualifying() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Theodore Roosevelt"),
          Employee.GENDER.newCell('M')
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertFalse(employeeWriterReader
          .on(empID)
          .iff(rec -> rec.get(Employee.GENDER).get().equals('F'))
          .delete()
          .isPresent());
      assertTrue(employeeWriterReader
          .on(empID)
          .iff(rec -> rec.get(Employee.GENDER).get().equals('M'))
          .delete()
          .isPresent());
    }
  }

  @Test
  public void onIffDeleteQualifying() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("William Howard Taft"),
          Employee.TELEPHONE.newCell(100L),
          Employee.CURRENT.newCell(false)

      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertThat(employeeWriterReader
              .on(empID)
              .iff(rec -> rec.get(Employee.TELEPHONE).get().equals(100L))
              .delete()
              .get(),
          containsInAnyOrder(cells));
      assertFalse(employeeWriterReader.get(empID).isPresent());
      assertFalse(employeeReader.get(empID).isPresent());
    }
  }
}
