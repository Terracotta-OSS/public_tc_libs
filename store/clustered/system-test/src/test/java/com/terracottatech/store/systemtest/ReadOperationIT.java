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
 * Test class for Read Operation Testing
 */
public class ReadOperationIT extends CRUDTests {

  @Test
  public void getExistingRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Abraham Lincoln"),
          Employee.CURRENT.newCell(false),
          Employee.TELEPHONE.newCell(911L)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Record<Integer> rec = employeeWriterReader.get(empID).get();
      assertThat(rec, containsInAnyOrder(cells));
      assertTrue(rec.getKey().equals(empID));

      rec = employeeReader.get(empID).get();
      assertThat(rec, containsInAnyOrder(cells));
      assertTrue(rec.getKey().equals(empID));
    }
  }

  @Test
  public void getNonExistingRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.get(empID).isPresent());
      assertFalse(employeeReader.get(empID).isPresent());
    }
  }

  @Test
  public void onReadNonExistingRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.on(empID).read(Employee.NAME.value()).isPresent());
      assertFalse(employeeWriterReader.on(empID).read(rec -> rec.getKey()).isPresent());

      assertFalse(employeeReader.on(empID).read(Employee.NAME.value()).isPresent());
      assertFalse(employeeReader.on(empID).read(rec -> rec.getKey()).isPresent());
    }
  }

  @Test
  public void onReadExistingRecordMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Andrew Johnson"),
          Employee.GENDER.newCell('M')
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertEquals("Andrew Johnson", employeeWriterReader.on(empID).read(Employee.NAME.value()).get().get());
      assertEquals('M', (char) employeeWriterReader.on(empID).read(Employee.GENDER.value()).get().get());
      assertFalse(employeeWriterReader.on(empID).read(Employee.SALARY.value()).get().isPresent());
      assertEquals(empID, (int) employeeWriterReader.on(empID).read(rec -> rec.getKey()).get());

      assertEquals("Andrew Johnson", employeeReader.on(empID).read(Employee.NAME.value()).get().get());
      assertEquals('M', (char) employeeReader.on(empID).read(Employee.GENDER.value()).get().get());
      assertFalse(employeeReader.on(empID).read(Employee.SALARY.value()).get().isPresent());
      assertEquals(empID, (int) employeeReader.on(empID).read(rec -> rec.getKey()).get());
    }
  }

  @Test
  public void onIffReadNonExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.on(empID).iff(Employee.CURRENT.value().is(true)).read().isPresent());
      assertFalse(employeeWriterReader.on(empID).iff(rec -> rec.getKey().equals(empID)).read().isPresent());

      assertFalse(employeeReader.on(empID).iff(Employee.CURRENT.value().is(true)).read().isPresent());
      assertFalse(employeeReader.on(empID).iff(rec -> rec.getKey().equals(empID)).read().isPresent());
    }
  }

  @Test
  public void onIffReadNonQualifying() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Ulysses S. Grant"),
          Employee.SSN.newCell(97856),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertFalse(employeeWriterReader.on(empID)
          .iff(Employee.NAME.value().is("Rutherford B. Hayes"))
          .read().isPresent());
      assertFalse(employeeWriterReader.on(empID)
          .iff(rec -> rec.getKey().equals((empID + 1)))
          .read().isPresent());

      assertFalse(employeeReader.on(empID)
          .iff(Employee.NAME.value().is("Rutherford B. Hayes"))
          .read().isPresent());
      assertFalse(employeeReader.on(empID)
          .iff(rec -> rec.getKey().equals((empID + 1)))
          .read().isPresent());
    }
  }

  @Test
  public void onIffReadQualifying() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("James Abram Garfield"),
          Employee.GENDER.newCell('M'),
          Employee.TELEPHONE.newCell(112L),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertTrue(employeeWriterReader.on(empID)
          .iff(Employee.TELEPHONE.value().is(112L))
          .read().isPresent());
      assertThat(employeeWriterReader.on(empID)
              .iff(Employee.GENDER.value().is('M'))
              .read().get()
          , containsInAnyOrder(cells));
      assertTrue(employeeWriterReader.on(empID)
          .iff(rec -> rec.getKey().equals(empID))
          .read().isPresent());

      assertTrue(employeeReader.on(empID)
          .iff(Employee.TELEPHONE.value().is(112L))
          .read().isPresent());
      assertThat(employeeReader.on(empID)
              .iff(Employee.GENDER.value().is('M'))
              .read().get()
          , containsInAnyOrder(cells));
      assertTrue(employeeReader.on(empID)
          .iff(rec -> rec.getKey().equals(empID))
          .read().isPresent());
    }
  }

  @Test
  public void onIffReadMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Chester Alan Arthur"),
          Employee.GENDER.newCell('M'),
          Employee.SALARY.newCell(99999D),
          Employee.CURRENT.newCell(true)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertTrue(employeeWriterReader.on(empID)
          .iff(Employee.NAME.value().is("Chester Alan Arthur"))
          .read(Employee.SALARY.value())
          .get().get().equals(99999D));
      assertEquals(empID, employeeWriterReader.on(empID)
          .iff(Employee.GENDER.value().is('M'))
          .read(rec -> rec.getKey()).get().intValue());
      assertEquals(100000D, employeeWriterReader.on(empID)
          .iff(Employee.SALARY.value().is(99999D))
          .read(rec -> (rec.get(Employee.SALARY).get().doubleValue() + 1))
          .get().doubleValue(), 1e-8D);

      assertTrue(employeeReader.on(empID)
          .iff(Employee.NAME.value().is("Chester Alan Arthur"))
          .read(Employee.SALARY.value())
          .get().get().equals(99999D));
      assertEquals(empID, employeeReader.on(empID)
          .iff(Employee.GENDER.value().is('M'))
          .read(rec -> rec.getKey()).get().intValue());
      assertEquals(100000D, employeeReader.on(empID)
          .iff(Employee.SALARY.value().is(99999D))
          .read(rec -> (rec.get(Employee.SALARY).get().doubleValue() + 1))
          .get().doubleValue(), 1e-8D);
    }
  }
}
