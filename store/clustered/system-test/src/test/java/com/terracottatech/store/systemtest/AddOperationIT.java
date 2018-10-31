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
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Optional;

import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static java.util.Optional.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Add Operation test class
 */
public class AddOperationIT extends CRUDTests {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void nullCellValue() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();

      exception.expect(new BaseMatcher<Throwable>() {
        @Override
        public boolean matches(Object item) {
          Throwable cause = (Throwable) item;
          while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
          }
          return cause instanceof NullPointerException &&
              cause.getMessage().equals("Cells collection contains null entries");
        }

        @Override
        public void describeTo(Description description) {
          description.appendText("NPE with 'Cells collection contains null entries' message should be the cause");
        }
      });

      employeeWriterReader.add(generateUniqueEmpID(),
          Employee.NAME.newCell("George Washington"),
          Employee.GENDER.newCell('M'),
          null);
    }
  }

  @Test
  public void allCellTypesSupported() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("John Adams"),
          Employee.SSN.newCell(1234567),
          Employee.TELEPHONE.newCell(Long.MAX_VALUE),
          Employee.SALARY.newCell(150000.87d),
          Employee.CURRENT.newCell(true),
          Employee.SIGNATURE.newCell(new byte[]{1, 2, 3})};

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void addEmptyRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID);

      Optional<Record<Integer>> optRecord = employeeWriterReader.get(empID);
      assertThat(optRecord.isPresent(), is(true));

      Record<Integer> rec = optRecord.get();
      assertThat(rec.getKey(), is(empID));
      assertEquals(0, rec.size());
    }
  }

  @Test
  public void addOneCellRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{Employee.NAME.newCell("Thomas Jefferson")};
      int empID = generateUniqueEmpID();

      assertTrue(employeeWriterReader.add(empID, cells));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void addMultipleCellRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("James Madison"),
          Employee.CURRENT.newCell(false),
          Employee.GENDER.newCell('M'),
          Employee.SSN.newCell(1983),
          Employee.TELEPHONE.newCell(987654321L)};
      int empID = generateUniqueEmpID();
      assertTrue(employeeWriterReader.add(empID, cells));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void addRecordWithDuplicateKey() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      Cell<?> cells[] = new Cell<?>[]{Employee.NAME.newCell("James Monroe")};
      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("John Quincy Adams"),
          Employee.CURRENT.newCell(false)};
      assertFalse(employeeWriterReader.add(empID, newCells));

    }
  }

  @Test
  public void addDuplicateCellDefInRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Andrew Jackson"),
          Employee.CURRENT.newCell(true),
          Employee.SSN.newCell(7654),
          Employee.NAME.newCell("Martin Van Buren")};

      int empID = generateUniqueEmpID();
      exception.expect(Exception.class);
      employeeWriterReader.add(empID, cells);
    }
  }

  @Test
  public void onAddReturnsEmptyOptional() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("William Henry Harrison"),
          Employee.GENDER.newCell('M')};

      int empID = generateUniqueEmpID();
      assertFalse(employeeWriterReader.on(empID).add(cells).isPresent());
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void onAddExistingRecord() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("John Tyler"),
          Employee.GENDER.newCell('M')};
      int empID = generateUniqueEmpID();
      employeeWriterReader.on(empID).add(cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("John Tyler")};
      Optional<Record<Integer>> optRec = employeeWriterReader.on(empID).add(newCells);
      assertTrue(optRec.isPresent());
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void onAddMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("James Knox Polk"),
          Employee.GENDER.newCell('M')};
      int empID = generateUniqueEmpID();
      employeeWriterReader.on(empID).add(cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Zachary Taylor")};
      Optional<String> optName = employeeWriterReader.on(empID).add(Employee.NAME.value(), newCells).orElse(of("error"));

      assertEquals("James Knox Polk", optName.get());
    }
  }

  @Test
  public void onUpsertNonExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Millard Fillmore"),
          Employee.GENDER.newCell('M'),
          Employee.CURRENT.newCell(true)};
      int empID = generateUniqueEmpID();
      employeeWriterReader.on(empID).upsert(cells);

      assertTrue(employeeWriterReader.get(empID).isPresent());
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));

      assertTrue(employeeReader.get(empID).isPresent());
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void onUpsertExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Franklin Pierce"),
          Employee.GENDER.newCell('M'),
          Employee.CURRENT.newCell(true)};
      int empID = generateUniqueEmpID();
      employeeWriterReader.on(empID).add(cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("James Buchanan"),
          Employee.SSN.newCell(92381),
          Employee.TELEPHONE.newCell(92837461L)};
      employeeWriterReader.on(empID).upsert(newCells);

      assertTrue(employeeWriterReader.get(empID).isPresent());
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));

      assertTrue(employeeReader.get(empID).isPresent());
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

}
