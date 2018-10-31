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
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import com.terracottatech.store.common.test.Employee;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Optional;

import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class UpdateOperationIT extends CRUDTests {

  @Test
  public void nonExistingRecordReturnsFalse() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader.update(empID,
          UpdateOperation.install(Employee.NAME.newCell("Woodrow Wilson"))));
    }
  }

  @Test
  public void existingRecordReturnsTrue() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Warren Gamaliel Harding"),
          Employee.TELEPHONE.newCell(100L),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Calvin Coolidge"),
          Employee.GENDER.newCell('M')
      };

      assertTrue(employeeWriterReader.update(empID, UpdateOperation.install(newCells)));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

  @Test
  public void onUpdateNonExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      assertFalse(employeeWriterReader
          .on(empID)
          .update(UpdateOperation.install(
              Employee.NAME.newCell("Herbert Clark Hoover")))
          .isPresent());
    }
  }

  @Test
  public void onUpdateExisting() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Franklin Delano Roosevelt"),
          Employee.TELEPHONE.newCell(911L),
          Employee.CURRENT.newCell(false)

      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Harry S. Truman"),
          Employee.GENDER.newCell('M')
      };

      Optional<Tuple<Record<Integer>, Record<Integer>>> retVal
          = employeeWriterReader
          .on(empID)
          .update(UpdateOperation.install(newCells));

      assertTrue(retVal.isPresent());
      assertThat(retVal.get().getFirst(), containsInAnyOrder(cells));
      assertThat(retVal.get().getSecond(), containsInAnyOrder(newCells));
      assertTrue(retVal.get().getFirst().getKey().equals(empID));
      assertTrue(retVal.get().getSecond().getKey().equals(empID));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

  @Test
  public void existingOnUpdateBiMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Dwight David Eisenhower"),
          Employee.SALARY.newCell(50000.50D),
          Employee.GENDER.newCell('M')
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("John Fitzgerald Kennedy"),
          Employee.SALARY.newCell(10000.10D)
      };

      Optional<Double> sum
          = employeeWriterReader
          .on(empID)
          .update(
              UpdateOperation.install(newCells),
              ((oldRec, newRec) -> (oldRec.get(Employee.SALARY).get().doubleValue()
                  + newRec.get(Employee.SALARY).get().doubleValue())));

      assertEquals(60000.60D, sum.get().doubleValue(), 1e-8D);
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

  @Test
  public void existingOnUpdateBiMapperCellDefinitionAbsent() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Dwight David Eisenhower Jr"),
          Employee.SALARY.newCell(50000.50D),
          Employee.GENDER.newCell('M')
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("John Fitzgerald Kennedy Jr"),
          Employee.SALARY.newCell(10000.10D)
      };

      Optional<Boolean> ssnBool
          = employeeWriterReader
          .on(empID)
          .update(
              UpdateOperation.install(newCells),
              ((oldRec, newRec) -> (oldRec.get(Employee.SSN).isPresent()
                  || newRec.get(Employee.SSN).isPresent())));

      assertTrue(ssnBool.get().equals(false));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

  @Test
  public void nonExistingOnUpdateBiMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Lyndon Baines Johnson"),
          Employee.SALARY.newCell(50000.50D),
          Employee.GENDER.newCell('M')
      };

      Optional<Double> sum = employeeWriterReader
          .on(empID)
          .update(
              UpdateOperation.install(cells),
              ((oldRec, newRec) -> (oldRec.get(Employee.SALARY).get().doubleValue()
                  + newRec.get(Employee.SALARY).get().doubleValue())));

      assertFalse(sum.isPresent());
    }
  }

  @Test
  public void existingOnIffNonQualifyingUpdate() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Richard Milhous Nixon"),
          Employee.GENDER.newCell('M'),
          Employee.SALARY.newCell(100000.0D),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Gerald Rudolph Ford"),
          Employee.SALARY.newCell(9999.99D)
      };

      Optional<Tuple<Record<Integer>, Record<Integer>>> retVal
          = employeeWriterReader
          .on(empID)
          .iff(rec -> rec.get(Employee.GENDER).get().equals('F'))
          .update(UpdateOperation.install(newCells));
      assertFalse(retVal.isPresent());
    }
  }

  @Test
  public void existingOnIffQualifyingUpdate() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("James Earl Carter"),
          Employee.SSN.newCell(978564),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Ronald Wilson Reagan"),
          Employee.SALARY.newCell(10099.99D)
      };

      Optional<Tuple<Record<Integer>, Record<Integer>>> retVal
          = employeeWriterReader
          .on(empID)
          .iff(rec -> rec.get(Employee.CURRENT).get().equals(false))
          .update(UpdateOperation.install(newCells));
      assertTrue(retVal.isPresent());
      assertThat(retVal.get().getFirst(), containsInAnyOrder(cells));
      assertThat(retVal.get().getSecond(), containsInAnyOrder(newCells));
      assertTrue(retVal.get().getFirst().getKey().equals(empID));
      assertTrue(retVal.get().getSecond().getKey().equals(empID));
    }
  }

  @Test
  public void nonExistingOnIffUpdate() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      int empID = generateUniqueEmpID();

      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("George Herbert Walker Bush"),
          Employee.GENDER.newCell('M'),
          Employee.TELEPHONE.newCell(911L)
      };

      Optional<Tuple<Record<Integer>, Record<Integer>>> retVal
          = employeeWriterReader
          .on(empID)
          .iff(rec -> rec.get(Employee.GENDER).get().equals('M'))
          .update(UpdateOperation.install(cells));

      assertFalse(retVal.isPresent());
    }
  }

  @Test
  public void onIffUpdateBiMapper() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("William Jefferson Clinton"),
          Employee.SALARY.newCell(-99.99D),
          Employee.GENDER.newCell('M')
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("George Walker Bush"),
          Employee.SALARY.newCell(-0.01D)
      };

      Optional<Double> sum
          = employeeWriterReader
          .on(empID)
          .iff(rec -> rec.getKey().equals(empID))
          .update(
              UpdateOperation.install(newCells),
              ((oldRec, newRec) -> (oldRec.get(Employee.SALARY).get().doubleValue()
                  + newRec.get(Employee.SALARY).get().doubleValue())));

      assertEquals(-100D, sum.get().doubleValue(), 1e-8D);
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

  @Test
  public void removeUpdateOperation() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Leonardo DiCaprio"),
          Employee.GENDER.newCell('M'),
          Employee.SALARY.newCell(1000000.0D)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> remainingCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Leonardo DiCaprio"),
          Employee.GENDER.newCell('M')
      };

      assertTrue(employeeWriterReader.update(empID, UpdateOperation.remove(Employee.SALARY)));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(remainingCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(remainingCells));
    }
  }

  @Test
  public void removeNonExistingCell() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Leonardo DiCaprio Jr"),
          Employee.GENDER.newCell('M'),
          Employee.SALARY.newCell(1000000.0D)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      assertTrue(employeeWriterReader.update(empID, UpdateOperation.remove(Employee.TELEPHONE)));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void customUpdateOperation() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Eddie Redmayne"),
          Employee.CURRENT.newCell(true),
          Employee.TELEPHONE.newCell(9999L)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Eddie Redmayne"),
          Employee.CURRENT.newCell(false),
          Employee.TELEPHONE.newCell(10000L)
      };

      assertTrue(employeeWriterReader.update(empID, UpdateOperation.custom(
          (rec -> {
            CellSet retCells = new CellSet();

            retCells.add(Employee.NAME.newCell(rec.get(Employee.NAME).get()));

            if (rec.get(Employee.SSN).isPresent()) {
              retCells.add(Employee.SSN.newCell(rec.get(Employee.SSN).get()));
            }

            if (rec.get(Employee.CURRENT).get().equals(true)) {
              retCells.add(Employee.CURRENT.newCell(false));
            }

            retCells.add(Employee.TELEPHONE.newCell(rec.get(Employee.TELEPHONE).get() + 1));
            return retCells;
          }))));

      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

  @Test
  public void allOfUpdateOperation() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Matthew McConaughey"),
          Employee.SSN.newCell(1234567),
          Employee.TELEPHONE.newCell(Long.MAX_VALUE),
          Employee.SALARY.newCell(150000.87d),
          Employee.CURRENT.newCell(true),
          Employee.SIGNATURE.newCell(new byte[]{1, 2, 3})
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      Cell<?> newCells[] = new Cell<?>[]{
          Employee.NAME.newCell("Matthew McConaughey"),
          Employee.SSN.newCell(7654321),
          Employee.TELEPHONE.newCell(Long.MAX_VALUE),
          Employee.SALARY.newCell(150000.87d),
          Employee.CURRENT.newCell(true)
      };

      assertTrue(employeeWriterReader.update(empID, UpdateOperation.allOf(
          UpdateOperation.write(Employee.SSN.newCell(7654321)),
          UpdateOperation.remove(Employee.SIGNATURE)
      )));

      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells));
    }
  }

  @Test
  public void writeCell() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Daniel Day-Lewis"),
          Employee.TELEPHONE.newCell(100L),
          Employee.CURRENT.newCell(false)

      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      ArrayList<Cell<?>> newCells = new ArrayList<>();
      newCells.add(Employee.NAME.newCell("Jean Dujardin"));
      newCells.add(Employee.CURRENT.newCell(false));
      newCells.add(Employee.TELEPHONE.newCell(101L));

      assertTrue(employeeWriterReader
          .update(empID, UpdateOperation.write(Employee.NAME.newCell("Jean Dujardin"))));

      assertTrue(employeeWriterReader
          .update(empID, UpdateOperation.write(Employee.TELEPHONE.newCell(101L))));

      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells.toArray()));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells.toArray()));
    }
  }

  @Test
  public void writeNonExistingCell() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Daniel Day-Lewis Jr"),
          Employee.TELEPHONE.newCell(100L),
          Employee.CURRENT.newCell(false)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      ArrayList<Cell<?>> newCells = new ArrayList<>();
      newCells.add(Employee.NAME.newCell("Daniel Day-Lewis Jr"));
      newCells.add(Employee.CURRENT.newCell(false));
      newCells.add(Employee.TELEPHONE.newCell(100L));
      newCells.add(Employee.SSN.newCell(100));

      assertTrue(employeeWriterReader.update(empID, UpdateOperation.write(Employee.SSN.newCell(100))));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(newCells.toArray()));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(newCells.toArray()));
    }
  }

  @Test
  public void writeCellNameValue() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Colin Firth"),
          Employee.CURRENT.newCell(true),
          Employee.GENDER.newCell('M')
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      cells[1] = Employee.CURRENT.newCell(false);
      assertTrue(employeeWriterReader.update(empID, UpdateOperation.write("current", false)));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));

      cells[2] = Employee.GENDER.newCell('F');
      assertTrue(employeeWriterReader.update(empID, UpdateOperation.write("gender", 'F')));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void writeCellDefinitionFunction() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?>[] cells = new Cell<?>[]{
          Employee.NAME.newCell("Jeff Bridges"),
          Employee.CURRENT.newCell(true),
          Employee.SSN.newCell(100),
          Employee.TELEPHONE.newCell(200L),
          Employee.SALARY.newCell(300D)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      cells[1] = Employee.CURRENT.newCell(false);
      assertTrue(employeeWriterReader
          .update(empID,
              UpdateOperation
                  .write(Employee.CURRENT).resultOf(rec -> !rec.get("current").get().equals(true))));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));

      cells[2] = Employee.SSN.newCell(101);
      assertTrue(employeeWriterReader
          .update(empID,
              UpdateOperation
                  .write(Employee.SSN).intResultOf(
                  rec -> {
                    if (rec.get(Employee.SALARY).get().compareTo(200D) > 0) {
                      return 101;
                    } else {
                      return 1001;
                    }
                  })));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));

      cells[3] = Employee.TELEPHONE.newCell(201L);
      assertTrue(employeeWriterReader
          .update(empID,
              UpdateOperation
                  .write(Employee.TELEPHONE).longResultOf(rec -> (rec.get(Employee.TELEPHONE).get() + 1L))));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));

      cells[4] = Employee.SALARY.newCell(301.10D);
      assertTrue(employeeWriterReader
          .update(empID,
              UpdateOperation
                  .write(Employee.SALARY).doubleResultOf(rec -> (rec.get(Employee.SALARY).get() + 1.10D))));
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void inputOutputUpdateOperation() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?> cells[] = new Cell<?>[]{
          Employee.NAME.newCell("Sean Penn"),
          Employee.TELEPHONE.newCell(911L),
          Employee.SALARY.newCell(50000.50D)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      cells[1] = Employee.TELEPHONE.newCell(112L);
      assertTrue(
          employeeWriterReader
              .on(empID)
              .update(UpdateOperation.write("telephone", 112L), UpdateOperation.input(Employee.TELEPHONE.value()))
              .get()
              .get()
              .equals(911L)
      );
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));

      cells[2] = Employee.SALARY.newCell(0.0D);
      assertTrue(
          employeeWriterReader
              .on(empID)
              .update(UpdateOperation.write("salary", 0.0D), UpdateOperation.output(Employee.SALARY.value()))
              .get()
              .get()
              .equals(0.0D)
      );
      assertThat(employeeWriterReader.get(empID).get(), containsInAnyOrder(cells));
      assertThat(employeeReader.get(empID).get(), containsInAnyOrder(cells));
    }
  }

  @Test
  public void UpdateOperationWriteWithNonIntrinsicPredicateAsArgument() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?>[] cells = new Cell<?>[]{
          Employee.NAME.newCell("A Raja"),
          Employee.CURRENT.newCell(false),
          Employee.SSN.newCell(100),
          Employee.TELEPHONE.newCell(200L),
          Employee.SALARY.newCell(300D)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      employeeWriterReader.update(empID, UpdateOperation.write(Employee.CURRENT).boolResultOf(rec -> true));

      assertThat(employeeReader.get(empID).get().get(Employee.CURRENT).get(), is(true));
    }
  }

  @Test
  public void UpdateOperationWriteWithIntrinsicPredicateAsArgument() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Cell<?>[] cells = new Cell<?>[]{
          Employee.NAME.newCell("A Raja"),
          Employee.CURRENT.newCell(false),
          Employee.SSN.newCell(100),
          Employee.TELEPHONE.newCell(200L),
          Employee.SALARY.newCell(300D)
      };

      int empID = generateUniqueEmpID();
      employeeWriterReader.add(empID, cells);

      employeeWriterReader.update(empID, UpdateOperation.write(Employee.CURRENT).boolResultOf(Employee.NAME.exists()));

      assertThat(employeeReader.get(empID).get().get(Employee.CURRENT).get(), is(true));
    }
  }
}
