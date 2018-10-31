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
package com.terracottatech.store.transactions.impl;

import com.terracottatech.store.CellSet;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.transactions.api.TransactionController.ReadWriteTransaction;
import org.junit.Test;

import java.util.Optional;

import static com.terracottatech.store.common.test.Employee.BONUS;
import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.SSN;
import static com.terracottatech.store.common.test.Employee.TELEPHONE;
import static com.terracottatech.store.transactions.impl.TransactionOperation.ADD;
import static com.terracottatech.store.transactions.impl.TransactionOperation.DELETE;
import static com.terracottatech.store.transactions.impl.TransactionOperation.UPDATE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

@SuppressWarnings("deprecation")
public class TransactionalReadWriteRecordAccessorIT extends TransactionsTests {

  @Test
  public void addTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertFalse(employeeTWR1.on(1).add(cells).isPresent());
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells, CellSet.of());
    assertFalse(employeeTWR2.on(1).read(r -> r).isPresent());

    transaction1.commit();

    assertThat(employeeTWR2.on(1).iff(r -> true).read().get(), containsInAnyOrder(cells.toArray()));
    assertFalse(employeeTWR2.on(1).iff(r -> false).read().isPresent());

    transaction2.commit();
  }

  @Test
  public void insertAlreadyExistingTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.on(1).add(cells);

    transaction1.commit();

    assertThat(employeeTWR2.on(1).add(TELEPHONE.newCell(100L)).get(), containsInAnyOrder(cells.toArray()));

    assertThat(employeeTWR2.on(1).iff(SSN.intValueOrFail().is(100)).read().get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();
  }

  @Test
  public void insertOnTopOfSelfInsertedTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);
    assertThat(employeeTWR1.on(1).add(TELEPHONE.newCell(100L)).get(), containsInAnyOrder(cells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells, CellSet.of());

    transaction1.commit();

    assertThat(employeeTWR2.on(1).read(NAME.valueOrFail()).get(), is("Rahul"));

    transaction2.commit();
  }

  @Test
  public void insertOnTopOfSelfDeletedTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    employeeTWR2.delete(1);
    assertFalse(employeeTWR2.on(1).add(TELEPHONE.newCell(100L)).isPresent());
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, CellSet.of(TELEPHONE.newCell(100L)), cells);

    assertThat(employeeTWR2.on(1).iff(TELEPHONE.exists()).read().get(), containsInAnyOrder(TELEPHONE.newCell(100L)));

    assertThat(employeeTWR3.on(1).read(r -> r).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.on(1).iff(r -> true).read().get(), containsInAnyOrder(TELEPHONE.newCell(100L)));

    transaction3.commit();
  }

  @Test
  public void deleteTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    assertThat(employeeTWR2.on(1).delete().get(), containsInAnyOrder(cells.toArray()));
    assertFalse(employeeTWR2.on(1).update(UpdateOperation.install(BONUS.newCell(100D), SALARY.newCell(100000D))).isPresent());
    assertFalse(employeeTWR2.on(1).delete().isPresent());

    assertFalse(employeeTWR2.get(1).isPresent());
    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertFalse(employeeTWR3.get(1).isPresent());
    transaction3.commit();
  }

  @Test
  public void deleteWithTruePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    assertThat(employeeTWR2.on(1).iff(r -> true).delete(r -> r).get(), containsInAnyOrder(cells.toArray()));
    assertFalse(employeeTWR2.on(1).iff(r -> true).update(UpdateOperation.install(BONUS.newCell(100D), SALARY.newCell(100000D))).isPresent());
    assertFalse(employeeTWR2.on(1).iff(r -> true).delete().isPresent());

    assertFalse(employeeTWR2.get(1).isPresent());
    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertFalse(employeeTWR3.get(1).isPresent());
    transaction3.commit();
  }

  @Test
  public void deleteWithFalsePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    assertFalse(employeeTWR2.on(1).iff(r -> false).delete().isPresent());

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(cells.toArray()));
    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));
    transaction3.commit();
  }

  @Test
  public void updateDeleteReadNonExisting() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertFalse(employeeTWR1.on(1).update(UpdateOperation.install(BONUS.newCell(100D))).isPresent());
    assertFalse(employeeTWR1.on(1).iff(r -> true).update(UpdateOperation.install(BONUS.newCell(100D))).isPresent());
    assertFalse(employeeTWR1.on(1).iff(r -> false).update(UpdateOperation.install(BONUS.newCell(100D))).isPresent());
    assertFalse(employeeTWR1.on(1).delete().isPresent());
    assertFalse(employeeTWR1.on(1).iff(r -> true).delete().isPresent());
    assertFalse(employeeTWR1.on(1).iff(r -> false).delete().isPresent());
    assertFalse(employeeWR.on(1).read(r -> r).isPresent());
    assertFalse(employeeWR.on(1).iff(r -> true).read().isPresent());

    transaction1.commit();

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction2.commit();
  }

  @Test
  public void deleteOnTopOfSelfInsertedTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    assertThat(employeeTWR1.on(1).delete().get(), containsInAnyOrder(cells.toArray()));
    assertFalse(employeeTWR2.get(1).isPresent());
    assertFalse(employeeWR.get(1).isPresent());
    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction2.commit();
  }

  @Test
  public void deleteOnTopOfSelfInsertedWithTruePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    assertThat(employeeTWR1.on(1).iff(r -> true).delete().get(), containsInAnyOrder(cells.toArray()));
    assertFalse(employeeTWR2.get(1).isPresent());
    assertFalse(employeeWR.get(1).isPresent());
    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction2.commit();
  }

  @Test
  public void deleteOnTopOfSelfInsertedWithFalsePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    assertFalse(employeeTWR1.on(1).iff(r -> false).delete().isPresent());
    assertThat(employeeTWR1.get(1).get(), containsInAnyOrder(cells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells, CellSet.of());
    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();
  }

  @Test
  public void deleteOnTopOfSelfUpdatedTest()  throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR2.on(1).update(UpdateOperation.install(newCells));

    assertThat(employeeTWR2.on(1).delete(r -> r).get(), containsInAnyOrder(newCells.toArray()));

    assertFalse(employeeTWR2.get(1).isPresent());
    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertFalse(employeeTWR3.get(1).isPresent());
    transaction3.commit();
  }

  @Test
  public void deleteOnTopOfSelfUpdatedWithTruePredicateTest()  throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR2.on(1).update(UpdateOperation.install(newCells));

    assertThat(employeeTWR2.on(1).iff(r -> true).delete().get(), containsInAnyOrder(newCells.toArray()));

    assertFalse(employeeTWR2.get(1).isPresent());
    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertFalse(employeeTWR3.get(1).isPresent());
    transaction3.commit();
  }

  @Test
  public void deleteOnTopOfSelfUpdatedWithFalsePredicateTest()  throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR2.on(1).update(UpdateOperation.install(newCells));

    assertFalse(employeeTWR2.on(1).iff(r -> false).delete(r -> r).isPresent());

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells.toArray()));
    transaction3.commit();
  }

  @Test
  public void updateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR2.on(1).update(UpdateOperation.install(newCells));
    assertThat(updateOutput.get().getFirst(), containsInAnyOrder(cells.toArray()));
    assertThat(updateOutput.get().getSecond(), containsInAnyOrder(newCells.toArray()));
    assertThat(employeeTWR2.on(1).add(TELEPHONE.newCell(100L)).get(), containsInAnyOrder(newCells.toArray()));

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells.toArray()));
    transaction3.commit();
  }

  @Test
  public void updateWithTruePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR2.on(1).iff(r -> true).update(UpdateOperation.install(newCells));
    assertThat(updateOutput.get().getFirst(), containsInAnyOrder(cells.toArray()));
    assertThat(updateOutput.get().getSecond(), containsInAnyOrder(newCells.toArray()));
    assertThat(employeeTWR2.on(1).add(TELEPHONE.newCell(100L)).get(), containsInAnyOrder(newCells.toArray()));

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells.toArray()));
    transaction3.commit();
  }

  @Test
  public void updateWithFalsePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR2.on(1).iff(r -> false).update(UpdateOperation.install(newCells));
    assertFalse(updateOutput.isPresent());

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(cells.toArray()));

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));
    transaction3.commit();
  }

  @Test
  public void updateOnTopOfSelfInsertedTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    CellSet newCells = new CellSet();
    newCells.add(CITY_ADDRESS.newCell("Pune"));
    newCells.add(COUNTRY_ADDRESS.newCell("India"));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR1.on(1).update(UpdateOperation.install(newCells));
    assertThat(updateOutput.get().getFirst(), containsInAnyOrder(cells.toArray()));
    assertThat(updateOutput.get().getSecond(), containsInAnyOrder(newCells.toArray()));

    assertThat(employeeTWR1.get(1).get(), containsInAnyOrder(newCells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, newCells, CellSet.of());

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells.toArray()));
    transaction2.commit();
  }

  @Test
  public void updateOnTopOfSelfInsertedWithTruePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    CellSet newCells = new CellSet();
    newCells.add(CITY_ADDRESS.newCell("Pune"));
    newCells.add(COUNTRY_ADDRESS.newCell("India"));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR1.on(1).iff(r -> true).update(UpdateOperation.install(newCells));
    assertThat(updateOutput.get().getFirst(), containsInAnyOrder(cells.toArray()));
    assertThat(updateOutput.get().getSecond(), containsInAnyOrder(newCells.toArray()));

    assertThat(employeeTWR1.get(1).get(), containsInAnyOrder(newCells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, newCells, CellSet.of());

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells.toArray()));
    transaction2.commit();
  }

  @Test
  public void updateOnTopOfSelfInsertedWithFalsePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    CellSet newCells = new CellSet();
    newCells.add(CITY_ADDRESS.newCell("Pune"));
    newCells.add(COUNTRY_ADDRESS.newCell("India"));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR1.on(1).iff(r -> false).update(UpdateOperation.install(newCells));
    assertFalse(updateOutput.isPresent());

    assertThat(employeeTWR1.get(1).get(), containsInAnyOrder(cells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells, CellSet.of());

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(cells.toArray()));
    transaction2.commit();
  }

  @Test
  public void updateOnTopOfSelfUpdatedTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells1 = new CellSet();
    newCells1.add(CITY_ADDRESS.newCell("Pune"));
    newCells1.add(COUNTRY_ADDRESS.newCell("India"));
    employeeTWR2.update(1, UpdateOperation.install(newCells1));

    CellSet newCells2 = new CellSet();
    newCells2.add(CITY_ADDRESS.newCell("London"));
    newCells2.add(COUNTRY_ADDRESS.newCell("England"));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR2.on(1).update(UpdateOperation.install(newCells2));
    assertThat(updateOutput.get().getFirst(), containsInAnyOrder(newCells1.toArray()));
    assertThat(updateOutput.get().getSecond(), containsInAnyOrder(newCells2.toArray()));

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells2.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells2, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells2.toArray()));
    transaction3.commit();
  }

  @Test
  public void updateOnTopOfSelfUpdatedWithTruePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells1 = new CellSet();
    newCells1.add(CITY_ADDRESS.newCell("Pune"));
    newCells1.add(COUNTRY_ADDRESS.newCell("India"));
    employeeTWR2.update(1, UpdateOperation.install(newCells1));

    CellSet newCells2 = new CellSet();
    newCells2.add(CITY_ADDRESS.newCell("London"));
    newCells2.add(COUNTRY_ADDRESS.newCell("England"));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR2.on(1).iff(r -> true).update(UpdateOperation.install(newCells2));
    assertThat(updateOutput.get().getFirst(), containsInAnyOrder(newCells1.toArray()));
    assertThat(updateOutput.get().getSecond(), containsInAnyOrder(newCells2.toArray()));

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells2.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells2, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells2.toArray()));
    transaction3.commit();
  }

  @Test
  public void updateOnTopOfSelfUpdatedWithFalsePredicateTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeTWR1.add(1, cells);

    transaction1.commit();

    CellSet newCells1 = new CellSet();
    newCells1.add(CITY_ADDRESS.newCell("Pune"));
    newCells1.add(COUNTRY_ADDRESS.newCell("India"));
    employeeTWR2.update(1, UpdateOperation.install(newCells1));

    CellSet newCells2 = new CellSet();
    newCells2.add(CITY_ADDRESS.newCell("London"));
    newCells2.add(COUNTRY_ADDRESS.newCell("England"));
    Optional<Tuple<Record<Integer>, Record<Integer>>> updateOutput =
      employeeTWR2.on(1).iff(r -> false).update(UpdateOperation.install(newCells2));
    assertFalse(updateOutput.isPresent());

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells1.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells1, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells1.toArray()));
    transaction3.commit();
  }
}
