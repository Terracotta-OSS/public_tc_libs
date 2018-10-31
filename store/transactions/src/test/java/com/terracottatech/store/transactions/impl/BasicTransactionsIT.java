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
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.transactions.api.TransactionController.ReadWriteTransaction;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;
import org.junit.Test;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class BasicTransactionsIT extends TransactionsTests {

  @Test
  public void insertTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertTrue(employeeTWR1.add(1, cells));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells, CellSet.of());
    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(cells.toArray()));

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
    employeeTWR1.add(1, cells);

    transaction1.commit();

    assertFalse(employeeTWR2.add(1, TELEPHONE.newCell(100L)));

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(cells.toArray()));

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
    assertFalse(employeeTWR1.add(1, TELEPHONE.newCell(100L)));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells, CellSet.of());

    transaction1.commit();

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(cells.toArray()));

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
    assertTrue(employeeTWR2.add(1, TELEPHONE.newCell(100L)));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, CellSet.of(TELEPHONE.newCell(100L)), cells);

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(TELEPHONE.newCell(100L)));

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(TELEPHONE.newCell(100L)));

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

    assertTrue(employeeTWR2.delete(1));
    assertFalse(employeeTWR2.update(1, UpdateOperation.install(BONUS.newCell(100D), SALARY.newCell(100000D))));
    assertFalse(employeeTWR2.delete(1));

    assertFalse(employeeTWR2.get(1).isPresent());
    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertFalse(employeeTWR3.get(1).isPresent());
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

    assertFalse(employeeTWR1.update(1, UpdateOperation.install(BONUS.newCell(100D))));
    assertFalse(employeeTWR1.delete(1));
    assertFalse(employeeWR.get(1).isPresent());

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

    assertTrue(employeeTWR1.delete(1));
    assertFalse(employeeTWR2.get(1).isPresent());
    assertFalse(employeeWR.get(1).isPresent());
    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertFalse(employeeTWR2.get(1).isPresent());

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
    employeeTWR2.update(1, UpdateOperation.install(newCells));

    assertTrue(employeeTWR2.delete(1));

    assertFalse(employeeTWR2.get(1).isPresent());
    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertFalse(employeeTWR3.get(1).isPresent());
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
    assertTrue(employeeTWR2.update(1, UpdateOperation.install(newCells)));
    assertFalse(employeeTWR2.add(1, TELEPHONE.newCell(100L)));

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells.toArray()));
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
    assertTrue(employeeTWR1.update(1, UpdateOperation.install(newCells)));
    assertThat(employeeTWR1.get(1).get(), containsInAnyOrder(newCells.toArray()));
    testUncommittedEmployeeRecord(employeeWR, ADD, 1, newCells, CellSet.of());

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction1.commit();

    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells.toArray()));
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
    assertTrue(employeeTWR2.update(1, UpdateOperation.install(newCells2)));
    assertThat(employeeTWR2.get(1).get(), containsInAnyOrder(newCells2.toArray()));
    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, newCells2, cells);

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction2.commit();

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(newCells2.toArray()));
    transaction3.commit();
  }

  @Test
  public void insertRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction2.commit();
  }

  @Test
  public void deleteRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.commit();

    // delete existing and then rollback
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    employeeTWR2.delete(1);

    transaction2.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertTrue(employeeTWR3.get(1).get().get(NAME).get().equals("Rakesh"));
    assertTrue(employeeTWR3.get(1).get().stream().count() == 1);

    transaction3.commit();
  }

  @Test
  public void updateRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.commit();

    // update existing and then rollback
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    employeeTWR2.update(1, UpdateOperation.install(TELEPHONE.newCell(100L), COUNTRY_ADDRESS.newCell("USA")));

    transaction2.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertTrue(employeeTWR3.get(1).get().get(NAME).get().equals("Rakesh"));
    assertTrue(employeeTWR3.get(1).get().stream().count() == 1);

    transaction3.commit();
  }

  @Test
  public void insertDeleteRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));
    employeeTWR1.delete(1);

    transaction1.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction2.commit();
  }

  @Test
  public void insertUpdateRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));
    employeeTWR1.update(1, UpdateOperation.install(TELEPHONE.newCell(100L), COUNTRY_ADDRESS.newCell("USA")));

    transaction1.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertFalse(employeeTWR2.get(1).isPresent());

    transaction2.commit();
  }

  @Test
  public void updateDeleteRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.commit();

    // update existing and then rollback
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    employeeTWR2.update(1, UpdateOperation.install(TELEPHONE.newCell(100L), COUNTRY_ADDRESS.newCell("USA")));
    employeeTWR2.delete(1);

    transaction2.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertTrue(employeeTWR3.get(1).get().get(NAME).get().equals("Rakesh"));
    assertTrue(employeeTWR3.get(1).get().stream().count() == 1);

    transaction3.commit();
  }

  @Test
  public void updateUpdateRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.commit();

    // update existing and then rollback
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    employeeTWR2.update(1, UpdateOperation.install(TELEPHONE.newCell(100L), COUNTRY_ADDRESS.newCell("USA")));
    employeeTWR2.update(1, UpdateOperation.write(TELEPHONE).value(911L));

    transaction2.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertTrue(employeeTWR3.get(1).get().get(NAME).get().equals("Rakesh"));
    assertTrue(employeeTWR3.get(1).get().stream().count() == 1);

    transaction3.commit();
  }

  @Test
  public void deleteInsertRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.commit();

    // update existing and then rollback
    ReadWriteTransaction transaction2
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    employeeTWR2.delete(1);
    employeeTWR2.add(1, TELEPHONE.newCell(100L), COUNTRY_ADDRESS.newCell("USA"));

    transaction2.rollback();

    // Post rollback verification
    ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertTrue(employeeTWR3.get(1).get().get(NAME).get().equals("Rakesh"));
    assertTrue(employeeTWR3.get(1).get().stream().count() == 1);

    transaction3.commit();
  }

  @Test
  public void afterCommitOperationsTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.commit();

    transactionEndTest(employeeTWR1, transaction1, false);
  }

  @Test
  public void afterRollbackOperationsTest() throws Exception {
    employeeWR.add(1024, NAME.newCell("Peter"));
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.add(1, NAME.newCell("Rakesh"));

    transaction1.rollback();

    transactionEndTest(employeeTWR1, transaction1, true);
  }

  @Test
  public void multiDatasetTransactionCommitTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .using("customerWR", this.customerWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    DatasetWriterReader<String> customerTWR1 = transaction1.writerReader("customerWR");
    ReadWriteTransaction transaction2
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .using("customerWR", this.customerWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    DatasetWriterReader<String> customerTWR2 = transaction2.writerReader("customerWR");
    ReadWriteTransaction transaction3
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .using("customerWR", this.customerWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");
    DatasetWriterReader<String> customerTWR3 = transaction3.writerReader("customerWR");

    CellSet employeeCells1 = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    CellSet employeeCells2 = CellSet.of(NAME.newCell("Ajay"), SSN.newCell(200));
    employeeTWR1.add(1, employeeCells1);
    employeeTWR1.add(2, employeeCells2);

    CellSet customerCells1 = CellSet.of(CITY_ADDRESS.newCell("Pune"), COUNTRY_ADDRESS.newCell("India"));
    CellSet customerCells2 = CellSet.of(NAME.newCell("Mumbai"), COUNTRY_ADDRESS.newCell("India"));
    customerTWR1.add("Rahul", customerCells1);
    customerTWR1.add("Ajay", customerCells2);

    transaction1.commit();

    // ADD
    CellSet employeeCells3 = CellSet.of(NAME.newCell("Piyush"), SSN.newCell(300));
    CellSet customerCells3 = CellSet.of(NAME.newCell("London"), COUNTRY_ADDRESS.newCell("England"));
    employeeTWR2.add(3, employeeCells3);
    customerTWR2.add("Piyush", customerCells3);

    // UPDATE
    employeeTWR2.update(1, UpdateOperation.write(SSN).intResultOf(SSN.intValueOrFail().increment()));
    customerTWR2.update(employeeTWR2.on(1).read(NAME.valueOrFail()).get(), UpdateOperation.write(CITY_ADDRESS).resultOf(r -> "Bengaluru"));

    // DELETE
    employeeTWR2.delete(2);
    customerTWR2.delete("Ajay");

    transaction2.commit();

    CellSet employeeUpdatedCells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(101));
    CellSet customerUpdatedCells = CellSet.of(CITY_ADDRESS.newCell("Bengaluru"), COUNTRY_ADDRESS.newCell("India"));
    assertThat(employeeTWR3.on(1).read(r -> r).get(), containsInAnyOrder(employeeUpdatedCells.toArray()));
    assertThat(customerTWR3.on("Rahul").read(r -> r).get(), containsInAnyOrder(customerUpdatedCells.toArray()));

    assertFalse(employeeTWR3.get(2).isPresent());
    assertFalse(customerTWR3.get("Ajay").isPresent());

    assertThat(employeeTWR3.on(3).read(r -> r).get(), containsInAnyOrder(employeeCells3.toArray()));
    assertThat(customerTWR3.on("Piyush").read(r -> r).get(), containsInAnyOrder(customerCells3.toArray()));

    transaction3.commit();
  }

  @Test
  public void multiDatasetTransactionRollbackTest() throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .using("customerWR", this.customerWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    DatasetWriterReader<String> customerTWR1 = transaction1.writerReader("customerWR");
    ReadWriteTransaction transaction2
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .using("customerWR", this.customerWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    DatasetWriterReader<String> customerTWR2 = transaction2.writerReader("customerWR");
    ReadWriteTransaction transaction3
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .using("customerWR", this.customerWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");
    DatasetWriterReader<String> customerTWR3 = transaction3.writerReader("customerWR");

    CellSet employeeCells1 = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    CellSet employeeCells2 = CellSet.of(NAME.newCell("Ajay"), SSN.newCell(200));
    employeeTWR1.add(1, employeeCells1);
    employeeTWR1.add(2, employeeCells2);

    CellSet customerCells1 = CellSet.of(CITY_ADDRESS.newCell("Pune"), COUNTRY_ADDRESS.newCell("India"));
    CellSet customerCells2 = CellSet.of(NAME.newCell("Mumbai"), COUNTRY_ADDRESS.newCell("India"));
    customerTWR1.add("Rahul", customerCells1);
    customerTWR1.add("Ajay", customerCells2);

    transaction1.commit();

    // ADD
    CellSet employeeCells3 = CellSet.of(NAME.newCell("Piyush"), SSN.newCell(300));
    CellSet customerCells3 = CellSet.of(NAME.newCell("London"), COUNTRY_ADDRESS.newCell("England"));
    employeeTWR2.add(3, employeeCells3);
    customerTWR2.add("Piyush", customerCells3);

    // UPDATE
    employeeTWR2.update(1, UpdateOperation.write(SSN).intResultOf(SSN.intValueOrFail().increment()));
    customerTWR2.update(employeeTWR2.on(1).read(NAME.valueOrFail()).get(), UpdateOperation.write(CITY_ADDRESS).resultOf(r -> "Bengaluru"));

    // DELETE
    employeeTWR2.delete(2);
    customerTWR2.delete("Ajay");

    transaction2.rollback();

    assertThat(employeeTWR3.on(1).read(r -> r).get(), containsInAnyOrder(employeeCells1.toArray()));
    assertThat(customerTWR3.on("Rahul").read(r -> r).get(), containsInAnyOrder(customerCells1.toArray()));

    assertThat(employeeTWR3.on(2).read(r -> r).get(), containsInAnyOrder(employeeCells2.toArray()));
    assertThat(customerTWR3.on("Ajay").read(r -> r).get(), containsInAnyOrder(customerCells2.toArray()));

    assertFalse(employeeTWR3.get(3).isPresent());
    assertFalse(customerTWR3.get("Piyush").isPresent());

    transaction3.commit();
  }

  private void transactionEndTest(DatasetWriterReader<Integer> employeeTWR, ReadWriteTransaction transaction, boolean afterRollback) throws Exception {
    try {
      employeeTWR.get(1);
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      employeeTWR.add(2);
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertFalse(employeeWR.get(2).isPresent());
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      employeeTWR.update(1, UpdateOperation.custom(r->r));
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      employeeTWR.delete(1);
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      if (!afterRollback) {
        assertTrue(employeeWR.get(1).isPresent());
      }
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      employeeTWR.records().count();
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      transaction.commit();
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      transaction.rollback();
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      transaction.reader("some");
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }

    try {
      transaction.writerReader("some");
      fail("Transaction has ended still operations are running without any exception");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("transaction has already ended"));
    }
  }
}
