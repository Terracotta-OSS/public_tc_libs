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
import com.terracottatech.store.transactions.api.TransactionController;
import org.junit.Test;

import static com.terracottatech.store.common.test.Employee.BONUS;
import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.SSN;
import static com.terracottatech.store.common.test.Employee.TELEPHONE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class PostCommitRollbackIT extends TransactionsTests {

  @Test
  public void insertPostCommitTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertTrue(employeeTWR1.add(1, cells));

    transaction1.commit();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void insertOnTopOfSelfDeletedPostCommitTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.delete(1);
    CellSet cells1 = CellSet.of(TELEPHONE.newCell(100L));
    assertTrue(employeeTWR1.add(1, cells1));

    transaction1.commit();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(cells1.toArray()));
  }

  @Test
  public void deletePostCommitTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.delete(1);

    transaction1.commit();

    assertFalse(employeeR.get(1).isPresent());
  }

  @Test
  public void deleteOnTopOfSelfInsertedPostCommitTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertTrue(employeeTWR1.add(1, cells));
    employeeTWR1.delete(1);

    transaction1.commit();

    assertFalse(employeeR.get(1).isPresent());
  }

  @Test
  public void deleteOnTopOfSelfUpdatedPostCommitTest()  throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR1.update(1, UpdateOperation.install(newCells));
    employeeTWR1.delete(1);

    transaction1.commit();

    assertFalse(employeeR.get(1).isPresent());
  }

  @Test
  public void updatePostCommitTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR1.update(1, UpdateOperation.install(newCells));

    transaction1.commit();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(newCells.toArray()));
  }

  @Test
  public void updateOnTopOfSelfInsertedPostCommitTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertTrue(employeeTWR1.add(1, cells));

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR1.update(1, UpdateOperation.install(newCells));

    transaction1.commit();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(newCells.toArray()));
  }

  @Test
  public void updateOnTopOfSelfUpdatedPostCommitTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet newCells1 = new CellSet();
    newCells1.add(CITY_ADDRESS.newCell("Pune"));
    newCells1.add(COUNTRY_ADDRESS.newCell("India"));
    employeeTWR1.update(1, UpdateOperation.install(newCells1));

    CellSet newCells2 = new CellSet();
    newCells2.add(CITY_ADDRESS.newCell("London"));
    newCells2.add(COUNTRY_ADDRESS.newCell("England"));
    assertTrue(employeeTWR1.update(1, UpdateOperation.install(newCells2)));

    transaction1.commit();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(newCells2.toArray()));
  }

  @Test
  public void insertPostRollbackTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertTrue(employeeTWR1.add(1, cells));

    transaction1.rollback();

    assertFalse(employeeR.get(1).isPresent());
  }

  @Test
  public void insertOnTopOfSelfDeletedPostRollbackTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.delete(1);
    CellSet cells1 = CellSet.of(TELEPHONE.newCell(100L));
    assertTrue(employeeTWR1.add(1, cells1));

    transaction1.rollback();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void deletePostRollbackTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    employeeTWR1.delete(1);

    transaction1.rollback();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void deleteOnTopOfSelfInsertedPostRollbackTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertTrue(employeeTWR1.add(1, cells));
    employeeTWR1.delete(1);

    transaction1.rollback();

    assertFalse(employeeR.get(1).isPresent());
  }

  @Test
  public void deleteOnTopOfSelfUpdatedPostRollbackTest()  throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR1.update(1, UpdateOperation.install(newCells));
    employeeTWR1.delete(1);

    transaction1.rollback();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void updatePostRollbackTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR1.update(1, UpdateOperation.install(newCells));

    transaction1.rollback();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void updateOnTopOfSelfInsertedPostRollbackTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    assertTrue(employeeTWR1.add(1, cells));

    CellSet newCells = new CellSet();
    newCells.add(BONUS.newCell(100D));
    newCells.add(SALARY.newCell(100000D));
    employeeTWR1.update(1, UpdateOperation.install(newCells));

    transaction1.rollback();

    assertFalse(employeeR.get(1).isPresent());
  }

  @Test
  public void updateOnTopOfSelfUpdatedPostRollbackTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    CellSet newCells1 = new CellSet();
    newCells1.add(CITY_ADDRESS.newCell("Pune"));
    newCells1.add(COUNTRY_ADDRESS.newCell("India"));
    employeeTWR1.update(1, UpdateOperation.install(newCells1));

    CellSet newCells2 = new CellSet();
    newCells2.add(CITY_ADDRESS.newCell("London"));
    newCells2.add(COUNTRY_ADDRESS.newCell("England"));
    assertTrue(employeeTWR1.update(1, UpdateOperation.install(newCells2)));

    transaction1.rollback();

    assertThat(employeeR.get(1).get(), containsInAnyOrder(cells.toArray()));
  }
}
