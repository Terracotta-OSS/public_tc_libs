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
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.transactions.api.TransactionController;
import org.junit.Test;

import static com.terracottatech.store.common.test.Employee.BONUS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.SSN;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class TransactionalStreamIT extends TransactionsTests {

  @Test
  public void transactionalReadStreamWithCommitTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact()
      .using("employeeR", this.employeeR)
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetReader<Integer> employeeTR2 = transaction2.reader("employeeR");
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    TransactionController.ReadOnlyTransaction transaction3
      = controller.transact()
      .using("employeeR", this.employeeR)
      .begin();
    DatasetReader<Integer> employeeTR3 = transaction3.reader("employeeR");

    CellSet employeeCells1 = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    CellSet employeeCells2 = CellSet.of(NAME.newCell("Ajay"), SSN.newCell(200));
    employeeTWR1.add(1, employeeCells1);
    employeeTWR1.add(2, employeeCells2);

    transaction1.commit();

    // ADD
    CellSet employeeCells3 = CellSet.of(NAME.newCell("Piyush"), SSN.newCell(300));
    employeeTWR2.add(3, employeeCells3);

    // UPDATE
    employeeTWR2.update(1, UpdateOperation.write(SSN).intResultOf(SSN.intValueOrFail().increment()));
    CellSet employeeUpdatedCells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(101));

    // DELETE
    employeeTWR2.delete(2);

    assertThat(employeeTR2.records().filter(r -> r.getKey().equals(1)).findFirst().get(),
      containsInAnyOrder(employeeUpdatedCells.toArray()));
    assertFalse(employeeTR2.records().filter(r -> r.getKey().equals(2)).findFirst().isPresent());
    assertThat(employeeTR2.records().filter(r -> r.getKey().equals(3)).findFirst().get(),
      containsInAnyOrder(employeeCells3.toArray()));
    assertTrue(employeeTR2.records().count() == 2);

    transaction2.commit();

    assertThat(employeeTR3.records().filter(r -> r.getKey().equals(1)).findFirst().get(),
      containsInAnyOrder(employeeUpdatedCells.toArray()));
    assertFalse(employeeTR3.records().filter(r -> r.getKey().equals(2)).findFirst().isPresent());
    assertThat(employeeTR3.records().filter(r -> r.getKey().equals(3)).findFirst().get(),
      containsInAnyOrder(employeeCells3.toArray()));
    assertTrue(employeeTR3.records().count() == 2);

    transaction3.commit();
  }

  @Test
  public void transactionalReadStreamWithRollbackTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact()
      .using("employeeR", this.employeeR)
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetReader<Integer> employeeTR2 = transaction2.reader("employeeR");
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");
    TransactionController.ReadOnlyTransaction transaction3
      = controller.transact()
      .using("employeeR", this.employeeR)
      .begin();
    DatasetReader<Integer> employeeTR3 = transaction3.reader("employeeR");

    CellSet employeeCells1 = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    CellSet employeeCells2 = CellSet.of(NAME.newCell("Ajay"), SSN.newCell(200));
    employeeTWR1.add(1, employeeCells1);
    employeeTWR1.add(2, employeeCells2);

    transaction1.commit();

    // ADD
    CellSet employeeCells3 = CellSet.of(NAME.newCell("Piyush"), SSN.newCell(300));
    employeeTWR2.add(3, employeeCells3);

    // UPDATE
    employeeTWR2.update(1, UpdateOperation.write(SSN).intResultOf(SSN.intValueOrFail().increment()));
    CellSet employeeUpdatedCells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(101));

    // DELETE
    employeeTWR2.delete(2);

    assertThat(employeeTR2.records().filter(r -> r.getKey().equals(1)).findFirst().get(),
      containsInAnyOrder(employeeUpdatedCells.toArray()));
    assertFalse(employeeTR2.records().filter(r -> r.getKey().equals(2)).findFirst().isPresent());
    assertThat(employeeTR2.records().filter(r -> r.getKey().equals(3)).findFirst().get(),
      containsInAnyOrder(employeeCells3.toArray()));
    assertTrue(employeeTR2.records().count() == 2);

    transaction2.rollback();

    assertThat(employeeTR3.records().filter(r -> r.getKey().equals(1)).findFirst().get(),
      containsInAnyOrder(employeeCells1.toArray()));
    assertThat(employeeTR3.records().filter(r -> r.getKey().equals(2)).findFirst().get(),
      containsInAnyOrder(employeeCells2.toArray()));
    assertFalse(employeeTR3.records().filter(r -> r.getKey().equals(3)).findFirst().isPresent());
    assertTrue(employeeTR3.records().count() == 2);

    transaction3.commit();
  }

  @Test
  public void transactionalReadWriteStreamNonMutativeOperationTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet employeeCells1 = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    CellSet employeeCells2 = CellSet.of(NAME.newCell("Ajay"), SSN.newCell(200));
    employeeTWR1.add(1, employeeCells1);
    employeeTWR1.add(2, employeeCells2);

    transaction1.commit();

    assertTrue(employeeTWR2.records().count() == 2);

    transaction2.commit();
  }

  @Test
  public void transactionalReadWriteStreamMutativeOperationTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact()
      .using("employeeWR", this.employeeWR)
      .begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet employeeCells1 = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    CellSet employeeCells2 = CellSet.of(NAME.newCell("Ajay"), SSN.newCell(200));
    employeeTWR1.add(1, employeeCells1);
    employeeTWR1.add(2, employeeCells2);

    transaction1.commit();

    try {
      employeeTWR2.records().delete();
      fail("Mutative operation on transactional streams should fail with exception");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().equals("Mutative Operations are not supported on transactional record streams"));
    }

    try {
      employeeTWR2.records().deleteThen().forEach(System.out::println);
      fail("Mutative operation on transactional streams should fail with exception");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().equals("Mutative Operations are not supported on transactional record streams"));
    }

    try {
      employeeTWR2.records().mutate(UpdateOperation.write(SALARY).value(100.01D));
      fail("Mutative operation on transactional streams should fail with exception");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().equals("Mutative Operations are not supported on transactional record streams"));
    }

    try {
      employeeTWR2.records().mutateThen(UpdateOperation.write(BONUS).value(10.101D)).forEach(System.out::println);
      fail("Mutative operation on transactional streams should fail with exception");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().equals("Mutative Operations are not supported on transactional record streams"));
    }

    assertTrue(employeeTWR2.records().count() == 2);
    assertTrue(employeeTWR2.records().filter(SALARY.exists()).count() == 0);
    assertTrue(employeeTWR2.records().filter(BONUS.exists()).count() == 0);

    transaction2.commit();

    assertTrue(employeeWR.records().count() == 2);
    assertTrue(employeeWR.records().filter(SALARY.exists()).count() == 0);
    assertTrue(employeeWR.records().filter(BONUS.exists()).count() == 0);
  }
}
