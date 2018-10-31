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
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SSN;
import static com.terracottatech.store.transactions.impl.TransactionOperation.ADD;
import static com.terracottatech.store.transactions.impl.TransactionOperation.DELETE;
import static com.terracottatech.store.transactions.impl.TransactionOperation.UPDATE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class ConcurrentTransactionsTimeOutIT extends TransactionsTests {

  private final int holdingtransactionsTimeOutMillis = 300;
  private final int waitingTransactionsTimeOutMillis = 1_000;

  @Test
  public void insertOnOthersUncommittedInsertTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertTrue(employeeTWR1.add(1, NAME.newCell("Rahul")));

    CellSet cells2 = CellSet.of(NAME.newCell("Ravi"), SSN.newCell(100));
    assertTrue(employeeTWR2.add(1, cells2));

    try {
      transaction1.commit();
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }

    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells2, CellSet.of());

    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells2.toArray()));

    transaction3.commit();
  }

  @Test
  public void insertOnOthersUncommittedUpdateTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction0
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR0 = transaction0.writerReader("employeeWR");
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), COUNTRY_ADDRESS.newCell("India"));
    employeeTWR0.add(1, cells);
    transaction0.commit();

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells1 = CellSet.of(NAME.newCell("Ravi"), SSN.newCell(100));
    assertTrue(employeeTWR1.update(1, UpdateOperation.install(cells1)));

    CellSet cells2 = CellSet.of(NAME.newCell("Ramesh"), CITY_ADDRESS.newCell("Bengaluru"));
    assertFalse(employeeTWR2.add(1, cells2));

    try {
      transaction1.commit();
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }

    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction3.commit();
  }

  @Test
  public void insertOnOthersUncommittedDeleteTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction0
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR0 = transaction0.writerReader("employeeWR");
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), COUNTRY_ADDRESS.newCell("India"));
    employeeTWR0.add(1, cells);
    transaction0.commit();

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertTrue(employeeTWR1.delete(1));

    CellSet cells2 = CellSet.of(NAME.newCell("Ramesh"), CITY_ADDRESS.newCell("Bengaluru"));
    assertFalse(employeeTWR2.add(1, cells2));

    try {
      transaction1.commit();
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }

    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells.toArray()));

    transaction3.commit();
  }

  @Test
  public void deleteOnOthersUncommittedInsertTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells1 = CellSet.of(NAME.newCell("Rahul"));
    assertTrue(employeeTWR1.add(1, cells1));
    assertFalse(employeeTWR2.delete(1));

    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells1, CellSet.of());

    transaction1.commit();
    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells1.toArray()));

    transaction3.commit();
  }

  @Test
  public void deleteOnOthersUncommittedUpdateTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction0
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR0 = transaction0.writerReader("employeeWR");
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), COUNTRY_ADDRESS.newCell("India"));
    employeeTWR0.add(1, cells);
    transaction0.commit();

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells1 = CellSet.of(NAME.newCell("Ravi"), SSN.newCell(100));
    assertTrue(employeeTWR1.update(1, UpdateOperation.install(cells1)));

    assertTrue(employeeTWR2.delete(1));

    try {
      transaction1.commit();
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }

    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertFalse(employeeTWR3.get(1).isPresent());

    transaction3.commit();
  }

  @Test
  public void deleteOnOthersUncommittedDeleteTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction0
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR0 = transaction0.writerReader("employeeWR");
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), COUNTRY_ADDRESS.newCell("India"));
    employeeTWR0.add(1, cells);
    transaction0.commit();

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertTrue(employeeTWR1.delete(1));

    assertTrue(employeeTWR2.delete(1));

    try {
      transaction1.commit();
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }

    testUncommittedEmployeeRecord(employeeWR, DELETE, 1, CellSet.of(), cells);

    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertFalse(employeeTWR3.get(1).isPresent());

    transaction3.commit();
  }

  @Test
  public void updateOnOthersUncommittedInsertTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells1 = CellSet.of(NAME.newCell("Rahul"));
    assertTrue(employeeTWR1.add(1, cells1));

    CellSet cells2 = CellSet.of(NAME.newCell("Ravi"), SSN.newCell(100));
    assertFalse(employeeTWR2.update(1, UpdateOperation.install(cells2)));

    testUncommittedEmployeeRecord(employeeWR, ADD, 1, cells1, CellSet.of());

    transaction1.commit();
    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells1.toArray()));

    transaction3.commit();
  }

  @Test
  public void updateOnOthersUncommittedUpdateTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction0
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR0 = transaction0.writerReader("employeeWR");
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), COUNTRY_ADDRESS.newCell("India"));
    employeeTWR0.add(1, cells);
    transaction0.commit();

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    CellSet cells1 = CellSet.of(NAME.newCell("Ravi"), SSN.newCell(100));
    assertTrue(employeeTWR1.update(1, UpdateOperation.install(cells1)));

    CellSet cells2 = CellSet.of(NAME.newCell("Ramesh"), CITY_ADDRESS.newCell("Bengaluru"));
    assertTrue(employeeTWR2.update(1, UpdateOperation.install(cells2)));

    try {
      transaction1.commit();
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }

    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, cells2, cells);

    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells2.toArray()));

    transaction3.commit();
  }

  @Test
  public void updateOnOthersUncommittedDeleteTest() throws Exception {
    TransactionController.ReadWriteTransaction transaction0
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR0 = transaction0.writerReader("employeeWR");
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), COUNTRY_ADDRESS.newCell("India"));
    employeeTWR0.add(1, cells);
    transaction0.commit();

    TransactionController.ReadWriteTransaction transaction1
      = controller.transact().timeout(holdingtransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");
    TransactionController.ReadWriteTransaction transaction2
      = controller.transact().timeout(waitingTransactionsTimeOutMillis, TimeUnit.MILLISECONDS).using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR2 = transaction2.writerReader("employeeWR");

    assertTrue(employeeTWR1.delete(1));

    CellSet cells2 = CellSet.of(NAME.newCell("Ramesh"), CITY_ADDRESS.newCell("Bengaluru"));
    assertTrue(employeeTWR2.update(1, UpdateOperation.install(cells2)));

    try {
      transaction1.commit();
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }

    testUncommittedEmployeeRecord(employeeWR, UPDATE, 1, cells2, cells);
    transaction2.commit();

    TransactionController.ReadWriteTransaction transaction3
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR3 = transaction3.writerReader("employeeWR");

    assertThat(employeeTWR3.get(1).get(), containsInAnyOrder(cells2.toArray()));

    transaction3.commit();
  }
}
