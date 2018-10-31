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
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.TimeUnit;

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
public class PostRebootIT extends TransactionsOnDiskTests {
  @Rule
  public TestName testName = new TestName();

  static final long TRANSACTION_TIMEOUT_MILLIS = 1_000;
  static final long TRANSACTION_TIMEOUT_MULTIPLICATION_FACTOR = 6;
  static final long MAX_TRANSACTION_TIMEOUT_MILLIS = (TRANSACTION_TIMEOUT_MULTIPLICATION_FACTOR ^ 2)  * TRANSACTION_TIMEOUT_MILLIS;
  long transactionTimeOut;

  @Before
  public void init() {
    transactionTimeOut = TRANSACTION_TIMEOUT_MILLIS;
  }

  public DatasetReader<Integer> postRebootTransactionReader() throws Exception {
    TransactionController.ReadOnlyTransaction postRebootTransaction
      = controller.transact().using("employeeR", this.employeeR).begin();
    return postRebootTransaction.reader("employeeR");
  }

  @Test
  public void insertPostRebootTest() throws Exception {
    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
      assertTrue(employeeTWR1.add(1, cells));
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertFalse(postRebootTransactionReader().get(1).isPresent());
  }

  @Test
  public void insertOnTopOfSelfDeletedPostRebootTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      employeeTWR1.delete(1);
      CellSet cells1 = CellSet.of(TELEPHONE.newCell(100L));
      assertTrue(employeeTWR1.add(1, cells1));
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertThat(postRebootTransactionReader().get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void deletePostRebootTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      employeeTWR1.delete(1);
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertThat(postRebootTransactionReader().get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void deleteOnTopOfSelfInsertedPostRebootTest() throws Exception {
    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
      assertTrue(employeeTWR1.add(1, cells));
      employeeTWR1.delete(1);
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertFalse(postRebootTransactionReader().get(1).isPresent());
  }

  @Test
  public void deleteOnTopOfSelfUpdatedPostRebootTest()  throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      CellSet newCells = new CellSet();
      newCells.add(BONUS.newCell(100D));
      newCells.add(SALARY.newCell(100000D));
      employeeTWR1.update(1, UpdateOperation.install(newCells));
      employeeTWR1.delete(1);
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertThat(postRebootTransactionReader().get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void updatePostRebootTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      CellSet newCells = new CellSet();
      newCells.add(BONUS.newCell(100D));
      newCells.add(SALARY.newCell(100000D));
      employeeTWR1.update(1, UpdateOperation.install(newCells));
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertThat(postRebootTransactionReader().get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  @Test
  public void updateOnTopOfSelfInsertedPostRebootTest() throws Exception {
    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
      assertTrue(employeeTWR1.add(1, cells));

      CellSet newCells = new CellSet();
      newCells.add(BONUS.newCell(100D));
      newCells.add(SALARY.newCell(100000D));
      employeeTWR1.update(1, UpdateOperation.install(newCells));
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertFalse(postRebootTransactionReader().get(1).isPresent());
  }

  @Test
  public void updateOnTopOfSelfUpdatedPostRebootTest() throws Exception {
    CellSet cells = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
    employeeWR.add(1, cells);

    runWithRetry(() -> {
      TransactionController.ReadWriteTransaction transaction1
        = controller.transact().timeout(transactionTimeOut, TimeUnit.MILLISECONDS)
        .using("employeeWR", this.employeeWR).begin();
      DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

      CellSet newCells1 = new CellSet();
      newCells1.add(CITY_ADDRESS.newCell("Pune"));
      newCells1.add(COUNTRY_ADDRESS.newCell("India"));
      employeeTWR1.update(1, UpdateOperation.install(newCells1));

      CellSet newCells2 = new CellSet();
      newCells2.add(CITY_ADDRESS.newCell("London"));
      newCells2.add(COUNTRY_ADDRESS.newCell("England"));
      assertTrue(employeeTWR1.update(1, UpdateOperation.install(newCells2)));
    });

    reboot();
    Thread.sleep(transactionTimeOut);

    assertThat(postRebootTransactionReader().get(1).get(), containsInAnyOrder(cells.toArray()));
  }

  void runWithRetry(Runnable transaction) {
    while (true) {
      try {
        transaction.run();
      } catch (StoreTransactionTimeoutException e) {
        if (transactionTimeOut < MAX_TRANSACTION_TIMEOUT_MILLIS) {
          transactionTimeOut *= TRANSACTION_TIMEOUT_MULTIPLICATION_FACTOR;
          System.out.println("Re-running " + testName.getMethodName() + " with transaction timeout=" + transactionTimeOut + "millis");
          continue;
        } else {
          throw e;
        }
      }

      break;
    }
  }
}

