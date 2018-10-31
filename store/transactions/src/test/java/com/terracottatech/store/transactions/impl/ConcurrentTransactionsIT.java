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
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.common.test.Employee.BONUS;
import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.SSN;
import static com.terracottatech.store.transactions.impl.TransactionOperation.ADD;
import static com.terracottatech.store.transactions.impl.TransactionOperation.DELETE;
import static com.terracottatech.store.transactions.impl.TransactionOperation.UPDATE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class ConcurrentTransactionsIT extends TransactionsTests {

  private final CountDownLatch holdingTransactionHoldingNow = new CountDownLatch(1);
  private final CountDownLatch waitingTransactionWaitingNow = new CountDownLatch(1);

  private final int key = 1;
  private final CellSet cells = CellSet.of(SALARY.newCell(1_000_000D), BONUS.newCell(100D));
  private final CellSet cells1 = CellSet.of(NAME.newCell("Rahul"), SSN.newCell(100));
  private final CellSet cells2 = CellSet.of(CITY_ADDRESS.newCell("London"), COUNTRY_ADDRESS.newCell("England"));

  @Test
  public void insertWaitingForUncommittedInsertTest() throws Exception {
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(ADD, key, cells1);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(ADD, key, cells2, false);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    transaction2.commit();

    postCommitVerfication(key, Optional.of(cells1));
  }

  @Test
  public void insertWaitingForUncommittedUpdateTest() throws Exception {
    employeeWR.add(key, cells);
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(UPDATE, key, cells1);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(ADD, key, cells2, false);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    transaction2.commit();

    postCommitVerfication(key, Optional.of(cells1));
  }

  @Test
  public void insertWaitingForUncommittedDeleteTest() throws Exception {
    employeeWR.add(key, cells);
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(DELETE, key, null);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(ADD, key, cells2, true);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    testUncommittedEmployeeRecord(employeeWR, ADD, key, cells2, CellSet.of());

    transaction2.commit();

    postCommitVerfication(key, Optional.of(cells2));
  }

  @Test
  public void updateWaitingForUncommittedInsertTest() throws Exception {
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(ADD, key, cells1);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(UPDATE, key, cells2, false);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    transaction2.commit();

    postCommitVerfication(key, Optional.of(cells1));
  }

  @Test
  public void updateWaitingForUncommittedUpdateTest() throws Exception {
    employeeWR.add(key, cells);
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(UPDATE, key, cells1);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(UPDATE, key, cells2, true);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    testUncommittedEmployeeRecord(employeeWR, UPDATE, key, cells2, cells1);

    transaction2.commit();

    postCommitVerfication(key, Optional.of(cells2));
  }

  @Test
  public void updateWaitingForUncommittedDeleteTest() throws Exception {
    employeeWR.add(key, cells);
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(DELETE, key, null);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(UPDATE, key, cells2, false);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    transaction2.commit();

    postCommitVerfication(key, Optional.empty());
  }

  @Test
  @org.junit.Ignore("TDB-3888")
  public void deleteWaitingForUncommittedInsertTest() throws Exception {
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(ADD, key, cells1);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(DELETE, key, null, false);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    transaction2.commit();

    postCommitVerfication(key, Optional.of(cells1));
  }

  @Test
  public void deleteWaitingForUncommittedUpdateTest() throws Exception {
    employeeWR.add(key, cells);
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(UPDATE, key, cells1);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(DELETE, key, null, true);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    testUncommittedEmployeeRecord(employeeWR, DELETE, key, CellSet.of(), cells1);

    transaction2.commit();

    postCommitVerfication(key, Optional.empty());
  }

  @Test
  public void deleteWaitingForUncommittedDeleteTest() throws Exception {
    employeeWR.add(key, cells);
    HoldingSingleKeyTransaction transaction1 = new HoldingSingleKeyTransaction(DELETE, key, null);
    WaitingSingleKeyTransaction transaction2 = new WaitingSingleKeyTransaction(DELETE, key, null, false);

    executeBothTransactionsConcurrently(transaction1, transaction2);

    transaction2.commit();

    postCommitVerfication(key, Optional.empty());
  }

  class WaitingSingleKeyTransaction implements Callable<Boolean> {
    ReadWriteTransaction transaction;
    DatasetWriterReader<Integer> employeeTWR;
    TransactionOperation operation;
    int key;
    CellSet cells;
    boolean expectedOperationResult;

    WaitingSingleKeyTransaction(TransactionOperation operation, int key, CellSet cells, boolean expectedOperationResult) {
      transaction = controller.transact().timeout(60, TimeUnit.SECONDS).using("employeeWR", employeeWR).begin();
      employeeTWR = transaction.writerReader("employeeWR");
      this.operation = operation;
      this.key = key;
      this.cells = cells;
      this.expectedOperationResult = expectedOperationResult;
    }

    @Override
    public Boolean call() {
      boolean operationResult;

      switch (operation) {
        case ADD:
          operationResult = employeeTWR.add(key, cells);
          break;
        case UPDATE:
          operationResult = employeeTWR.update(key, UpdateOperation.install(cells));
          break;
        case DELETE:
          operationResult = employeeTWR.delete(key);
          break;
        default:
          throw new AssertionError();
      }

      return (operationResult == expectedOperationResult);
    }

    public void commit() {
      transaction.commit();
    }
  }

  class HoldingSingleKeyTransaction implements Callable<Boolean> {
    ReadWriteTransaction transaction;
    DatasetWriterReader<Integer> employeeTWR;
    TransactionOperation operation;
    int key;
    CellSet cells;

    HoldingSingleKeyTransaction(TransactionOperation operation, int key, CellSet cells) {
      transaction = controller.transact().timeout(60, TimeUnit.SECONDS).using("employeeWR", employeeWR).begin();
      employeeTWR = transaction.writerReader("employeeWR");
      this.operation = operation;
      this.key = key;
      this.cells = cells;
    }

    @Override
    public Boolean call() {
      Boolean retVal;

      switch (operation) {
        case ADD:
          retVal = employeeTWR.add(key, cells);
          break;
        case UPDATE:
          retVal = employeeTWR.update(key, UpdateOperation.install(cells));
          break;
        case DELETE:
          retVal = employeeTWR.delete(key);
          break;
        default:
          throw new AssertionError();
      }

      holdingTransactionHoldingNow.countDown();
      try {
        waitingTransactionWaitingNow.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        return false;
      }

      try {
        transaction.commit();
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }

      return retVal;
    }
  }

  public void postCommitVerfication(int key, Optional<CellSet> cells) throws Exception {
    ReadWriteTransaction transaction1
      = controller.transact().using("employeeWR", this.employeeWR).begin();
    DatasetWriterReader<Integer> employeeTWR1 = transaction1.writerReader("employeeWR");

    if (cells.isPresent()) {
      assertThat(employeeTWR1.get(key).get(), containsInAnyOrder(cells.get().toArray()));
    } else {
      assertFalse(employeeTWR1.get(key).isPresent());
    }

    transaction1.commit();
  }

  private void executeBothTransactionsConcurrently(Callable<Boolean> transaction1, Callable<Boolean> transaction2) throws Exception {
    ExecutorService excutor = Executors.newFixedThreadPool(2);
    Future<Boolean> transaction1Result = excutor.submit(transaction1);
    holdingTransactionHoldingNow.await();
    Future<Boolean> transaction2Result = excutor.submit(transaction2);
    Thread.sleep(200);  // wait to allow the waiting transaction to start waiting on the holding transaction
    waitingTransactionWaitingNow.countDown();

    assertTrue(transaction1Result.get());
    assertTrue(transaction2Result.get());
  }
}
