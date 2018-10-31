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

import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.api.TransactionController.ReadOnlyTransaction;
import com.terracottatech.store.transactions.api.TransactionController.ReadWriteTransaction;
import com.terracottatech.store.transactions.api.TransactionalTask;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"deprecation", "unchecked"})
public class TransactionControllerIT extends TransactionsTests {

  @Test
  public void testControllerOnClosedDatasetManager() throws Exception {
    DatasetManager dm = DatasetManager.embedded().offheap("offheap", 128L, MemoryUnit.MB).build();
    dm.close();

    try {
      TransactionController tc = new TransactionControllerImpl(dm);
      fail("TransactionController creation with closed DatasetManager should throw exception");
    } catch (StoreException e) {
      assertTrue(e.getMessage().equals("Attempt to use DatasetManager after close()"));
    }
  }

  @Test
  public void testControllerOnDatasetManagerWithoutSystemTransactions() throws Exception {
    try (DatasetManager dm = DatasetManager.embedded().offheap("offheap", 128L, MemoryUnit.MB).build()) {

      try {
        TransactionController tc = new TransactionControllerImpl(dm);
        fail("TransactionController creation with closed DatasetManager should throw exception");
      } catch (DatasetMissingException e) {
        assertTrue(e.getMessage().equals("Dataset 'SystemTransactions' not found"));
      }
    }
  }

  @Test
  public void testControllerOnDatasetManagerAlreadyHavingSystemTransactions() throws Exception {
    DatasetConfigurationBuilder embeddedConfigBuilder =
      datasetManager.datasetConfiguration().offheap("offheap");
    TransactionController tc = new TransactionControllerImpl(datasetManager, embeddedConfigBuilder);
  }

  @Test
  public void testControllerWithDefaultTimeOut() throws Exception {
    ReadWriteTransaction transaction =
      controller.withDefaultTimeOut(50, TimeUnit.MILLISECONDS).transact().using("WR", employeeWR).begin();

    Thread.sleep(100);

    try {
      transaction.commit();
      fail("Transaction should have timed out by this time and hence the operation should have failed");
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }
  }

  @Test
  public void testControllerDuplicateResourceAdding() {
    try {
      controller.transact()
        .using("r1", employeeR)
        .using("r1", customerR);
      fail("Duplicate resource adding should not have been allowed");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().equals("A DatasetReader with the given name r1 has already been added."));
    }

    try {
      controller.transact()
        .using("rw1", employeeWR)
        .using("rw1", customerWR);
      fail("Duplicate resource adding should not have been allowed");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().equals("A DatasetWriterReader with the given name rw1 has already been added."));
    }

    try {
      controller.transact()
        .using("r1", employeeR)
        .using("rw1", customerWR)
        .using("r1", customerR);
      fail("Duplicate resource adding should not have been allowed");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().equals("A DatasetReader with the given name r1 has already been added."));
    }

    try {
      controller.transact()
        .using("rw1", employeeWR)
        .using("r1", customerR)
        .using("r1", employeeR);
      fail("Duplicate resource adding should not have been allowed");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().equals("A DatasetReader with the given name r1 has already been added."));
    }

    controller.transact()
      .using("rw1", employeeWR)
      .using("rw2", employeeWR);

    controller.transact()
      .using("r1", employeeR)
      .using("r2", employeeR);

    controller.transact()
      .using("r1", employeeR)
      .using("rw1", employeeWR)
      .using("r2", employeeR);
  }

  @Test
  public void testReadOnlyTransactionTimeOut() throws Exception {
    ReadOnlyTransaction transaction =
      controller.transact().timeout(50, TimeUnit.MILLISECONDS).using("r1", employeeR).begin();

    Thread.sleep(100);

    try {
      transaction.commit();
      fail("Transaction should have timed out by this time and hence the operation should have failed");
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }
  }

  @Test
  public void testReadWriteTransactionTimeOut() throws Exception {
    ReadWriteTransaction transaction =
      controller.transact().timeout(50, TimeUnit.MILLISECONDS).using("rw1", employeeWR).begin();

    Thread.sleep(100);

    try {
      transaction.commit();
      fail("Transaction should have timed out by this time and hence the operation should have failed");
    } catch (StoreTransactionTimeoutException e) {
      assertTrue(e.getMessage().equals("Transaction has timed out and has been rolled back."));
    }
  }

  @Test
  public void testReadOnlyTransactionExecuteForWrongResource() throws Exception {
    try {
      controller.transact()
        .using("r1", employeeR)
        .execute(readers -> {
          DatasetReader<String> customerReader = (DatasetReader<String>) readers.get("r2");
          customerReader.records().count();
          return null;
        });
      fail("NullPointerException expected if wrong resource used in execute()");
    } catch (NullPointerException e) {
      return;
    }
  }

  @Test
  public void testReadOnlyTransactionExecuteWithWrongTypeCast() throws Exception {
    try {
      controller.transact()
        .using("r1", employeeR)
        .execute(readers -> {
          DatasetReader<String> customerReader = (DatasetReader<String>) readers.get("r1");
          return customerReader.get("Rahul").isPresent();
        });
      fail("ClassCastException expected if wrong resource used in execute()");
    } catch (ClassCastException e) {
      assertTrue(e.getMessage().equals("Expecting record key type java.lang.Integer: found java.lang.String"));
    }
  }

  @Test
  public void testReadWriteTransactionExecuteForWrongResource() throws Exception {
    try {
      controller.transact()
        .using("rw1", employeeWR)
        .execute((writers, readers) -> {
          DatasetWriterReader<String> customerWriterReader = (DatasetWriterReader<String>) writers.get("rw2");
          customerWriterReader.records().count();
          return null;
        });
      fail("NullPointerException expected if wrong resource used in execute()");
    } catch (NullPointerException e) {
      return;
    }
  }

  @Test
  public void testReadWriteTransactionExecuteWithWrongTypeCast() throws Exception {
    try {
      controller.transact()
        .using("rw1", employeeWR)
        .execute((writers, readers) -> {
          DatasetWriterReader<String> customerWriterReader = (DatasetWriterReader<String>) writers.get("rw1");
          return customerWriterReader.get("Rahul").isPresent();
        });
      fail("ClassCastException expected if wrong resource used in execute()");
    } catch (ClassCastException e) {
      assertTrue(e.getMessage().equals("Expecting record key type java.lang.Integer: found java.lang.String"));
    }
  }

  @Test
  public void testReadWriteTransactionExecuteWithWrongWriterReaderTypeCast() throws Exception {
    try {
      controller.transact()
        .using("r1", employeeR)
        .using("rw1", employeeWR)
        .execute((writers, readers) -> {
          DatasetWriterReader<Integer> employeeWriterReader = (DatasetWriterReader<Integer>) readers.get("r1");
          return employeeWriterReader.get(1).isPresent();
        });
      fail("ClassCastException expected if wrong resource used in execute()");
    } catch (ClassCastException e) {
      assertTrue(e.getMessage().equals("com.terracottatech.store.transactions.impl.TransactionalDatasetReader cannot be cast to com.terracottatech.store.DatasetWriterReader"));
    }
  }

  @Test
  public void testReadOnlyTransactionExecute() throws Exception {
    employeeWR.add(1, NAME.newCell("Rahul"), SALARY.newCell(1000D));
    customerWR.add("Rahul", EXPENDITURE.newCell(200D));

    Double takeHome = controller.transact()
      .using("r1", employeeR)
      .using("r2", customerR)
      .execute(readers -> {
        DatasetReader<Integer> employeeReader = (DatasetReader<Integer>) readers.get("r1");
        DatasetReader<String> customerReader = (DatasetReader<String>) readers.get("r2");

        double salary = employeeReader.get(1).get().get(SALARY).get();
        double expenditure = customerReader.get(employeeReader.get(1).get().get(NAME).get()).get().get(EXPENDITURE).get();

        return salary - expenditure;
      });

    assertTrue(takeHome.equals(800D));
  }

  @Test
  public void testReadWriteTransactionExecute() throws Exception {
    employeeWR.add(1, NAME.newCell("Rahul"), SALARY.newCell(1000D));
    customerWR.add("Rahul", EXPENDITURE.newCell(200D));

    Double takeHome = controller.transact()
      .using("r1", employeeR)
      .using("r2", customerR)
      .using("rw1", employeeWR)
      .using("rw2", customerWR)
      .execute((writers, readers) -> {
        DatasetWriterReader<Integer> employeeWriterReader = (DatasetWriterReader<Integer>) writers.get("rw1");
        DatasetWriterReader<String> customerWriterReader = (DatasetWriterReader<String>) writers.get("rw2");

        employeeWriterReader.on(1)
          .update(UpdateOperation.write(SALARY).doubleResultOf(SALARY.doubleValueOrFail().multiply(2D)));
        customerWriterReader.on(employeeWriterReader.get(1).get().get(NAME).get())
          .update(UpdateOperation.write(EXPENDITURE).doubleResultOf(EXPENDITURE.doubleValueOrFail().divide(2D)));

        DatasetReader<Integer> employeeReader = (DatasetReader<Integer>) readers.get("r1");
        DatasetReader<String> customerReader = (DatasetReader<String>) readers.get("r2");

        double salary = employeeReader.get(1).get().get(SALARY).get();
        double expenditure = customerReader.get(employeeReader.get(1).get().get(NAME).get()).get().get(EXPENDITURE).get();

        return salary - expenditure;
      });

    assertTrue(takeHome.equals(1900D));
  }

  @Test
  public void testReadOnlyTransactionExecuteWithInternalException() throws Exception {
    employeeWR.add(1, NAME.newCell("Rahul"), SALARY.newCell(1000D));

    try {
      controller.transact()
        .using("r1", employeeR)
        .execute(readers -> {
          throw new IllegalStateException("User thrown exception from inside execute lambda");
        });
      fail("Exception should have been thrown by previous statement");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().equals("User thrown exception from inside execute lambda"));
    }
  }

  @Test
  public void testReadWriteTransactionExecuteWithInternalException() throws Exception {
    employeeWR.add(1, NAME.newCell("Rahul"), SALARY.newCell(1000D));

    try {
      controller.transact()
        .using("rw1", employeeWR)
        .execute((writers, readers) -> {
          throw new IllegalStateException("User thrown exception from inside execute lambda");
        });
      fail("Exception should have been thrown by previous statement");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().equals("User thrown exception from inside execute lambda"));
    }
  }

  @Test
  public void testTransactionControllerAfterClose() throws Exception {
    TransactionController transactionController = new TransactionControllerImpl(datasetManager);

    ReadWriteTransaction transaction1 = transactionController.transact().using("wr", employeeWR).begin();
    DatasetWriterReader<Integer> writerReader1 = transaction1.writerReader("wr");

    transactionController.close();

    try {
      transactionController.execute(employeeR, reader -> reader.get(1));
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      transactionController.execute(employeeWR, (TransactionalTask<DatasetWriterReader<Integer>>) wr -> wr.get(1));
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      transactionController.transact().using("r", employeeR).begin();
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      transactionController.transact().using("wr", employeeWR).begin();
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      writerReader1.get(1);
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      writerReader1.add(1);
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      writerReader1.delete(1);
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      writerReader1.update(1, UpdateOperation.write(SALARY).value(100D));
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }

    try {
      transaction1.commit();
      fail("Previous statement should have failed because the transaction controller has already been closed");
    } catch (StoreTransactionRuntimeException e) {
      assertTrue(e.getMessage().equals("SystemTransactions Dataset has already been closed"));
    }
  }
}
