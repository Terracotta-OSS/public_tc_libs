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
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.api.TransactionController.ReadWriteTransaction;
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;

@SuppressWarnings("deprecation")
public class ConcurrentTransactionsTests extends TransactionsTests {

  private enum TestOperation {
    ADD {
      @Override
      void perform(DatasetWriterReader<Integer> writerReader, int key, boolean debugMode, String prefix) {
        Optional<Record<Integer>> addResult = writerReader.on(key).add(addedCells);
        if (debugMode) {
          System.out.println(prefix + (addResult.isPresent() ? "Failure:" : "Success:") + "ADD:" + key);
        }
      }
    },
    READ {
      @Override
      void perform(DatasetWriterReader<Integer> writerReader, int key, boolean debugMode, String prefix) {
        Optional<Record<Integer>> getResult = writerReader.get(key);
        if (debugMode) {
          System.out.println(prefix + (getResult.isPresent() ? "Success:" : "Failure:") + "GET:" + key);
        }
      }
    },
    UPDATE {
      @Override
      void perform(DatasetWriterReader<Integer> writerReader, int key, boolean debugMode, String prefix) {
        Optional<Tuple<Record<Integer>, Record<Integer>>> updateResult
          = writerReader.on(key).update(UpdateOperation.install(updatedCells));
        if (debugMode) {
          System.out.println(prefix + (updateResult.isPresent() ? "Success:" : "Failure:") + "UPDATE:" + key);
        }
      }
    },
    DELETE {
      @Override
      void perform(DatasetWriterReader<Integer> writerReader, int key, boolean debugMode, String prefix) {
        Optional<Record<Integer>> deleteResult = writerReader.on(key).delete();
        if (debugMode) {
          System.out.println(prefix + (deleteResult.isPresent() ? "Success:" : "Failure:") + "DELETE:" + key);
        }
      }
    };

    private static final CellSet addedCells = CellSet.of(NAME.newCell("Rahul"), SALARY.newCell(1000.827D));
    private static final CellSet updatedCells = CellSet.of(NAME.newCell("Chris"), SALARY.newCell(98765.999D));
    private static final List<TestOperation> VALUES =
      Collections.unmodifiableList(Arrays.asList(values()));
    private static final int SIZE = VALUES.size();
    private static final Random RANDOM = new Random();

    public static TestOperation randomOperation()  {
      return VALUES.get(RANDOM.nextInt(SIZE));
    }

    abstract void perform(DatasetWriterReader<Integer> writerReader, int key, boolean debugMode, String prefix);
  }

  private class RandomTransaction implements Runnable {

    private final TransactionController controller;
    private final DatasetWriterReader<Integer> writerReader;
    private final int minKeyValue;
    private final int maxKeyValue;

    private final int minTransactionSize;
    private final int maxTransactionSize;

    private final boolean debugMode;

    private ReadWriteTransaction transaction;
    private MyUncaughtExceptionHandler uncaughtExceptionHandler;

    private final long transactionTimeOutMillis;

    public RandomTransaction(TransactionController controller, DatasetWriterReader<Integer> writerReader,
                             int minKeyValue, int maxKeyValue,
                             int minTransactionSize, int maxTransactionSize,
                             boolean debugMode, MyUncaughtExceptionHandler uncaughtExceptionHandler,
                             long transactionTimeOutMillis) {
      this.controller = controller;
      this.writerReader = writerReader;
      this.minKeyValue = minKeyValue;
      this.maxKeyValue = maxKeyValue;
      this.minTransactionSize = minTransactionSize;
      this.maxTransactionSize = maxTransactionSize;
      this.debugMode = debugMode;
      this.uncaughtExceptionHandler = uncaughtExceptionHandler;
      this.transactionTimeOutMillis = transactionTimeOutMillis;
    }

    public RandomTransaction(TransactionController controller, DatasetWriterReader<Integer> writerReader,
                             int minKeyValue, int maxKeyValue,
                             int minTransactionSize, int maxTransactionSize,
                             MyUncaughtExceptionHandler uncaughtExceptionHandler,
                             long transactionTimeOutMillis) {
      this(controller, writerReader,
        minKeyValue, maxKeyValue,
        minTransactionSize, maxTransactionSize,
        false, uncaughtExceptionHandler,
        transactionTimeOutMillis);
    }

    @Override
    public void run() {
      Thread.currentThread().setUncaughtExceptionHandler(uncaughtExceptionHandler);
      int transactionSize = ThreadLocalRandom.current().nextInt(minTransactionSize, maxTransactionSize + 1);

      transaction = controller.transact()
        .timeout(transactionTimeOutMillis, TimeUnit.MILLISECONDS)
        .using("WR", writerReader)
        .begin();
      DatasetWriterReader<Integer> tWR = transaction.writerReader("WR");

      try {
        AtomicInteger i = new AtomicInteger(0);
        ThreadLocalRandom.current()
          .ints(transactionSize, minKeyValue, maxKeyValue + 1)
          .forEach(key -> performRandomOperation(tWR, key, debugMode, prefix(i.incrementAndGet(), transactionSize)));
      } catch (StoreTransactionTimeoutException e) {
        // the transaction would have rolled back due to timeout exception, hence exit
        if (debugMode) {
          System.out.println("ThreadID-" + Thread.currentThread().getId() + ":Timed out");
        }
        return;
      }

      try {
        transaction.commit();
      } catch (StoreTransactionTimeoutException exception) {
        if (debugMode) {
          System.out.println("ThreadID-" + Thread.currentThread().getId() + ":Timed out");
        }
        return;
      }
    }

    private void performRandomOperation(DatasetWriterReader<Integer> writerReader, int key, boolean debugMode, String prefix) {
      TestOperation.randomOperation().perform(writerReader, key, debugMode, prefix);
    }

    private String prefix(int currentOperationID, int transactionSize) {
      if (debugMode) {
        return "ThreadID-" + Thread.currentThread().getId() + ":(" + currentOperationID + "/" + transactionSize + "):";
      } else {
        return "";
      }
    }
  }

  public class EmbeddedConcurrentTransactions {

    private final int minKeyValue = 1;
    private final int maxKeyValue;
    private final int minTransactionSize;
    private final int maxTransactionSize;
    private final int numThreads;
    private final long transactionPerThread;

    private final boolean debugMode;
    private final MyUncaughtExceptionHandler uncaughtExceptionHandler;

    private final long transactionTimeOutMillis;

    public EmbeddedConcurrentTransactions(int numThreads, long transactionPerThread,
                                          int minTransactionSize, int maxTransactionSize,
                                          int maxKeyValue, boolean debugMode,
                                          long transactionTimeOutMillis) {
      this.maxKeyValue = maxKeyValue;
      this.minTransactionSize = minTransactionSize;
      this.maxTransactionSize = maxTransactionSize;
      this.numThreads = numThreads;
      this.transactionPerThread = transactionPerThread;
      this.debugMode = debugMode;
      this.uncaughtExceptionHandler = new MyUncaughtExceptionHandler();
      this.transactionTimeOutMillis = transactionTimeOutMillis;

      initData();
    }

    public EmbeddedConcurrentTransactions(int numThreads, long transactionPerThread,
                                          int minTransactionSize, int maxTransactionSize,
                                          int maxKeyValue,
                                          long transactionTimeOutMillis) {
      this(numThreads, transactionPerThread, minTransactionSize, maxTransactionSize, maxKeyValue, false, transactionTimeOutMillis);
    }

    public Map<Long, Throwable> run() throws Exception {
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);

      for (long i = 0; i < (numThreads * transactionPerThread); i++) {
        executor.execute(new RandomTransaction(controller, employeeWR,
          minKeyValue, maxKeyValue,
          minTransactionSize, maxTransactionSize,
          debugMode, uncaughtExceptionHandler, transactionTimeOutMillis));
      }

      executor.shutdown();
      executor.awaitTermination(120, TimeUnit.SECONDS);

      return uncaughtExceptionHandler.getUncaughtExceptions();
    }

    private void initData() {
      for (int i = 1; i <= maxKeyValue; i++) {
        if (i % 4 == 0) {
          continue;
        }
        TestOperation.ADD.perform(employeeWR, i, false, "");
      }
    }
  }

  public class MyUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private ConcurrentHashMap<Long, Throwable> uncaughtExceptions = new ConcurrentHashMap<>();

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      uncaughtExceptions.put(t.getId(), e);
    }

    public ConcurrentHashMap<Long, Throwable> getUncaughtExceptions() {
      return uncaughtExceptions;
    }
  }
}
