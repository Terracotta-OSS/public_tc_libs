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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class ConcurrentTransactionsSanityIT extends ConcurrentTransactionsTests {

  private boolean debugMode = false;

  private List<RunParameters> runParametersList = Arrays.asList(
    // small size transactions, low concurrency
    new RunParameters(10, 4, 1, 10, 100, 300)
    // small size transactions, higher concurrency
    , new RunParameters(10, 4, 1, 10, 50, 300)
    // small size transactions, high concurrency
    , new RunParameters(10, 4, 1, 10, 20, 300)

    // medium size transactions, low concurrency
    , new RunParameters(10, 3, 10, 20, 200, 600)
    // medium size transactions, higher concurrency
    , new RunParameters(10, 3, 10, 20, 150, 600)
    // medium size transactions, high concurrency
    , new RunParameters(10, 3, 10, 20, 100, 600)

    // large size transactions, low concurrency
    , new RunParameters(10, 2, 20, 50, 500, 1_000)
    // large size transactions, higher concurrency
    , new RunParameters(10, 2, 20, 50, 350, 1_000)
    // large size transactions, high concurrency
    , new RunParameters(10, 2, 20, 50, 200, 1_000)

    // very small transactions, very high concurrency
    , new RunParameters(20, 500, 1, 3, 30, 300)

    // one record, single operation transactions
    , new RunParameters(40, 1000, 1, 1, 1, 300)
    // one record, one or two operations transactions
    , new RunParameters(40, 1000, 1, 2, 1, 300)
  );

  @Test
  public void test() throws Exception {
    for (RunParameters runParameter : runParametersList) {
      long startTime = System.currentTimeMillis();

      Map<Long, Throwable> throwableMap
        = new EmbeddedConcurrentTransactions(runParameter.numThreads, runParameter.transactionsPerThread,
        runParameter.minTransactionSize, runParameter.maxTransactionSize,
        runParameter.maxKeyValue, debugMode, runParameter.transactionTimeOutMillis)
        .run();

      long totalTime = System.currentTimeMillis() - startTime;
      System.out.println(runParameter + ": " + totalTime / 1000 + " seconds");

      throwableMap.entrySet().forEach(entry -> System.out.println("Thread ID:" + entry.getKey() + "\n" + entry.getValue()));
      assertTrue(throwableMap.isEmpty());
    }
  }

  private class RunParameters {
    public final int numThreads;
    public final int transactionsPerThread;
    public final int minTransactionSize;
    public final int maxTransactionSize;
    public final int maxKeyValue;
    public final long transactionTimeOutMillis;

    public RunParameters(int numThreads, int transactionsPerThread,
                         int minTransactionSize, int maxTransactionSize,
                         int maxKeyValue,
                         long transactionTimeOutMillis) {
      this.numThreads = numThreads;
      this.transactionsPerThread = transactionsPerThread;
      this.minTransactionSize = minTransactionSize;
      this.maxTransactionSize = maxTransactionSize;
      this.maxKeyValue = maxKeyValue;
      this.transactionTimeOutMillis = transactionTimeOutMillis;
    }

    @Override
    public String toString() {
      return "RunParameters{" +
        "numThreads=" + numThreads +
        ", transactionsPerThread=" + transactionsPerThread +
        ", minTransactionSize=" + minTransactionSize +
        ", maxTransactionSize=" + maxTransactionSize +
        ", maxKeyValue=" + maxKeyValue +
        '}';
    }
  }
}
