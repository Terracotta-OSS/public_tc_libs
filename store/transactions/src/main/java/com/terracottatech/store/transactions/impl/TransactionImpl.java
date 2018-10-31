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

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

abstract class TransactionImpl<T> implements TransactionController.Transaction {
  static final String TRANSACTIONAL_CELLNAME_PREFIX = "__tc_store_tx:";
  static final String TRANSACTIONAL_UNCOMMITED_CELL_PREFIX = TRANSACTIONAL_CELLNAME_PREFIX + "pending:";

  private final long startTimeMillis;
  private final long deadlineMillis;
  private final AtomicBoolean active = new AtomicBoolean(true);
  private final SystemTransactions systemTransactions;

  TransactionImpl(SystemTransactions systemTransactions, Duration timeOut) {
    systemTransactions.checkIfClosed();
    this.systemTransactions = systemTransactions;
    this.startTimeMillis = TimeSource.currentTimeMillis();
    this.deadlineMillis = this.startTimeMillis + timeOut.toMillis();
  }

  abstract T execute() throws Exception;

  abstract boolean identifiesSelf(TransactionID transactionID);

  long getStartTimeMillis() {
    return startTimeMillis;
  }

  long getDeadlineMillis() {
    return deadlineMillis;
  }

  boolean isActive() {
    return active.get();
  }

  boolean markInactive() {
    return active.compareAndSet(true, false);
  }

  SystemTransactions getSystemTransactions() {
    systemTransactions.checkIfClosed();
    return systemTransactions;
  }

  // Should be called when the transaction instance is synchronized
  private void checkTimeOut() {
    if (getDeadlineMillis() < TimeSource.currentTimeMillis()) {

      StoreTransactionTimeoutException storeTransactionTimeoutException
        = new StoreTransactionTimeoutException("Transaction has timed out and has been rolled back.");

      try {
        rollback();
      } catch (Exception e) {
        StoreTransactionRuntimeException exception =
          new StoreTransactionRuntimeException("Transaction has timed out and even the rollback has failed.", e);
        exception.addSuppressed(storeTransactionTimeoutException);
        throw exception;
      }

      throw storeTransactionTimeoutException;
    }
  }

  // Should be called when the transaction instance is synchronized
  final void checkStatus() {
    if (!isActive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }

    getSystemTransactions().checkIfClosed();

    checkTimeOut();
  }

  abstract <K extends Comparable<K>> Optional<Record<K>> transactionalGet(Optional<Record<K>> recordOptional, DatasetReader<K> reader);

  final <K extends Comparable<K>> Stream<Record<K>> transactionalRecordStream(DatasetReader<K> reader) {
    return reader.records()
      .map(record -> transactionalGet(Optional.of(record), reader))
      .filter(Optional::isPresent)
      .map(Optional::get);
  }
}
