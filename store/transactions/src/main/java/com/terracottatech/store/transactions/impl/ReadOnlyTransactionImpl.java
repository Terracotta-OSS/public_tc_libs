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
import com.terracottatech.store.transactions.api.TransactionalAction;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.terracottatech.store.transactions.impl.TransactionID.getTransactionID;
import static com.terracottatech.store.transactions.impl.TransactionStatus.COMMITTED;
import static java.util.stream.Collectors.toMap;

class ReadOnlyTransactionImpl<T> extends TransactionImpl<T> implements TransactionController.ReadOnlyTransaction {

  private final TransactionalAction<Map<String, TransactionalDatasetReader<?>>, T> action;
  protected final Map<String, TransactionalDatasetReader<?>> readers;

  ReadOnlyTransactionImpl(SystemTransactions systemTransactions, Duration timeOut,
                          Map<String, DatasetReader<?>> readers,
                          TransactionalAction<Map<String, TransactionalDatasetReader<?>>, T> action) {
    super(systemTransactions, timeOut);
    this.action = action;
    this.readers = transactionalReaders(readers);
  }

  @Override
  T execute() throws Exception {
    return action.perform(readers);
  }

  @Override
  public synchronized void commit() {
    checkStatus();

    if (!markInactive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }
  }

  @Override
  public synchronized void rollback() {
    if (!markInactive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K extends Comparable<K>> DatasetReader<K> reader(String name) {
    if (!isActive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }
    return (DatasetReader<K>) readers.get(name);
  }

  @Override
  boolean identifiesSelf(TransactionID transactionID) {
    return false;
  }

  @Override
  synchronized <K extends Comparable<K>> Optional<Record<K>> transactionalGet(Optional<Record<K>> recordOptional, DatasetReader<K> reader) {

    while (true) {
      checkStatus();

      if (!recordOptional.isPresent()) {
        return recordOptional;
      }

      Record<K> record = recordOptional.get();
      Optional<TransactionID> transactionIDOptional = getTransactionID(record);

      if (!transactionIDOptional.isPresent()) {
        return recordOptional;
      } else if (identifiesSelf(transactionIDOptional.get())) {
        return Optional.ofNullable(COMMITTED.recordImage(recordOptional.get()));
      }

      Optional<TransactionStatus> transactionStatusOptional
        = getSystemTransactions().getTransactionStatus(transactionIDOptional.get());

      if (transactionStatusOptional.isPresent()) {
        TransactionStatus transactionStatus = transactionStatusOptional.get();

        if (transactionStatus.equals(COMMITTED)) {
          Optional<Record<K>> newRecordOptional = reader.get(record.getKey());
          if (!newRecordOptional.isPresent()) {
            return Optional.empty();
          }

          Optional<TransactionID> newTransactionIDOptional = getTransactionID(newRecordOptional.get());

          if (!newTransactionIDOptional.isPresent()) {
            return newRecordOptional;
          } else if (!newTransactionIDOptional.get().equals(transactionIDOptional.get())) {
            recordOptional = newRecordOptional;
            continue;
          } else {
            return Optional.ofNullable(COMMITTED.resolvedRecordImageForRead(newRecordOptional.get()));
          }
        }

        return Optional.ofNullable(transactionStatus.resolvedRecordImageForRead(record));
      } else {
        Optional<Record<K>> newRecordOptional = reader.get(record.getKey());
        if (!newRecordOptional.isPresent()) {
          return Optional.empty();
        }

        Optional<TransactionID> newTransactionIDOptional = getTransactionID(newRecordOptional.get());

        if (!newTransactionIDOptional.isPresent()) {
          return newRecordOptional;
        } else if (!newTransactionIDOptional.get().equals(transactionIDOptional.get())) {
          recordOptional = newRecordOptional;
          continue;
        } else {
          throw new AssertionError("Transaction with transactionID " + transactionIDOptional.get()
            + " present on record does not exist.");
        }
      }
    }
  }

  private Map<String, TransactionalDatasetReader<?>> transactionalReaders(Map<String, DatasetReader<?>> readers) {
    return readers.entrySet().stream()
      .collect(toMap(HashMap.Entry::getKey,
        entry -> new TransactionalDatasetReader<>(entry.getValue(), this),
        (entry1, entry2) -> {throw new AssertionError("Duplicate Entry for a key found"); }
      ));
  }
}
