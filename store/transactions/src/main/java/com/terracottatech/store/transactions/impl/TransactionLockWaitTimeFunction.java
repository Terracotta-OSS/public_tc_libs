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

import com.terracottatech.store.Record;

import java.util.Optional;

import static com.terracottatech.store.transactions.impl.SystemTransactions.getTransactionDeadline;
import static com.terracottatech.store.transactions.impl.SystemTransactions.getTransactionID;

class TransactionLockWaitTimeFunction {

  private static final long ZERO_WAIT_TIME = 0;
  private static final long STARTING_WAIT_TIME_MILLIS = 10;
  private long waitTimeMillis = ZERO_WAIT_TIME;
  private final ReadWriteTransactionImpl<?> transaction;
  private Optional<TransactionID> recordsTransactionID = Optional.empty();
  private Optional<Long> recordsTransactionDeadline = Optional.empty();

  TransactionLockWaitTimeFunction(ReadWriteTransactionImpl<?> transaction) {
    this.transaction = transaction;
  }

  Long apply(Record<String> systemTransactionsRecordOfHoldingTransaction) {

    if (!recordsTransactionID.isPresent()) {
      setLocalRecordsTransactionIDAndDeadline(systemTransactionsRecordOfHoldingTransaction);

      if (!recordsTransactionID.isPresent()) {
        waitTimeMillis = STARTING_WAIT_TIME_MILLIS;
        return waitTimeMillis;
      }
    } else if (!recordsTransactionID.get().equals(getTransactionID(systemTransactionsRecordOfHoldingTransaction))){
      setLocalRecordsTransactionIDAndDeadline(systemTransactionsRecordOfHoldingTransaction);
      waitTimeMillis = STARTING_WAIT_TIME_MILLIS;
      return waitTimeMillis;
    }

    long currentTimeMillis = TimeSource.currentTimeMillis();

    if (recordsTransactionDeadline.get() >= currentTimeMillis) {
      waitTimeMillis = ZERO_WAIT_TIME;
      return waitTimeMillis;
    }

    long newWaitTimeMillis = (waitTimeMillis == ZERO_WAIT_TIME) ? STARTING_WAIT_TIME_MILLIS : (waitTimeMillis * 2);
    long newWakeUpTimeMillis = currentTimeMillis + newWaitTimeMillis;

    if (recordsTransactionDeadline.get() < transaction.getDeadlineMillis()) {
      if (newWakeUpTimeMillis > recordsTransactionDeadline.get()) {
        newWaitTimeMillis = recordsTransactionDeadline.get() - currentTimeMillis;
      }
    } else {
      if (newWaitTimeMillis >= transaction.getDeadlineMillis()) {
        newWaitTimeMillis = (transaction.getDeadlineMillis() - currentTimeMillis) * 9/10;
      }
    }

    waitTimeMillis = newWaitTimeMillis;
    return waitTimeMillis;
  }

  // returns true if transactions is still executing else returns false
  private void setLocalRecordsTransactionIDAndDeadline(Record<String> systemTransactionsRecordOfHoldingTransaction) {
    recordsTransactionDeadline = getTransactionDeadline(systemTransactionsRecordOfHoldingTransaction);
    if (!recordsTransactionDeadline.isPresent()) {
      recordsTransactionID = Optional.empty();
    } else {
      recordsTransactionID = Optional.of(getTransactionID(systemTransactionsRecordOfHoldingTransaction));
    }
  }
}
