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

import com.terracottatech.store.Cell;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.terracottatech.store.transactions.impl.TransactionStatus.COMMITTED;
import static com.terracottatech.store.transactions.impl.TransactionStatus.EXECUTING;
import static com.terracottatech.store.transactions.impl.TransactionStatus.ROLLEDBACK;
import static java.util.stream.Collectors.toSet;

class SystemTransactions implements AutoCloseable {

  private static final String SYSTEMTRANSACTIONS_DATASET_NAME = "SystemTransactions";
  private static final Type<String> SYSTEMTRANSACTIONS_KEY_TYPE = Type.STRING;
  private static final LongCellDefinition START_TIME = CellDefinition.defineLong("startTime");
  private static final LongCellDefinition DEADLINE = CellDefinition.defineLong("deadline");
  static final StringCellDefinition STATUS = CellDefinition.defineString("status");

  private final Dataset<String> dataset;
  private final DatasetWriterReader<String> writerReader;
  private boolean closed = false;

  SystemTransactions(DatasetManager datasetManager) throws StoreException {
    this.dataset = datasetManager.getDataset(SYSTEMTRANSACTIONS_DATASET_NAME, SYSTEMTRANSACTIONS_KEY_TYPE);
    this.writerReader = dataset.writerReader();
  }

  SystemTransactions(DatasetManager datasetManager, DatasetConfigurationBuilder datasetConfigurationBuilder) throws StoreException {
    datasetManager.newDataset(SYSTEMTRANSACTIONS_DATASET_NAME, SYSTEMTRANSACTIONS_KEY_TYPE,
      durableDatasetConfigurationBuilder(datasetConfigurationBuilder));
    this.dataset = datasetManager.getDataset(SYSTEMTRANSACTIONS_DATASET_NAME, SYSTEMTRANSACTIONS_KEY_TYPE);
    this.writerReader = dataset.writerReader();
  }

  private static DatasetConfigurationBuilder durableDatasetConfigurationBuilder(DatasetConfigurationBuilder builder) {
    return builder.build().getDiskDurability().isPresent() ? builder : builder.durabilityEveryMutation();
  }

  TransactionID addRecordForTransaction(long startTimeMillis, long deadlineMillis) {
    checkIfClosed();
    while (true) {
      TransactionID transactionID = new TransactionID();

      if (writerReader.add(transactionID.getSystemTransactionsKeyValue(),
        START_TIME.newCell(startTimeMillis),
        DEADLINE.newCell(deadlineMillis),
        EXECUTING.cell())) {
        return transactionID;
      }
    }
  }

  /**
   * Try to commit the transaction.
   *
   * @return  Optional of TransactionStatus that existed before the update here
   *          empty Optional, if record for transaction not present
   */
  Optional<TransactionStatus> markCommitted(TransactionID transactionID) {
    checkIfClosed();
    // try to commit the transaction
    Optional<Tuple<Record<String>, Record<String>>> updateOutput =
      writerReader.on(transactionID.getSystemTransactionsKeyValue())
        .update(UpdateOperation.custom(record -> {
            Set<Cell<?>> cells = new HashSet<>(record);

            // transaction already ended
            if (!record.get(STATUS).get().equals(EXECUTING.name())) {
              return cells;
            }

            // mark the transaction as committed
            Set<Cell<?>> newCells = cells.stream()
              .filter(c -> !c.definition().equals(STATUS))
              .collect(toSet());
            newCells.add(COMMITTED.cell());

            return newCells;
          }
        ));

    return updateOutput.map(recordRecordTuple -> TransactionStatus.valueOf(recordRecordTuple.getFirst().get(STATUS).get()));
  }

  boolean markRollbacked(TransactionID transactionID) {
    checkIfClosed();
    return writerReader.on(transactionID.getSystemTransactionsKeyValue())
      .iff(STATUS.valueOrFail().is(EXECUTING.name()))
      .update(UpdateOperation.write(ROLLEDBACK.cell()))
      .isPresent()
      || writerReader.on(transactionID.getSystemTransactionsKeyValue())
      .iff(STATUS.valueOrFail().is(ROLLEDBACK.name()))
      .read()
      .isPresent();
  }

  /**
   * Deletes the record with given transactionID
   * Called when no record in any dataset has the transactionID of this transaction.
   */
  void deleteRecordForTransaction(TransactionID transactionID) {
    checkIfClosed();
    if (!writerReader.get(transactionID.getSystemTransactionsKeyValue()).isPresent()) {
      throw new AssertionError("No record present for transactionID: " + transactionID + " in SystemTransactionsDataset");
    }

    writerReader.on(transactionID.getSystemTransactionsKeyValue()).delete();
  }

  /**
   * Gets the status of transaction corresponding to given transaction id.
   * Marks the transaction rolled back if it has timeout and is still in executing state.
   *
   * @return transaction status if the record for the transaction exists, else empty Optional
   */
  Optional<TransactionStatus> getTransactionStatus(TransactionID transactionID) {
    checkIfClosed();
    // If timed out and still executing, then rollback
    writerReader.on(transactionID.getSystemTransactionsKeyValue())
      .iff(isTimedOutAndExecutingTransaction())
      .update(UpdateOperation.write(ROLLEDBACK.cell()));

    Optional<String> status = writerReader.on(transactionID.getSystemTransactionsKeyValue()).read(STATUS.valueOrFail());

    return status.map(TransactionStatus::valueOf);
  }

  static TransactionStatus getTransactionStatus(Record<String> systemTransactionsRecord) {
    return TransactionStatus.valueOf(systemTransactionsRecord.get(STATUS).get());
  }

  static TransactionID getTransactionID(Record<String> systemTransactionsRecord) {
    return TransactionID.getTransactionID(systemTransactionsRecord.getKey());
  }

  static Optional<Long> getTransactionDeadline(Record<String> systemTransactionsRecord) {
    return systemTransactionsRecord.get(STATUS).get().equals(EXECUTING.name())
      ? Optional.of(systemTransactionsRecord.get(DEADLINE).get())
      : Optional.empty();
  }

  Optional<Record<String>> getTransactionsRecord(TransactionID transactionID) {
    checkIfClosed();
    // If timed out and still executing, then rollback
    writerReader.on(transactionID.getSystemTransactionsKeyValue())
      .iff(isTimedOutAndExecutingTransaction())
      .update(UpdateOperation.write(ROLLEDBACK.cell()));

    return writerReader.get(transactionID.getSystemTransactionsKeyValue());
  }

  @Override
  public void close() {
    closed = true;
    dataset.close();
  }

  void checkIfClosed() {
    if (closed) {
      throw new StoreTransactionRuntimeException("SystemTransactions Dataset has already been closed");
    }
  }

  private BuildablePredicate<Record<?>> isTimedOutAndExecutingTransaction() {
    return DEADLINE.longValueOrFail().isLessThan(TimeSource.currentTimeMillis())
      .and(STATUS.valueOrFail().is(EXECUTING.name()));
  }
}
