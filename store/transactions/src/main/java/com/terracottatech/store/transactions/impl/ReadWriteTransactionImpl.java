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
import com.terracottatech.store.CellSet;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.api.TransactionalBiAction;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;
import com.terracottatech.store.transactions.exception.StoreTransactionTimeoutException;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static com.terracottatech.store.transactions.impl.SystemTransactions.getTransactionStatus;
import static com.terracottatech.store.transactions.impl.TransactionID.getTransactionID;
import static com.terracottatech.store.transactions.impl.TransactionOperation.ADD;
import static com.terracottatech.store.transactions.impl.TransactionOperation.DELETE;
import static com.terracottatech.store.transactions.impl.TransactionOperation.UPDATE;
import static com.terracottatech.store.transactions.impl.TransactionOperation.getOperation;
import static com.terracottatech.store.transactions.impl.TransactionStatus.EXECUTING;
import static com.terracottatech.store.transactions.impl.TransactionalRecordResolvingOperation.USING_DELETE;
import static com.terracottatech.store.transactions.impl.TransactionalRecordResolvingOperation.USING_UPDATE;
import static java.util.stream.Collectors.toMap;

class ReadWriteTransactionImpl<T> extends ReadOnlyTransactionImpl<T> implements TransactionController.ReadWriteTransaction {

  private final TransactionID transactionID;
  private final Map<String, TransactionalDatasetWriterReader<?>> writerReaders;
  private final TransactionalBiAction<Map<String, TransactionalDatasetWriterReader<?>>, Map<String, TransactionalDatasetReader<?>>, T> action;
  private final Map<DatasetWriterReader<?>, Set<Object>> dirtedData;

  ReadWriteTransactionImpl(SystemTransactions systemTransactions, Duration timeOut,
                           Map<String, DatasetReader<?>> readers,
                           Map<String, DatasetWriterReader<?>> writerReaders,
                           TransactionalBiAction<Map<String, TransactionalDatasetWriterReader<?>>, Map<String, TransactionalDatasetReader<?>>, T> action) {
    super(systemTransactions, timeOut, readers, null);
    this.writerReaders = transactionalWriterReaders(writerReaders);
    this.action = action;
    this.transactionID = systemTransactions.addRecordForTransaction(getStartTimeMillis(), getDeadlineMillis());
    dirtedData = new HashMap<>();
    writerReaders.values().forEach(dwr -> dirtedData.put(dwr, new HashSet<>()));
  }

  @Override
  T execute() throws Exception {
    T retVal;

    try {
      retVal = action.perform(writerReaders, readers);
    } catch (Exception e) {
      try {
        rollback();
      } catch (Exception rollbackException) {
        e.addSuppressed(rollbackException);
        throw e;
      }

      throw e;
    }

    commit();
    return retVal;
  }

  @Override
  public synchronized void commit() {
    checkStatus();

    if (!markInactive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }

    Optional<TransactionStatus> status = getSystemTransactions().markCommitted(transactionID);

    if (!status.isPresent()) {
      throw new AssertionError("Transaction information not present in SystemTransactions Dataset.");
    }

    switch (TransactionStatus.values()[status.get().ordinal()]) {
      case COMMITTED:
        throw new AssertionError("Transaction has already been marked as committed in SystemTransactions Dataset.");
      case EXECUTING:
        cleanUpTransaction();
        return;
      case ROLLEDBACK:
        cleanUpTransaction();
        throw new StoreTransactionTimeoutException("Transaction has already been rolled back due to timeout by some other transaction");
      default:
        throw new AssertionError();
    }
  }

  @Override
  public synchronized void rollback() {
    if (!markInactive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }

    if (!getSystemTransactions().markRollbacked(transactionID)) {
      throw new AssertionError("Transaction information does not exist in SystemTransactions Dataset or has already been marked as committed");
    }

    cleanUpTransaction();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K extends Comparable<K>> DatasetWriterReader<K> writerReader(String name) {
    if (!isActive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }
    return (DatasetWriterReader<K>) writerReaders.get(name);
  }

  void dirtied(DatasetWriterReader<?> writerReader, Object key) {
    dirtedData.get(writerReader).add(key);
  }

  @SuppressWarnings("unchecked")
  private void resolveDirtiedData() {
    dirtedData.entrySet()
      .forEach(entry -> entry.getValue().forEach(key -> resolve((DatasetWriterReader) entry.getKey(), (Comparable) key)));
  }

  private void cleanUpTransaction() {
    resolveDirtiedData();
    getSystemTransactions().deleteRecordForTransaction(transactionID);
  }

  @Override
  boolean identifiesSelf(TransactionID id) {
    return transactionID.equals(id);
  }

  <K extends Comparable<K>> boolean isDirty(Record<K> r) {
    return getTransactionID(r).isPresent();
  }

  <K extends Comparable<K>> boolean dirtiedBySelf(Record<K> r) {
    Optional<TransactionID> recordsTransactionID = getTransactionID(r);
    return recordsTransactionID.isPresent() && transactionID.equals(recordsTransactionID.get());
  }

  <K extends Comparable<K>> boolean dirtiedByOthers(Record<K> r) {
    Optional<TransactionID> recordsTransactionID = getTransactionID(r);
    return recordsTransactionID.isPresent() && !transactionID.equals(recordsTransactionID.get());
  }

  <K extends Comparable<K>> boolean addedBySelf(Record<K> r) {
    return dirtiedBySelf(r) && getOperation(r).get().equals(ADD);
  }

  <K extends Comparable<K>> boolean isRecordUpdatable(Record<K> r, Predicate<? super Record<K>> predicate) {
    return ((!isDirty(r) && predicate.test(r))
      || (dirtiedBySelf(r) && getOperation(r).get().isSelfDirtiedRecordUpdatable(r, predicate)));
  }

  <K extends Comparable<K>> boolean isRecordDeletableUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
    return ((!isDirty(r) && predicate.test(r))
      || (dirtiedBySelf(r) && getOperation(r).get().isSelfDirtiedRecordDeletableUsingUpdate(r, predicate)));
  }

  <K extends Comparable<K>> boolean deletionRejectedUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
    return ((!isDirty(r) && !predicate.test(r))
      || (dirtiedBySelf(r) && getOperation(r).get().isSelfDirtiedRecordDeleteRejectedUsingUpdate(r, predicate)));
  }

  // NOTE: this method should only be used as Predicate for ConditionalReadWriteAccessors as this method does not
  // only work on passed in record but it may refetch the record us using the passed in DatasetReader.
  // This should not be a problem given the loop implementation of CRUD in ConditionalReadWriteAccessors
  <K extends Comparable<K>> Predicate<? super Record<K>> notUncommittedAddByEXECUTINGOthers(DatasetReader<K> reader) {
    return record -> {
      while (true) {
        Optional<TransactionOperation> operationOptional = getOperation(record);
        if (!operationOptional.isPresent() || !operationOptional.get().equals(ADD)) {
          return true;
        }

        if (dirtiedBySelf(record)) {
          return true;
        }

        Optional<TransactionStatus> status = getSystemTransactions().getTransactionStatus(getTransactionID(record).get());

        // If SystemTransactions record not present, record should have changed
        if (!status.isPresent()) {
          Optional<Record<K>> recordOptional = reader.get(record.getKey());
          if (!recordOptional.isPresent()) {
            return false;
          } else if (!recordOptional.get().equals(record)) {
            record = recordOptional.get();
            continue;
          } else {
            throw new AssertionError("No data about the transactionID " + transactionID + " found in the record exists in the system.");
          }
        }

        return !status.get().equals(EXECUTING);
      }
    };
  }

  private Map<String, TransactionalDatasetWriterReader<?>> transactionalWriterReaders(
    Map<String, DatasetWriterReader<?>> writerReaders) {
    return writerReaders.entrySet().stream()
      .collect(toMap(HashMap.Entry::getKey,
        entry -> new TransactionalDatasetWriterReader<>(entry.getValue(), this),
        (entry1, entry2) -> {
          throw new AssertionError("Duplicate Entry for a key found");
        }
      ));
  }

  <K extends Comparable<K>> void tryResolve(DatasetWriterReader<K> writerReader, K key,
                                            TransactionLockWaitTimeFunction waitTimeFunction) {
    Optional<Record<String>> systemTransactionRecordOfHoldingTransaction = resolve(writerReader, key);
    if (systemTransactionRecordOfHoldingTransaction.isPresent()) {
      boolean interrupted = Thread.interrupted();
      try {
        try {
          Long waitTimeMillis = waitTimeFunction.apply(systemTransactionRecordOfHoldingTransaction.get());
          if (waitTimeMillis > 0) {
            wait(waitTimeMillis);
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  // returns empty Optional if it resolved or the record is already resolved,
  // else returns Optional of SytemTransactions record corresponding to the record
  private <K extends Comparable<K>> Optional<Record<String>> resolve(DatasetWriterReader<K> writerReader, K key) {

    Optional<Record<K>> recordOptional = writerReader.get(key);

    // record not present
    if (!recordOptional.isPresent()) {
      return Optional.empty();
    }

    Record<K> record = recordOptional.get();

    // already resolved
    if (!getTransactionID(record).isPresent()) {
      return Optional.empty();
    }

    // try to delete the record, rollbacked added record or commit deleted record can be deleted
    final AtomicReference<Optional<Record<String>>> systemTransactionsRecord = new AtomicReference<>(Optional.empty());
    if (canBeResolved(USING_DELETE, systemTransactionsRecord, writerReader).test(record)) {
      if (writerReader.on(key)
        .iff(canBeResolved(USING_DELETE, systemTransactionsRecord, writerReader))
        .delete().isPresent()) {
        return Optional.empty();
      }
    }

    if (!systemTransactionsRecord.get().isPresent()) {
      return Optional.empty();
    }

    // try to resolve by update
    if (canBeResolved(USING_UPDATE, systemTransactionsRecord, writerReader).test(record)) {
      if (writerReader.on(key)
        .iff(canBeResolved(USING_UPDATE, systemTransactionsRecord, writerReader))
        .update(UpdateOperation.custom(r -> getTransactionStatus(systemTransactionsRecord.get().get()).resolveUsingUpdate(r)))
        .isPresent()) {
        return Optional.empty();
      }
    }

    return systemTransactionsRecord.get();
  }

  private <K extends Comparable<K>> Predicate<Record<K>> canBeResolved(TransactionalRecordResolvingOperation resolvingOperation,
                                                               AtomicReference<Optional<Record<String>>> systemTransactionsRecord, DatasetReader<K> reader) {
    return record -> {
      systemTransactionsRecord.set(Optional.empty());
      while (true) {
        Optional<TransactionID> transactionIDOptional = getTransactionID(record);

        if (!transactionIDOptional.isPresent()) {
          return false;
        }

        Optional<Record<String>> transactionRecord = getSystemTransactions().getTransactionsRecord(transactionIDOptional.get());

        // Recheck the record if it still has the same transactionID
        if (!transactionRecord.isPresent()) {
          Optional<Record<K>> recordOptional = reader.get(record.getKey());

          if (!recordOptional.isPresent()) {
            return false;
          } else if (!recordOptional.get().equals(record)) {
            record = recordOptional.get();
            continue;
          } else {
            throw new AssertionError("No data about the transactionID " + transactionID + " found in the record exists in the system.");
          }
        }

        systemTransactionsRecord.set(transactionRecord);
        TransactionStatus transactionStatus = getTransactionStatus(transactionRecord.get());
        return resolvingOperation.canBeResolvedUsingFunction().apply(transactionStatus, record);
      }
    };
  }

  CellSet addOperationCells(Iterable<Cell<?>> cells) {
    return addOperationCells(cells, transactionID);
  }

  CellSet deleteOperationCells(Iterable<Cell<?>> cells) {
    return deleteOperationCells(cells, transactionID);
  }

  <K extends Comparable<K>> CellSet updateOperationCells(Record<K> record, UpdateOperation<K> transform) {
    return updateOperationCells(record, transform, transactionID);
  }

  static CellSet addOperationCells(Iterable<Cell<?>> cells, TransactionID transactionID) {
    CellSet newCells = new CellSet();
    cells.forEach(c -> {
      String newCellName = TRANSACTIONAL_UNCOMMITED_CELL_PREFIX + c.definition().name();
      newCells.add(Cell.cell(newCellName, c.value()));
    });
    newCells.add(transactionID.cell());
    newCells.add(ADD.cell());
    return newCells;
  }

  static CellSet deleteOperationCells(Iterable<Cell<?>> cells, TransactionID transactionID) {
    CellSet newCells = new CellSet(cells);
    newCells.add(transactionID.cell());
    newCells.add(DELETE.cell());
    return newCells;
  }

  static <K extends Comparable<K>> CellSet updateOperationCells(Record<K> record, UpdateOperation<K> transform,
                                                                TransactionID transactionID) {
    CellSet updatedCells = new CellSet(transform.apply(record));
    CellSet newCells = new CellSet(record);
    updatedCells.forEach(c -> {
      String newCellName = TRANSACTIONAL_UNCOMMITED_CELL_PREFIX + c.definition().name();
      newCells.add(Cell.cell(newCellName, c.value()));
    });
    newCells.add(transactionID.cell());
    newCells.add(UPDATE.cell());
    return newCells;
  }
}
