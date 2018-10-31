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
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import java.util.Optional;
import java.util.function.Predicate;

import static com.terracottatech.store.transactions.impl.ReadWriteTransactionImpl.addOperationCells;
import static com.terracottatech.store.transactions.impl.ReadWriteTransactionImpl.deleteOperationCells;
import static com.terracottatech.store.transactions.impl.ReadWriteTransactionImpl.updateOperationCells;
import static com.terracottatech.store.transactions.impl.TransactionID.getTransactionID;
import static com.terracottatech.store.transactions.impl.TransactionImpl.TRANSACTIONAL_CELLNAME_PREFIX;
import static com.terracottatech.store.transactions.impl.TransactionStatus.COMMITTED;
import static com.terracottatech.store.transactions.impl.TransactionStatus.ROLLEDBACK;

enum TransactionOperation {
  ADD {
    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> addOnSelfDirtiedRecord(Record<K> r, Iterable<Cell<?>> newCells) {
      return r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordUpdatable(Record<K> r, Predicate<? super Record<K>> predicate) {
      return predicate.test(COMMITTED.recordImage(r));
    }

    @SuppressWarnings("unchecked")
    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> updateOnSelfDirtiedRecord(Record<K> r, Predicate<? super Record<K>> predicate, UpdateOperation<? super K> transform) {
      return isSelfDirtiedRecordUpdatable(r, predicate) ? updateOnSelfAddedRecord(r, (UpdateOperation<K>) transform) : r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordDeletableUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return false;
    }

    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> deleteOnSelfDirtiedRecordUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordDeleteRejectedUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return !predicate.test(COMMITTED.recordImage(r));
    }
  },
  DELETE {
    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> addOnSelfDirtiedRecord(Record<K> r, Iterable<Cell<?>> newCells) {
      return addOnSelfDeletedRecord(r, newCells);
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordUpdatable(Record<K> r, Predicate<? super Record<K>> predicate) {
      return false;
    }

    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> updateOnSelfDirtiedRecord(Record<K> r, Predicate<? super Record<K>> predicate, UpdateOperation<? super K> transform) {
      return r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordDeletableUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return false;
    }

    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> deleteOnSelfDirtiedRecordUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordDeleteRejectedUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return true;
    }
  },
  UPDATE {
    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> addOnSelfDirtiedRecord(Record<K> r, Iterable<Cell<?>> newCells) {
      return r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordUpdatable(Record<K> r, Predicate<? super Record<K>> predicate) {
      return predicate.test(COMMITTED.recordImage(r));
    }

    @SuppressWarnings("unchecked")
    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> updateOnSelfDirtiedRecord(Record<K> r, Predicate<? super Record<K>> predicate, UpdateOperation<? super K> transform) {
      return isSelfDirtiedRecordUpdatable(r, predicate) ? updateOnSelfUpdatedRecord(r, (UpdateOperation<K>) transform) : r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordDeletableUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return predicate.test(COMMITTED.recordImage(r));
    }

    @Override
    <K extends Comparable<K>> Iterable<Cell<?>> deleteOnSelfDirtiedRecordUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return isSelfDirtiedRecordDeletableUsingUpdate(r, predicate)
        ? deleteOperationCells(ROLLEDBACK.cellSetImage(r), getTransactionID(r).get())
        : r;
    }

    @Override
    <K extends Comparable<K>> boolean isSelfDirtiedRecordDeleteRejectedUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate) {
      return !predicate.test(COMMITTED.recordImage(r));
    }
  };

  private static final String TRANSACTIONAL_OPERATION_TYPE_CELLNAME = TRANSACTIONAL_CELLNAME_PREFIX + "operation";
  private static final StringCellDefinition TRANSACTION_OPERATION = CellDefinition.defineString(TRANSACTIONAL_OPERATION_TYPE_CELLNAME);

  static Optional<TransactionOperation> getOperation(Record<?> r) {
    Optional<String> optionalOperation = r.get(TRANSACTION_OPERATION);

    return optionalOperation.map(TransactionOperation::valueOf);
  }

  static boolean isTransactionOperationCell(Cell<?> cell) {
    return cell.definition().equals(TRANSACTION_OPERATION);
  }

  Cell<?> cell() {
    return TRANSACTION_OPERATION.newCell(name());
  }

  abstract <K extends Comparable<K>> Iterable<Cell<?>> addOnSelfDirtiedRecord(Record<K> r, Iterable<Cell<?>> newCells);

  abstract <K extends Comparable<K>> boolean isSelfDirtiedRecordUpdatable(Record<K> r, Predicate<? super Record<K>> predicate);
  abstract <K extends Comparable<K>> Iterable<Cell<?>> updateOnSelfDirtiedRecord(Record<K> r, Predicate<? super Record<K>> predicate, UpdateOperation<? super K> transform);

  abstract <K extends Comparable<K>> boolean isSelfDirtiedRecordDeletableUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate);
  abstract <K extends Comparable<K>> Iterable<Cell<?>> deleteOnSelfDirtiedRecordUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate);
  abstract <K extends Comparable<K>> boolean isSelfDirtiedRecordDeleteRejectedUsingUpdate(Record<K> r, Predicate<? super Record<K>> predicate);

  private static <K extends Comparable<K>> CellSet addOnSelfDeletedRecord(Record<K> r, Iterable<Cell<?>> newCells) {
    Record<K> preDeleteCleanRecord = new SimpleRecordImpl<>(r.getKey(), ROLLEDBACK.cellSetImage(r));
    UpdateOperation<K> transform = rec -> newCells;
    return updateOperationCells(preDeleteCleanRecord, transform, getTransactionID(r).get());
  }

  private static <K extends Comparable<K>> CellSet updateOnSelfAddedRecord(Record<K> r, UpdateOperation<K> transform) {
    Record<K> record = COMMITTED.recordImage(r);
    TransactionID transactionID = getTransactionID(r).get();
    CellSet updatedCells = updateOperationCells(record, transform, transactionID);
    Record<K> updatedRecord = new SimpleRecordImpl<>(r.getKey(), updatedCells);
    return addOperationCells(COMMITTED.cellSetImage(updatedRecord), transactionID);
  }

  private static <K extends Comparable<K>> CellSet updateOnSelfUpdatedRecord(Record<K> r, UpdateOperation<K> transform) {
    Record<K> preUpdateCleanRecord = new SimpleRecordImpl<>(r.getKey(), ROLLEDBACK.cellSetImage(r));
    Record<K> updatedCleanRecord = COMMITTED.recordImage(r);
    UpdateOperation<K> newTransform = rec -> transform.apply(updatedCleanRecord);
    return updateOperationCells(preUpdateCleanRecord, newTransform, getTransactionID(r).get());
  }
}
