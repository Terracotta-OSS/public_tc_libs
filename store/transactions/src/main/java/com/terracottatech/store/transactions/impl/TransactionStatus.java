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

import java.util.function.Predicate;

import static com.terracottatech.store.transactions.impl.SystemTransactions.STATUS;
import static com.terracottatech.store.transactions.impl.TransactionID.isTransactionIDCell;
import static com.terracottatech.store.transactions.impl.TransactionImpl.TRANSACTIONAL_CELLNAME_PREFIX;
import static com.terracottatech.store.transactions.impl.TransactionImpl.TRANSACTIONAL_UNCOMMITED_CELL_PREFIX;
import static com.terracottatech.store.transactions.impl.TransactionOperation.ADD;
import static com.terracottatech.store.transactions.impl.TransactionOperation.DELETE;
import static com.terracottatech.store.transactions.impl.TransactionOperation.getOperation;
import static com.terracottatech.store.transactions.impl.TransactionOperation.isTransactionOperationCell;

enum TransactionStatus {
  EXECUTING {
    @Override
    <K extends Comparable<K>> Record<K> resolvedRecordImageForRead(Record<K> record) {
      return ROLLEDBACK.recordImage(record);
    }

    @Override
    <K extends Comparable<K>> boolean canRecordBeResolvedUsingDelete(Record<K> record) {
      return false;
    }

    @Override
    <K extends Comparable<K>> boolean canRecordBeResolvedUsingUpdate(Record<K> r) {
      return false;
    }

    @Override
    <K extends Comparable<K>> CellSet resolveUsingUpdate(Record<K> r) {
      throw new AssertionError("Executing transaction's record cannot be resolved");
    }

    @Override
    CellSet cellSetImage(Record<?> record) {
      throw new UnsupportedOperationException();
    }

    @Override
    <K extends Comparable<K>> Record<K> recordImage(Record<K> record) {
      throw new UnsupportedOperationException();
    }
  },
  COMMITTED {
    @Override
    <K extends Comparable<K>> Record<K> resolvedRecordImageForRead(Record<K> record) {
      return recordImage(record);
    }

    @Override
    <K extends Comparable<K>> boolean canRecordBeResolvedUsingDelete(Record<K> record) {
      return getOperation(record).get().equals(DELETE);
    }

    @Override
    <K extends Comparable<K>> boolean canRecordBeResolvedUsingUpdate(Record<K> r) {
      return !canRecordBeResolvedUsingDelete(r);
    }

    @Override
    <K extends Comparable<K>> CellSet resolveUsingUpdate(Record<K> r) {
      if (getOperation(r).get().equals(DELETE)) {
        throw new AssertionError("Committed deleted record cannot be resolved using update operation");
      }
      return cellSetImage(r);
    }

    @Override
    CellSet cellSetImage(Record<?> record) {
      CellSet newCells = new CellSet();
      record.forEach(c -> {
        if (c.definition().name().startsWith(TRANSACTIONAL_CELLNAME_PREFIX)
          && !isTransactionMetadataCell.test(c)) {
          String newCellName = c.definition().name().substring(TRANSACTIONAL_UNCOMMITED_CELL_PREFIX.length());
          newCells.add(Cell.cell(newCellName, c.value()));
        }
      });
      return newCells;
    }

    @Override
    <K extends Comparable<K>> Record<K> recordImage(Record<K> record) {
      if (record == null) {
        return null;
      }

      if (!getOperation(record).isPresent()) {
        return record;
      } else {
        if (getOperation(record).get().equals(DELETE)) {
          return null;
        } else {
          return new SimpleRecordImpl<>(record.getKey(), cellSetImage(record));
        }
      }
    }
  },
  ROLLEDBACK {
    @Override
    <K extends Comparable<K>> Record<K> resolvedRecordImageForRead(Record<K> record) {
      return recordImage(record);
    }

    @Override
    <K extends Comparable<K>> boolean canRecordBeResolvedUsingDelete(Record<K> record) {
      return getOperation(record).get().equals(ADD);
    }

    @Override
    <K extends Comparable<K>> boolean canRecordBeResolvedUsingUpdate(Record<K> r) {
      return !canRecordBeResolvedUsingDelete(r);
    }

    @Override
    <K extends Comparable<K>> CellSet resolveUsingUpdate(Record<K> r) {
      if (getOperation(r).get().equals(ADD)) {
        throw new AssertionError("Rollbacked added record cannot be resolved using update operation");
      }
      return cellSetImage(r);
    }

    @Override
    CellSet cellSetImage(Record<?> record) {
      CellSet newCells = new CellSet();
      record.forEach(c -> {
        if (!c.definition().name().startsWith(TRANSACTIONAL_CELLNAME_PREFIX)) {
          newCells.add(c);
        }
      });
      return newCells;
    }

    @Override
    <K extends Comparable<K>> Record<K> recordImage(Record<K> record) {
      if (record == null) {
        return null;
      }

      if (!getOperation(record).isPresent()) {
        return record;
      } else {
        if (getOperation(record).get().equals(ADD)) {
          return null;
        } else {
          return new SimpleRecordImpl<>(record.getKey(), cellSetImage(record));
        }
      }
    }
  };

  Cell<?> cell() {
    return STATUS.newCell(name());
  }

  abstract <K extends Comparable<K>> Record<K> resolvedRecordImageForRead(Record<K> record);
  abstract <K extends Comparable<K>> boolean canRecordBeResolvedUsingDelete(Record<K> record);
  abstract <K extends Comparable<K>> boolean canRecordBeResolvedUsingUpdate(Record<K> r);
  abstract <K extends Comparable<K>> CellSet resolveUsingUpdate(Record<K> r);

  abstract CellSet cellSetImage(Record<?> record);
  abstract <K extends Comparable<K>> Record<K> recordImage(Record<K> record);

  private static Predicate<Cell<?>> isTransactionMetadataCell
    = c -> isTransactionOperationCell(c) || isTransactionIDCell(c);
}
