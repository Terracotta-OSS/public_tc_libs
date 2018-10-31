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
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;

import java.util.Optional;
import java.util.function.Predicate;

import static com.terracottatech.store.transactions.impl.TransactionID.getTransactionID;
import static com.terracottatech.store.transactions.impl.TransactionOperation.getOperation;
import static com.terracottatech.store.transactions.impl.TransactionStatus.COMMITTED;

class TransactionalGatewayWriterReader<K extends Comparable<K>> extends TransactionalGatewayReader<K> {
  private final DatasetWriterReader<K> writerReader;
  private final ReadWriteTransactionImpl<?> transaction;

  TransactionalGatewayWriterReader(DatasetWriterReader<K> writerReader, ReadWriteTransactionImpl<?> transaction) {
    super(writerReader, transaction);
    this.writerReader = writerReader;
    this.transaction = transaction;
  }

  boolean add(K key, Iterable<Cell<?>> cells) {
    // TODO: can be optimized as only boolean is to be returned
    return !addReturnRecord(key, cells).isPresent();
  }

  Optional<Record<K>> addReturnRecord(K key, Iterable<Cell<?>> cells) {
    synchronized (transaction) {
      TransactionLockWaitTimeFunction waitTimeFunction = new TransactionLockWaitTimeFunction(transaction);
      CellSet newCells = transaction.addOperationCells(cells);

      while (true) {
        transaction.checkStatus();

        // try adding the record
        Optional<Record<K>> addResult = writerReader.on(key).add(newCells);
        if (!addResult.isPresent()) {
          transaction.dirtied(writerReader, key);
          return Optional.empty();
          // If clean record present, do nothing to the record
        } else if (!getTransactionID(addResult.get()).isPresent()) {
          return addResult;
          // Dirtied by self
        } else if (transaction.dirtiedBySelf(addResult.get())) {
          Optional<Tuple<Record<K>, Record<K>>> updateResult
            = writerReader.on(key)
            .iff(transaction::dirtiedBySelf)
            .update(UpdateOperation.custom(r -> getOperation(r).get().addOnSelfDirtiedRecord(r, cells)));

          if (updateResult.isPresent()) {
            return Optional.ofNullable(COMMITTED.recordImage(updateResult.get().getFirst()));
          }
        }

        // Dirtied by others, try to resolve and then retry
        transaction.tryResolve(writerReader, key, waitTimeFunction);
      }
    }
  }

  boolean update(K key, UpdateOperation<? super K> transform) {
    // TODO: can be optimized, as only boolean is to be returned
    return updateReturnTuple(key, transform).isPresent();
  }

  Optional<Tuple<Record<K>, Record<K>>> updateReturnTuple(K key, UpdateOperation<? super K> transform) {
    // TODO: sending always true predicate may not be the most optimized way to do it
    return updateReturnTuple(key, r -> true, transform);
  }

  Optional<Tuple<Record<K>, Record<K>>> updateReturnTuple(K key, Predicate<? super Record<K>> predicate, UpdateOperation<? super K> transform) {
    synchronized (transaction) {

      TransactionLockWaitTimeFunction waitTimeFunction = new TransactionLockWaitTimeFunction(transaction);

      while (true) {
        transaction.checkStatus();

        Optional<Tuple<Record<K>, Record<K>>> updateResult
          = writerReader.on(key)
          .iff(transaction.notUncommittedAddByEXECUTINGOthers(writerReader))
          .update(UpdateOperation.custom(r -> {

            // if record is clean, update it
            if (!transaction.isDirty(r)) {
              if (!predicate.test(r)) {
                return r;
              }
              transaction.dirtied(writerReader, key);
              @SuppressWarnings("unchecked")
              UpdateOperation<K> updateOperation = (UpdateOperation<K>) transform;
              return transaction.updateOperationCells(r, updateOperation);
            }

            // if dirtied by self
            if (transaction.dirtiedBySelf(r)) {
              return getOperation(r).get().updateOnSelfDirtiedRecord(r, predicate, transform);
            } else {
              // Dirtied by others, try to resolve and then retry the update
              return r;
            }
          }));

        // record absent, nothing to update
        if (!updateResult.isPresent()) {
          return Optional.empty();
        }

        Record<K> first = updateResult.get().getFirst();
        Record<K> second = updateResult.get().getSecond();

        if (!transaction.dirtiedByOthers(first)) {
          if (transaction.isRecordUpdatable(first, predicate)) {
            return Optional.of(Tuple.of(COMMITTED.recordImage(first), COMMITTED.recordImage(second)));
          } else {
            return Optional.empty();
          }
        }

        // try to resolve and then retry
        transaction.tryResolve(writerReader, key, waitTimeFunction);
      }
    }
  }

  boolean delete(K key) {
    // TODO: can be optimized, as only boolean is to be returned
    return deleteReturnRecord(key).isPresent();
  }

  Optional<Record<K>> deleteReturnRecord(K key) {
    // TODO: sending always true predicate may not be the most optimized way to do it
    return deleteReturnRecord(key, r -> true);
  }

  Optional<Record<K>> deleteReturnRecord(K key, Predicate<? super Record<K>> predicate) {
    synchronized (transaction) {

      TransactionLockWaitTimeFunction waitTimeFunction = new TransactionLockWaitTimeFunction(transaction);

      while (true) {
        transaction.checkStatus();

        Optional<Tuple<Record<K>, Record<K>>> updateResult = writerReader.on(key)
          .iff(transaction.notUncommittedAddByEXECUTINGOthers(writerReader))
          .update(UpdateOperation.custom(r -> {

            // if record is clean, mark it deleted
            if (!transaction.isDirty(r)) {
              // if predicate not satisfied, cannot be deleted
              if (!predicate.test(r)) {
                return r;
              }
              transaction.dirtied(writerReader, key);
              return transaction.deleteOperationCells(r);
            }

            // if dirtied by self
            if (transaction.dirtiedBySelf(r)) {
              return getOperation(r).get().deleteOnSelfDirtiedRecordUsingUpdate(r, predicate);
            } else {
              // Dirtied by others, try to resolve and then retry the delete
              return r;
            }
          }));

        // update failed because record was not present, hence return false
        if (!updateResult.isPresent()) {
          return Optional.empty();
        }

        Record<K> preImage = updateResult.get().getFirst();

        if (!transaction.dirtiedByOthers(preImage)) {
          if (transaction.isRecordDeletableUsingUpdate(preImage, predicate)) {
            return Optional.of(COMMITTED.recordImage(preImage));
          } else if (transaction.deletionRejectedUsingUpdate(preImage, predicate)) {
            return Optional.empty();
          }

          // if added by self and predicate is satisfied, then delete the record
          // NOTE: the case of added by self and predicate not satisfied case is handled inside the custom update
          Optional<Record<K>> deleteResult = writerReader.on(key)
            .iff(r -> (transaction.addedBySelf(r) && predicate.test(COMMITTED.recordImage(r))))
            .delete();

          if (deleteResult.isPresent()) {
            return Optional.of(COMMITTED.recordImage(deleteResult.get()));
          }
        }

        // try to resolve and then retry
        transaction.tryResolve(writerReader, key, waitTimeFunction);
      }
    }
  }

  MutableRecordStream<K> mutableStream() {
    return new TransactionalMutableRecordStream<>(transaction.transactionalRecordStream(writerReader));
  }
}
