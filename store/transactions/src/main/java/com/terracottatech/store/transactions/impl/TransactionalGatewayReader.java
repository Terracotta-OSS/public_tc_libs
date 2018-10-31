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
import com.terracottatech.store.Type;
import com.terracottatech.store.stream.RecordStream;
import com.terracottatech.store.transactions.exception.StoreTransactionRuntimeException;

import java.util.Optional;
import java.util.function.Predicate;

class TransactionalGatewayReader<K extends Comparable<K>> {
  private final DatasetReader<K> reader;
  private final ReadOnlyTransactionImpl<?> transaction;

  TransactionalGatewayReader(DatasetReader<K> reader, ReadOnlyTransactionImpl<?> transaction) {
    this.reader = reader;
    this.transaction = transaction;
  }

  Type<K> getKeyType() {
    if (!transaction.isActive()) {
      throw new StoreTransactionRuntimeException("transaction has already ended");
    }
    return reader.getKeyType();
  }

  Optional<Record<K>> get(K key) {
    synchronized (transaction) {
      return transaction.transactionalGet(reader.get(key), reader);
    }
  }

  Optional<Record<K>> get(K key, Predicate<? super Record<K>> predicate) {
    return get(key).filter(predicate);
  }

  RecordStream<K> nonMutableStream() {
    return new TransactionalRecordStream<>(transaction.transactionalRecordStream(reader));
  }
}
