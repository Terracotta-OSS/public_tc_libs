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

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.async.AsyncDatasetReader;
import com.terracottatech.store.stream.RecordStream;

import java.util.Optional;

class TransactionalDatasetReader<K extends Comparable<K>> implements DatasetReader<K> {
  private final TransactionalGatewayReader<K> transactionalGatewayReader;

  TransactionalDatasetReader(DatasetReader<K> reader, ReadOnlyTransactionImpl<?> transaction) {
    this.transactionalGatewayReader = new TransactionalGatewayReader<>(reader, transaction);
  }

  @Override
  public Type<K> getKeyType() {
    return transactionalGatewayReader.getKeyType();
  }

  @Override
  public Optional<Record<K>> get(K key) {
    return transactionalGatewayReader.get(key);
  }

  @Override
  public ReadRecordAccessor<K> on(K key) {
    return new TransactionalReadRecordAccessor<>(key, transactionalGatewayReader);
  }

  @Override
  public RecordStream<K> records() {
    return transactionalGatewayReader.nonMutableStream();
  }

  @Override
  public AsyncDatasetReader<K> async() {
    throw new UnsupportedOperationException("Async Dataset readers and writerReaders are not supported with transactions");
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    throw new UnsupportedOperationException("ChangeListeners are not supported with transactions");
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    throw new UnsupportedOperationException("ChangeListeners are not supported with transactions");
  }
}
