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
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.stream.MutableRecordStream;

class TransactionalDatasetWriterReader<K extends Comparable<K>> extends TransactionalDatasetReader<K> implements DatasetWriterReader<K> {
  private final TransactionalGatewayWriterReader<K> transactionalGatewayWriterReader;

  TransactionalDatasetWriterReader(DatasetWriterReader<K> writerReader, ReadWriteTransactionImpl<?> transaction) {
    super(writerReader, transaction);
    this.transactionalGatewayWriterReader = new TransactionalGatewayWriterReader<>(writerReader, transaction);
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    return transactionalGatewayWriterReader.add(key, cells);
  }

  @Override
  public boolean update(K key, UpdateOperation<? super K> transform) {
    return transactionalGatewayWriterReader.update(key, transform);
  }

  @Override
  public boolean delete(K key) {
    return transactionalGatewayWriterReader.delete(key);
  }

  @Override
  public ReadWriteRecordAccessor<K> on(K key) {
    return new TransactionalReadWriteRecordAccessor<K>(key, transactionalGatewayWriterReader);
  }

  @Override
  public MutableRecordStream<K> records() {
    return transactionalGatewayWriterReader.mutableStream();
  }

  @Override
  public AsyncDatasetWriterReader<K> async() {
    throw new UnsupportedOperationException("Async Dataset readers and writerReaders are not supported with transactions");
  }
}