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
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import java.util.Optional;
import java.util.function.Predicate;

class TransactionalReadWriteRecordAccessor<K extends Comparable<K>> extends TransactionalReadRecordAccessor<K> implements ReadWriteRecordAccessor<K> {

  private final TransactionalGatewayWriterReader<K> transactionalGatewayWriterReader;

  TransactionalReadWriteRecordAccessor(K key, TransactionalGatewayWriterReader<K> transactionalGatewayWriterReader) {
    super(key, transactionalGatewayWriterReader);
    this.transactionalGatewayWriterReader = transactionalGatewayWriterReader;
  }

  @Override
  public TransactionalConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
    return new TransactionalConditionalReadWriteRecordAccessor<>(key, predicate, transactionalGatewayWriterReader);
  }

  @Override
  public Optional<Record<K>> add(Iterable<Cell<?>> cells) {
    return transactionalGatewayWriterReader.addReturnRecord(key, cells);
  }

  @Override
  public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
    return transactionalGatewayWriterReader.updateReturnTuple(key, transform);
  }

  @Override
  public Optional<Record<K>> delete() {
    return transactionalGatewayWriterReader.deleteReturnRecord(key);
  }
}
