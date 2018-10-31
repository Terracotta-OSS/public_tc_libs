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

import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

class TransactionalReadRecordAccessor<K extends Comparable<K>> implements ReadRecordAccessor<K> {

  protected final K key;
  private final TransactionalGatewayReader<K> transactionalGatewayReader;

  TransactionalReadRecordAccessor(K key, TransactionalGatewayReader<K> transactionalGatewayReader) {
    this.key = key;
    this.transactionalGatewayReader = transactionalGatewayReader;
  }

  @Override
  public TransactionalConditionalReadRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
    return new TransactionalConditionalReadRecordAccessor<>(key, predicate, transactionalGatewayReader);
  }

  @Override
  public <T> Optional<T> read(Function<? super Record<K>, T> mapper) {
    return transactionalGatewayReader.get(key).map(mapper);
  }
}
