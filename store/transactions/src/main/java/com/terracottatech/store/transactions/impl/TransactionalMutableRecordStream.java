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

import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

class TransactionalMutableRecordStream<K extends Comparable<K>> extends TransactionalRecordStream<K> implements MutableRecordStream<K> {

  private final Stream<Record<K>> delegate;

  TransactionalMutableRecordStream(Stream<Record<K>> delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public MutableRecordStream<K> explain(Consumer<Object> consumer) {
    return wrap(((RecordStream<K>) delegate).explain(consumer));
  }

  @Override
  public MutableRecordStream<K> batch(int sizeHint) {
    return wrap(((RecordStream<K>) delegate).batch(sizeHint));
  }

  @Override
  public MutableRecordStream<K> inline() {
    return wrap(((RecordStream<K>) delegate).inline());
  }

  @Override
  public MutableRecordStream<K> filter(Predicate<? super Record<K>> predicate) {
    return wrap(delegate.filter(predicate));
  }

  @Override
  public MutableRecordStream<K> distinct() {
    return wrap(delegate.distinct());
  }

  @Override
  public MutableRecordStream<K> sorted() {
    throw new UnsupportedOperationException("sorted() is not supported - Record is not Comparable, what you mean probably is sorted(keyFunction().asComparator())");
  }

  @Override
  public MutableRecordStream<K> sorted(Comparator<? super Record<K>> comparator) {
    return wrap(delegate.sorted(comparator));
  }

  @Override
  public MutableRecordStream<K> peek(Consumer<? super Record<K>> action) {
    return wrap(delegate.peek(action));
  }

  @Override
  public MutableRecordStream<K> limit(long maxSize) {
    return wrap(delegate.limit(maxSize));
  }

  @Override
  public MutableRecordStream<K> skip(long n) {
    return wrap(delegate.skip(n));
  }

  @Override
  public MutableRecordStream<K> sequential() {
    return wrap(delegate.sequential());
  }

  @Override
  public MutableRecordStream<K> parallel() {
    return wrap(delegate.parallel());
  }

  @Override
  public MutableRecordStream<K> unordered() {
    return wrap(delegate.unordered());
  }

  @Override
  public MutableRecordStream<K> onClose(Runnable closeHandler) {
    return wrap(delegate.onClose(closeHandler));
  }

  @Override
  public void mutate(UpdateOperation<? super K> transform) {
    throw new UnsupportedOperationException("Mutative Operations are not supported on transactional record streams");
  }

  @Override
  public Stream<Tuple<Record<K>, Record<K>>> mutateThen(UpdateOperation<? super K> transform) {
    throw new UnsupportedOperationException("Mutative Operations are not supported on transactional record streams");
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException("Mutative Operations are not supported on transactional record streams");
  }

  @Override
  public Stream<Record<K>> deleteThen() {
    throw new UnsupportedOperationException("Mutative Operations are not supported on transactional record streams");
  }

  @Override
  protected MutableRecordStream<K> wrap(Stream<Record<K>> stream) {
    return new TransactionalMutableRecordStream<>(stream);
  }
}
