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
package com.terracottatech.store.client.reconnectable;

import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.client.RecordImpl;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link DatasetEntity} supporting reconnect testing.  This class is
 * used due to a conflict between Mockito mocks and a requirement that
 */
class TestDatasetEntity<K extends Comparable<K>> implements DatasetEntity<K> {
  final AtomicBoolean closed = new AtomicBoolean(false);
  private final Type<K> keyType;

  TestDatasetEntity(Type<K> keyType) {
    this.keyType = keyType;
  }

  @Override
  public void close() {
    closed.set(true);
  }

  @Override
  public Type<K> getKeyType() {
    return keyType;
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    throw new UnsupportedOperationException("registerChangeListener");
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    throw new UnsupportedOperationException("deregisterChangeListener");
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    throw new UnsupportedOperationException("add(K, Iterable)");
  }

  @Override
  public Record<K> addReturnRecord(K key, Iterable<Cell<?>> cells) {
    throw new UnsupportedOperationException("addReturnRecord(K, Iterable)");
  }

  @Override
  public RecordImpl<K> get(K key) {
    throw new UnsupportedOperationException("get(K)");
  }

  @Override
  public RecordImpl<K> get(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    throw new UnsupportedOperationException("get(K, IntrinsicPredicate)");
  }

  @Override
  public boolean update(K key, IntrinsicPredicate<? super Record<K>> predicate, IntrinsicUpdateOperation<? super K> transform) {
    throw new UnsupportedOperationException("update(K, IntrinsicPredicate, IntrinsicUpdateOperation)");
  }

  @Override
  public Tuple<Record<K>, Record<K>> updateReturnTuple(K key, IntrinsicPredicate<? super Record<K>> predicate, IntrinsicUpdateOperation<? super K> transform) {
    throw new UnsupportedOperationException("updateReturnTuple(K, IntrinsicPredicate, IntrinsicUpdateOperation)");
  }

  @Override
  public boolean delete(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    throw new UnsupportedOperationException("delete(K, IntrinsicPredicate)");
  }

  @Override
  public Record<K> deleteReturnRecord(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    throw new UnsupportedOperationException("deleteReturnRecord(K, IntrinsicPredicate)");
  }

  @Override
  public Indexing getIndexing() {
    throw new UnsupportedOperationException("getIndexing()");
  }

  @Override
  public RecordStream<K> nonMutableStream() {
    throw new UnsupportedOperationException("nonMutableStream()");
  }

  @Override
  public MutableRecordStream<K> mutableStream() {
    throw new UnsupportedOperationException("mutableStream()");
  }
}
