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
package com.terracottatech.store.client;

import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.ExceptionFreeAutoCloseable;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;

public interface DatasetEntity<K extends Comparable<K>> extends ExceptionFreeAutoCloseable, Entity {
  Type<K> getKeyType();

  void registerChangeListener(ChangeListener<K> listener);

  void deregisterChangeListener(ChangeListener<K> listener);

  boolean add(K key, Iterable<Cell<?>> cells);

  Record<K> addReturnRecord(K key, Iterable<Cell<?>> cells);

  RecordImpl<K> get(K key);

  RecordImpl<K> get(K key, IntrinsicPredicate<? super Record<K>> predicate);

  boolean update(K key, IntrinsicPredicate<? super Record<K>> predicate, IntrinsicUpdateOperation<? super K> transform);

  Tuple<Record<K>, Record<K>> updateReturnTuple(K key, IntrinsicPredicate<? super Record<K>> predicate, IntrinsicUpdateOperation<? super K> transform);

  boolean delete(K key, IntrinsicPredicate<? super Record<K>> predicate);

  Record<K> deleteReturnRecord(K key, IntrinsicPredicate<? super Record<K>> predicate);

  Indexing getIndexing();

  RecordStream<K> nonMutableStream();

  MutableRecordStream<K> mutableStream();

  /**
   * A marker interface classifying the {@code userData} argument of the
   * {@link EntityClientService#create(EntityClientEndpoint, java.lang.Object) EntityClientService.create}
   * method used to create a {@link DatasetEntity}.
   */
  interface Parameters {
  }
}
