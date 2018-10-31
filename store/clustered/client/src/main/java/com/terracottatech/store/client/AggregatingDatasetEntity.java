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

import com.terracottatech.entity.AggregateEndpoint;
import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.indexing.AggregatedIndexing;
import com.terracottatech.store.client.stream.sharded.ShardedMutableRecordStream;
import com.terracottatech.store.client.stream.sharded.ShardedRecordStream;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class AggregatingDatasetEntity<K extends Comparable<K>> implements DatasetEntity<K>  {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregatingDatasetEntity.class);

  private final int stripeCount;
  private final List<DatasetEntity<K>> entityList;
  private final Set<RecordStream<K>> openStreams;
  private final AtomicBoolean closed = new AtomicBoolean();

  public AggregatingDatasetEntity(AggregateEndpoint<DatasetEntity<K>> endpoint) {
    this.entityList = endpoint.getEntities();
    this.openStreams = ConcurrentHashMap.newKeySet();
    this.stripeCount = entityList.size();
  }

  protected AggregatingDatasetEntity(AggregatingDatasetEntity<K> entity, UnaryOperator<DatasetEntity<K>> transform) {
    this.entityList = entity.entityList.stream().map(transform).collect(Collectors.toList());
    this.openStreams = entity.openStreams;
    this.stripeCount = entityList.size();
  }

  @Override
  public Type<K> getKeyType() {
    Set<Type<K>> types = entityList.stream().map(DatasetEntity::getKeyType).collect(toSet());

    if (types.size() == 1) {
      return types.iterator().next();
    } else {
      throw new IllegalStateException("Stripe dataset key types are not consistent: " + types);
    }
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    checkIfClosed();
    entityList.forEach(entity -> entity.registerChangeListener(listener));
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    checkIfClosed();
    entityList.forEach(entity -> entity.deregisterChangeListener(listener));
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    checkIfClosed();
    return getDatasetEntity(key).add(key, cells);
  }

  @Override
  public Record<K> addReturnRecord(K key, Iterable<Cell<?>> cells) {
    checkIfClosed();
    return getDatasetEntity(key).addReturnRecord(key, cells);
  }

  @Override
  public RecordImpl<K> get(K key) {
    checkIfClosed();
    return getDatasetEntity(key).get(key);
  }

  @Override
  public RecordImpl<K> get(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    checkIfClosed();
    return getDatasetEntity(key).get(key, predicate);
  }

  @Override
  public boolean update(K key, IntrinsicPredicate<? super Record<K>> predicate,
                        IntrinsicUpdateOperation<? super K> operation) {
    checkIfClosed();
    return getDatasetEntity(key).update(key, predicate, operation);
  }

  @Override
  public Tuple<Record<K>, Record<K>> updateReturnTuple(K key, IntrinsicPredicate<? super Record<K>> predicate,
                                                       IntrinsicUpdateOperation<? super K> operation) {
    checkIfClosed();
    return getDatasetEntity(key).updateReturnTuple(key, predicate, operation);
  }

  @Override
  public boolean delete(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    checkIfClosed();
    return getDatasetEntity(key).delete(key, predicate);
  }

  @Override
  public Record<K> deleteReturnRecord(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    checkIfClosed();
    return getDatasetEntity(key).deleteReturnRecord(key, predicate);
  }

  @Override
  public Indexing getIndexing() {
    return new AggregatedIndexing<>(entityList, closed::get);
  }

  @Override
  public RecordStream<K> nonMutableStream() {
    //Shuffle the shards to try and even out the load of order agnostic partial stream consuming operations (findAny, *Match, etc.)
    return registerStream(new ShardedRecordStream<>(shuffled(DatasetEntity::nonMutableStream)));
  }

  @Override
  public MutableRecordStream<K> mutableStream() {
    //Shuffle the shards to try and even out the load of order agnostic partial stream consuming operations (findAny, *Match, etc.)
    return registerStream(new ShardedMutableRecordStream<>(shuffled(DatasetEntity::mutableStream)));
  }

  private <T> Collection<T> shuffled(Function<DatasetEntity<K>, T> mapper) {
    List<T> list = entityList.stream().map(mapper).collect(Collectors.toList());
    Collections.shuffle(list);
    return list;
  }

  private <S extends RecordStream<K>> S registerStream(S stream) {
    @SuppressWarnings("unchecked")
    S newStream = (S) stream.onClose(() -> openStreams.remove(stream));
    try {
      checkIfClosed();
      openStreams.add(stream);
      checkIfClosed();
    } catch (StoreRuntimeException e) {
      stream.close();
      throw e;
    }
    return newStream;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      final AtomicReference<RuntimeException> exRef = new AtomicReference<>();
      int streamCount = 0;
      for (RecordStream<K> openStream : openStreams) {
        streamCount++;
        try {
          openStream.close();
        } catch (Exception e) {
          // ignored
        }
      }
      if (streamCount > 0) {
        LOGGER.warn("Closed {} streams at Dataset close; for best resource management, each stream should be explicitly closed", streamCount);
      }
      for (DatasetEntity<K> datasetEntity : entityList) {
        try {
         datasetEntity.close();
        } catch (RuntimeException e) {
          Exception existing = exRef.get();
          if (existing == null) {
            exRef.set(e);
          } else {
            existing.addSuppressed(e);
          }
        }
      }
      if (exRef.get() != null) {
        throw exRef.get();
      }
    }
  }

  private int getStripeID(K key) {
    return StripeChooser.getStripeForKey(key, stripeCount);
  }

  private DatasetEntity<K> getDatasetEntity(K key) {
    return entityList.get(getStripeID(key));
  }

  private void checkIfClosed() throws StoreRuntimeException {
    if (closed.get()) {
      throw new StoreRuntimeException("Attempt to use Dataset after close()");
    }
  }
}
