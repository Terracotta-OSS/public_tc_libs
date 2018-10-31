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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EntityClientEndpoint;

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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;

/**
 * A {@link DatasetEntity} implementation delegating operations to a <i>replaceable</i>
 * instance.  This instance is passed to consumers of {@code DatasetEntity} as a level
 * of indirection that permits "live" replacement of the entity during a reconnection.
 */
public class ReconnectableDatasetEntity<K extends Comparable<K>>
    extends AbstractReconnectableEntity<DatasetEntity<K>> implements DatasetEntity<K> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectableDatasetEntity.class);

  private final Set<ChangeListener<K>> registeredChangeListeners = new LinkedHashSet<>();

  private final Set<ReconnectableIndexing> fetchedIndexings = synchronizedSet(newSetFromMap(new WeakHashMap<>()));

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  @GuardedBy("lock")
  private volatile DatasetEntity<K> delegate;

  public ReconnectableDatasetEntity(DatasetEntity<K> datasetEntity, ReconnectController reconnectController) {
    super(reconnectController);
    delegate = Objects.requireNonNull(datasetEntity, "datasetEntity");
  }

  @Override
  protected DatasetEntity<K> getDelegate() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      return delegate;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Atomically replaces the current {@link DatasetEntity} with a new {@code DatasetEntity}, closes all open
   * streams, and swaps {@link ChangeListener} instances from the current to the new entity.
   * @param newDatasetEntity the replacement {@code DatasetEntity}
   * @throws IllegalArgumentException if {@code newDatasetEntity} has a different keyType than the current datasetEntity
   */
  @Override
  public void swap(DatasetEntity<K> newDatasetEntity) {
    Objects.requireNonNull(newDatasetEntity, "newDatasetEntity");

    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      DatasetEntity<K> oldDatasetEntity = delegate;

      if (!oldDatasetEntity.getKeyType().equals(newDatasetEntity.getKeyType())) {
        throw new IllegalArgumentException(String.format("New DatasetEntity has keyType %s, expecting %s",
            newDatasetEntity.getKeyType(), oldDatasetEntity.getKeyType()));
      }

      for (ChangeListener<K> listener : registeredChangeListeners) {
        // Events arriving during this transition may wind up notifying the _old_ entity
        oldDatasetEntity.deregisterChangeListener(listener);
        newDatasetEntity.registerChangeListener(listener);
        listener.missedEvents();
      }

      synchronized (fetchedIndexings) {
        Iterator<ReconnectableIndexing> iterator = fetchedIndexings.iterator();
        while (iterator.hasNext()) {
          ReconnectableIndexing indexing = iterator.next();
          if (indexing != null) {
            Indexing newIndexing = newDatasetEntity.getIndexing();
            LOGGER.debug("Invoking {}.swap({})", indexing, newIndexing);
            indexing.swap(newIndexing);
          } else {
            iterator.remove();
          }
        }
      }

      delegate = newDatasetEntity;

    } finally {
      lock.unlock();
    }
  }

  @Override
  public Type<K> getKeyType() {
    return getDelegate().getKeyType();
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    reconnectController.withConnectionClosedHandling(() -> {
      getDelegate().registerChangeListener(listener);
      return null;
    });
    registeredChangeListeners.add(listener);
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    try {
      reconnectController.withConnectionClosedHandling(() -> {
        getDelegate().deregisterChangeListener(listener);
        return null;
      });
    } finally {
      registeredChangeListeners.remove(listener);
    }
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().add(key, cells));
  }

  @Override
  public Record<K> addReturnRecord(K key, Iterable<Cell<?>> cells) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().addReturnRecord(key, cells));
  }

  @Override
  public RecordImpl<K> get(K key) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().get(key));
  }

  @Override
  public RecordImpl<K> get(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().get(key, predicate));
  }

  @Override
  public boolean update(K key, IntrinsicPredicate<? super Record<K>> predicate, IntrinsicUpdateOperation<? super K> transform) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().update(key, predicate, transform));
  }

  @Override
  public Tuple<Record<K>, Record<K>> updateReturnTuple(K key, IntrinsicPredicate<? super Record<K>> predicate, IntrinsicUpdateOperation<? super K> transform) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().updateReturnTuple(key, predicate, transform));
  }

  @Override
  public boolean delete(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().delete(key, predicate));
  }

  @Override
  public Record<K> deleteReturnRecord(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().deleteReturnRecord(key, predicate));
  }

  @Override
  public Indexing getIndexing() {
    ReconnectableIndexing indexing = new ReconnectableIndexing(
        reconnectController.withConnectionClosedHandling(() -> getDelegate().getIndexing()), reconnectController);
    fetchedIndexings.add(indexing);
    return indexing;
  }

  @Override
  public RecordStream<K> nonMutableStream() {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().nonMutableStream());
  }

  @Override
  public MutableRecordStream<K> mutableStream() {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().mutableStream());
  }

  /**
   * {@link org.terracotta.entity.EntityClientService#create(EntityClientEndpoint, Object) EntityClientService.create}
   * {@code userData} wrapper for {@link DatasetEntity} creation.
   */
  public static class Parameters implements DatasetEntity.Parameters {
    private final ReconnectController reconnectController;
    private final DatasetEntity.Parameters wrappedParameters;

    public Parameters(ReconnectController reconnectController, DatasetEntity.Parameters wrappedParameters) {
      this.reconnectController = reconnectController;
      this.wrappedParameters = wrappedParameters;
    }

    public ReconnectController reconnectController() {
      return reconnectController;
    }

    public DatasetEntity.Parameters unwrap() {
      return wrappedParameters;
    }
  }
}
