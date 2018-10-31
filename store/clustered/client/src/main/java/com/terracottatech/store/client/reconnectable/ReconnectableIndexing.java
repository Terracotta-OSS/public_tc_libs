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

import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.client.indexing.AggregatedIndex;
import com.terracottatech.store.client.indexing.LiveIndex;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * An {@link Indexing} implementation delegating operations to a wrapped {@code Indexing} instance.
 */
final class ReconnectableIndexing implements Indexing {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectableIndexing.class);

  private final Set<ReconnectableIndex<?>> fetchedIndexes = synchronizedSet(newSetFromMap(new WeakHashMap<>()));
  private final ReconnectController reconnectController;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  @GuardedBy("lock")
  private volatile Indexing delegate;

  ReconnectableIndexing(Indexing delegate, ReconnectController reconnectController) {
    this.delegate = delegate;
    this.reconnectController = reconnectController;
  }

  private Indexing getDelegate() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      return delegate;
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  void swap(Indexing newDelegate) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      // Update the tracked Index instances
      Collection<Index<?>> allIndexes = newDelegate.getAllIndexes();
      synchronized (fetchedIndexes) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Fetched Indexes [{}]: {}",
              fetchedIndexes.size(),
              fetchedIndexes.stream()
                  .filter(Objects::nonNull)
                  .map(i -> String.format("Index[%s,%s]", i.on(), i.definition()))
                  .collect(joining(",")));
        }
        Iterator<ReconnectableIndex<?>> iterator = fetchedIndexes.iterator();
        while (iterator.hasNext()) {
          ReconnectableIndex<?> oldIndex = iterator.next();
          if (oldIndex != null) {
            allIndexes.stream()
                .filter(i -> Objects.equals(i.on(), oldIndex.on()) && Objects.equals(i.definition(), oldIndex.definition()))
                .forEach(i -> {
                  LOGGER.debug("Invoking {}.swap({})", oldIndex, i);
                  oldIndex.swap((Index)i);
                });   // Unchecked
          } else {
            iterator.remove();
          }
        }
      }

      delegate = newDelegate;

    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T extends Comparable<T>> Operation<Index<T>> createIndex(CellDefinition<T> cellDefinition, IndexSettings settings) {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().createIndex(cellDefinition, settings))
        .thenApply(i -> {
          if (i instanceof LiveIndex || i instanceof AggregatedIndex) {
            ReconnectableIndex<T> reconnectableIndex = new ReconnectableIndex<>(i, reconnectController);
            fetchedIndexes.add(reconnectableIndex);
            return reconnectableIndex;
          }
          return i;
        });
  }

  @Override
  public void destroyIndex(Index<?> index) throws StoreIndexNotFoundException {
    reconnectController.withConnectionClosedHandling(() -> {
      getDelegate().destroyIndex(index);
      return null;
    });
  }

  @Override
  public Collection<Index<?>> getLiveIndexes() {
    return wrapIndexes(Indexing::getLiveIndexes);
  }

  @Override
  public Collection<Index<?>> getAllIndexes() {
    return wrapIndexes(Indexing::getAllIndexes);
  }

  /**
   * Query the {@link Index} instances wrapping the "live" index instances in a {@link ReconnectableIndex}.
   * @param method the query method to use
   * @return the collection of indexes
   */
  private Collection<Index<?>> wrapIndexes(Function<Indexing, Collection<Index<?>>> method) {
    Collection<Index<?>> indexes = reconnectController.withConnectionClosedHandling(() -> method.apply(getDelegate()));
    Collection<Index<?>> wrappedIndexes = indexes.stream()
            .map(i -> {
              if (i instanceof LiveIndex || i instanceof AggregatedIndex) {
                return new ReconnectableIndex<>(i, reconnectController);
              } else {
                return i;
              }
            })
            .collect(toList());
    fetchedIndexes.addAll(wrappedIndexes.stream()
        .filter(ReconnectableIndex.class::isInstance)
        .map(i -> (ReconnectableIndex<?>)i).collect(toList()));
    return wrappedIndexes;
  }
}
