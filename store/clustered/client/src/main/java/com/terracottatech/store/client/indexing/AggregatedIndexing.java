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

package com.terracottatech.store.client.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.toList;

public class AggregatedIndexing<K extends Comparable<K>> implements Indexing {

  private static final Logger LOGGER = LoggerFactory.getLogger(AggregatedIndexing.class);
  private final List<? extends DatasetEntity<K>> entityList;
  private final BooleanSupplier closed;

  /**
   * The {@link Executor} to use for {@link #createIndex(CellDefinition, IndexSettings)} requests.
   * This field relies lazy initialization and must be accessed through {@link #getExecutor()}.
   */
  private volatile Executor executor;

  public AggregatedIndexing(List<? extends DatasetEntity<K>> entityList, BooleanSupplier parentClosed) {
    this.entityList = entityList;
    this.closed = parentClosed;
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation sends a {@code createIndex} request to each {@link DatasetEntity} defined for
   * the aggregation.  The operation reports a failure only when <i>all</i> entities report a failure.
   *
   * @param cellDefinition {@inheritDoc}
   * @param settings {@inheritDoc}
   * @param <T> {@inheritDoc}
   * @return {@inheritDoc}
   */
  @Override
  public <T extends Comparable<T>> Operation<Index<T>> createIndex(CellDefinition<T> cellDefinition, IndexSettings settings) {
    checkIfClosed();

    Lock resultLock = new ReentrantLock();
    Collection<Index<T>> successfulCreates = new ArrayList<>();
    Map<DatasetEntity<K>, Throwable> failedCreates = new IdentityHashMap<>();

    List<CompletableFuture<Index<T>>> createOperations = new ArrayList<>();
    for (DatasetEntity<K> entity : entityList) {
      try {
        CompletableFuture<Index<T>> createRequest = entity.getIndexing().createIndex(cellDefinition, settings)
            .whenComplete((index, fault) -> {
              resultLock.lock();
              try {
                if (index != null) {
                  successfulCreates.add(index);
                } else {
                  failedCreates.put(entity, fault);
                  LOGGER.info("Failure creating index on {}: {}", entity, fault, fault);
                }
              } finally {
                resultLock.unlock();
              }
            }).toCompletableFuture();
        createOperations.add(createRequest);

      } catch (Exception e) {
        resultLock.lock();
        try {
          failedCreates.put(entity, e);
          LOGGER.info("Failure creating index on {}: {}", entity, e, e);
        } finally {
          resultLock.unlock();
        }
      }
    }

    CompletableFuture<Index<T>> allFuture;
    if (!createOperations.isEmpty()) {
      allFuture = CompletableFuture.allOf(createOperations.toArray(new CompletableFuture<?>[createOperations.size()]))
          .handleAsync((v, t) -> {
                if (t != null && successfulCreates.isEmpty()) {
                  StoreRuntimeException fault =
                      new StoreRuntimeException("Unable to create index; all index requests failed", t);
                  failedCreates.values().forEach(t::addSuppressed);
                  throw fault;
                }

                return new AggregatedIndex<>(cellDefinition, settings, successfulCreates, failedCreates.keySet());
              }, getExecutor());

    } else {
      StoreRuntimeException fault = new StoreRuntimeException("Unable to create index; all index requests failed");
      failedCreates.values().forEach(fault::addSuppressed);
      throw fault;
    }

    return Operation.operation(allFuture);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation attempts to destroy the index in each {@link DatasetEntity} defined for this
   * aggregation.  This operation reports a failure if any entity reports a failure <i>other</i> than
   * a {@link StoreIndexNotFoundException}.  This operation reporta a {@code StoreIndexNotFoundException}
   * only if <i>all</i> entities report the failure.
   *
   * @param index {@inheritDoc}
   * @throws StoreIndexNotFoundException {@inheritDoc}
   */
  @Override
  public void destroyIndex(Index<?> index) throws StoreIndexNotFoundException {
    checkIfClosed();
    List<StoreIndexNotFoundException> notFounds = null;
    RuntimeException fault = null;
    for (DatasetEntity<K> entity : entityList) {
      try {
        // TODO: Convert this to background tasks to permit destroy operation overlap
        entity.getIndexing().destroyIndex(index);

      } catch (StoreIndexNotFoundException e) {
        /*
         * Hold a StoreIndexNotFoundException aside; we'll only use these
         * if all entities throw this or an entity throws something else.
         */
        if (fault == null) {
          if (notFounds == null) {
            notFounds = new ArrayList<>();
          }
          notFounds.add(e);
        } else {
          fault.addSuppressed(e);
        }

      } catch (RuntimeException e) {
        if (fault == null) {
          fault = e;
          if (notFounds != null) {
            notFounds.forEach(fault::addSuppressed);
            notFounds = null;
          }
        } else {
          fault.addSuppressed(e);
        }
      }
    }
    if (fault != null) {
      throw fault;
    } else if (notFounds != null && notFounds.size() == entityList.size()) {
      StoreIndexNotFoundException e = new StoreIndexNotFoundException("Cannot destroy "
          + index.definition() + ":" + index.on() + " index : does not exist");
      notFounds.forEach(e::addSuppressed);
      throw e;
    }
  }

  @Override
  public Collection<Index<?>> getLiveIndexes() {
    checkIfClosed();
    return getAllIndexes().stream().filter(index -> index.status().equals(Index.Status.LIVE)).collect(toList());
  }

  @Override
  public Collection<Index<?>> getAllIndexes() {
    checkIfClosed();

    /*
     * Create an instance of AggregateIndex for each index reported by any stripe server.  Each server
     * **not** reporting a given index gets added to that index's nonReportingEntities list so, in the
     * "live" AggregatedIndex can poll the non-reporting server again.
     */
    Map<Key<?>, List<Index<?>>> definedIndexes = new HashMap<>();
    Map<Key<?>, Set<DatasetEntity<K>>> entityMap = new HashMap<>();
    for (DatasetEntity<K> entity : entityList) {
      Collection<Index<?>> allIndexes = entity.getIndexing().getAllIndexes();
      for (Index<?> index : allIndexes) {
        Key<?> key = new Key<>(index);

        List<Index<?>> indexes = definedIndexes.computeIfAbsent(key, k -> new ArrayList<>());
        indexes.add(index);

        Set<DatasetEntity<K>> entities = entityMap.computeIfAbsent(key, k -> newSetFromMap(new IdentityHashMap<>()));
        entities.add(entity);
      }
    }

    Collection<Index<?>> indexes = new ArrayList<>();
    for (Map.Entry<Key<?>, List<Index<?>>> entry : definedIndexes.entrySet()) {
      Key<?> key = entry.getKey();
      Set<DatasetEntity<K>> withoutIndex = newSetFromMap(new IdentityHashMap<>());
      withoutIndex.addAll(entityList);
      withoutIndex.removeAll(entityMap.get(key));
      @SuppressWarnings({"unchecked", "rawtypes"})
      AggregatedIndex<?> index = new AggregatedIndex(key.on, key.definition, entry.getValue(), withoutIndex);
      indexes.add(index);
    }

    return Collections.unmodifiableCollection(indexes);
  }

  /**
   * Returns the {@link Executor} to use for {@link #createIndex(CellDefinition, IndexSettings)} requests.
   * @return the {@code Executor}
   */
  private Executor getExecutor() {
    if (executor == null) {
      synchronized (this) {
        if (executor == null) {
          executor = new CreateIndexExecutor();
        }
      }
    }
    return executor;
  }

  private void checkIfClosed() {
    if (closed.getAsBoolean()) {
      throw new StoreRuntimeException("Attempt to use Indexing after Dataset closed");
    }
  }

  private static final class Key<T extends Comparable<T>> {
    private final CellDefinition<T> on;
    private final IndexSettings definition;

    private Key(Index<T> index) {
      this.on = index.on();
      this.definition = index.definition();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Key<?> key = (Key<?>)o;
      return Objects.equals(on, key.on) &&
          Objects.equals(definition, key.definition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(on, definition);
    }
  }
}
