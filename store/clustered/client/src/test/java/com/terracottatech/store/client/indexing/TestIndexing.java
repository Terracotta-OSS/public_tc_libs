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

import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.terracottatech.store.indexing.Index.Status.LIVE;
import static java.util.stream.Collectors.toList;

/**
 * A test implementation of an {@link Index} that can be primed with index instances and failures.
 */
final class TestIndexing implements Indexing {

  private final Map<IndexKey<?>, Index<?>> indexMap = new LinkedHashMap<>();
  private final Map<IndexKey<?>, RuntimeException> faultMap = new LinkedHashMap<>();

  /**
   * Adds or replaces an index to this {@code Indexing} instance inventory.
   * @param index the index to add/replace.
   * @param <T> the index key type
   * @return this {@code TestIndexing}
   */
  <T extends Comparable<T>> TestIndexing put(Index<T> index) {
    IndexKey<T> key = new IndexKey<>(index.on(), index.definition());
    faultMap.remove(key);
    indexMap.put(key, index);
    return this;
  }

  /**
   * Adds or replaces a <i>failure</i> to this {@code Indexing} instance inventory.  When observed
   * via a {@link #createIndex(CellDefinition, IndexSettings)} or {@link #destroyIndex(Index)} operation,
   * the specified exception is thrown to complete the operation.
   * @param on the {@code CellDefinition} on which the index would be defined
   * @param settings the {@code IndexSettings} specifying the index type
   * @param fault the exception to throw
   * @param <T> the index key type
   * @return this {@code TestIndexing}
   */
  <T extends Comparable<T>> TestIndexing put(CellDefinition<T> on, IndexSettings settings, RuntimeException fault) {
    IndexKey<T> key = new IndexKey<>(on, settings);
    indexMap.remove(key);
    faultMap.put(key, fault);
    return this;
  }

  @Override
  public <T extends Comparable<T>> Operation<Index<T>> createIndex(CellDefinition<T> cellDefinition, IndexSettings settings) {
    IndexKey<T> key = new IndexKey<>(cellDefinition, settings);
    CompletableFuture<Index<T>> future = new CompletableFuture<>();
    RuntimeException fault = faultMap.get(key);
    if (fault != null) {
      if (fault instanceof ImmediateFailure) {
        throw fault;
      }
      future.completeExceptionally(fault);
    } else {
      @SuppressWarnings("unchecked") Index<T> index = (Index<T>)indexMap.get(key);
      if (index != null) {
        future.completeExceptionally(new IllegalArgumentException(settings + ":" + cellDefinition + " Index already exists"));
      } else {
        index = new ImmutableIndex<>(cellDefinition, settings, LIVE);
        indexMap.put(key, index);
        future.complete(index);
      }
    }

    return Operation.operation(future);
  }

  @Override
  public void destroyIndex(Index<?> index) throws StoreIndexNotFoundException {
    IndexKey<?> key = new IndexKey<>(index.on(), index.definition());
    RuntimeException fault = faultMap.get(key);
    if (fault != null) {
      throw fault;
    } else {
      Index<?> target = indexMap.get(key);
      if (target == null) {
        throw new StoreIndexNotFoundException(key.settings + ":" + key.on + ": Index not defined");
      } else {
        indexMap.remove(key);
      }
    }
  }

  @Override
  public Collection<Index<?>> getLiveIndexes() {
    return Collections.unmodifiableCollection(
        getAllIndexes().stream().filter(i -> i.status().equals(LIVE)).collect(toList()));
  }

  @Override
  public Collection<Index<?>> getAllIndexes() {
    return Collections.unmodifiableCollection(indexMap.values());
  }

  private static final class IndexKey<T extends Comparable<T>> {
    private final CellDefinition<T> on;
    private final IndexSettings settings;

    IndexKey(CellDefinition<T> on, IndexSettings settings) {
      this.on = on;
      this.settings = settings;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IndexKey<?> indexKey = (IndexKey<?>)o;
      return Objects.equals(on, indexKey.on) &&
          Objects.equals(settings, indexKey.settings);
    }

    @Override
    public int hashCode() {
      return Objects.hash(on, settings);
    }
  }

  /**
   * When provided to {@link TestIndexing#put(CellDefinition, IndexSettings, RuntimeException)}, causes
   * {@link TestIndexing#createIndex(CellDefinition, IndexSettings)} to fail immediately instead of failing
   * at {@link Operation#get()}.
   */
  static final class ImmediateFailure extends RuntimeException {
    private static final long serialVersionUID = 1844610447529614501L;
  }
}
