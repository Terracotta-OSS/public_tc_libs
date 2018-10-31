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
package com.terracottatech.store.wrapper;

import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;

/**
 *
 * @author cdennis
 */
public class WrapperIndexing implements Indexing {

  private final SovereignIndexing indexing;
  private final ExecutorService executor;
  private final Supplier<Boolean> closed;

  WrapperIndexing(SovereignIndexing indexing, ExecutorService executor, Supplier<Boolean> parentClosed) {
    this.indexing = indexing;
    this.executor = executor;
    this.closed = parentClosed;
  }

  @Override
  public <T extends Comparable<T>> Operation<Index<T>> createIndex(CellDefinition<T> cellDefinition, IndexSettings settings) {
    checkIfClosed();
    return operation(supplyAsync(() -> {
            Callable<SovereignIndex<T>> indexCreator = indexing.createIndex(cellDefinition, convert(settings));
            try {
              return indexCreator.call();
            } catch (RuntimeException e) {
              throw e;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
    }, executor)).thenApply(i -> new EmbeddedIndex<>(i, WrapperIndexing::convert));
  }

  @Override
  public void destroyIndex(Index<?> index) throws StoreIndexNotFoundException {
    checkIfClosed();
    if (index instanceof EmbeddedIndex) {
      indexing.destroyIndex(((EmbeddedIndex<?>) index).getSovereignIndex());
    } else {
      throw new IllegalArgumentException("Index not associated with this dataset (wrong type)");
    }
  }

  @Override
  public Collection<Index<?>> getLiveIndexes() {
    checkIfClosed();
    return indexing.getIndexes().stream().map(i -> (SovereignIndex<? extends Comparable<?>>) i).filter(i -> i.isLive())
            .map(i -> new EmbeddedIndex<>(i, WrapperIndexing::convert)).collect(toList());
  }

  @Override
  public Collection<Index<?>> getAllIndexes() {
    checkIfClosed();
    return indexing.getIndexes().stream()
            .map(i -> new EmbeddedIndex<>((SovereignIndex<? extends Comparable<?>>) i, WrapperIndexing::convert))
            .collect(toList());
  }

  public static SovereignIndexSettings convert(IndexSettings settings) {
    if (IndexSettings.btree().equals(settings)) {
      return SovereignIndexSettings.btree();
    } else {
      throw new IllegalArgumentException("Unsupported Index Settings " + settings);
    }
  }

  public static IndexSettings convert(SovereignIndexSettings settings) {
    if (SovereignIndexSettings.btree().equals(settings)) {
      return IndexSettings.btree();
    } else {
      throw new IllegalArgumentException("Unconvertable Index Settings " + settings);
    }
  }

  private void checkIfClosed() {
    if (closed.get()) {
      throw new StoreRuntimeException("Attempt to use Indexing after Dataset closed");
    }
  }
}
