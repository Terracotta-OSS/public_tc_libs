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
package com.terracottatech.store.async;

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.Record;

import java.util.Optional;
import java.util.concurrent.Executor;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * An {@link AsyncDatasetReader} implementation that uses an {@link Executor} to run
 * operations asynchronously.
 *
 * @author Chris Dennis
 */
public class ExecutorDrivenAsyncDatasetReader<K extends Comparable<K>> implements AsyncDatasetReader<K> {

  private final DatasetReader<K> delegate;
  private final Executor executor;

  /**
   * Create a new async dataset reader.  The new async reader uses the given executor
   * to execute operations against the supplied synchronous reader.
   *
   * @param reader underlying synchronous reader
   * @param executor executor that runs the operations
   */
  public ExecutorDrivenAsyncDatasetReader(DatasetReader<K> reader, Executor executor) {
    this.delegate = reader;
    this.executor = executor;
  }

  @Override
  public Operation<Optional<Record<K>>> get(K key) {
    return operation(supplyAsync(() -> delegate.get(key), executor));
  }

  @Override
  public AsyncReadRecordAccessor<K> on(K key) {
    return new ExecutorDrivenAsyncReadRecordAccessor<>(delegate.on(key), executor);
  }

  @Override
  public AsyncRecordStream<K> records() {
    return new ExecutorDrivenAsyncRecordStream<>(delegate.records(), executor);
  }

}
