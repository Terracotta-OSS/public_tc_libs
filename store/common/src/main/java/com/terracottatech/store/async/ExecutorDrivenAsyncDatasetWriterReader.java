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

import com.terracottatech.store.Cell;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.UpdateOperation;

import java.util.concurrent.Executor;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * An {@link AsyncDatasetWriterReader} implementation that uses an {@link Executor} to run
 * operations asynchronously.
 *
 * @author Chris Dennis
 */
public class ExecutorDrivenAsyncDatasetWriterReader<K extends Comparable<K>> extends ExecutorDrivenAsyncDatasetReader<K> implements AsyncDatasetWriterReader<K> {

  private final DatasetWriterReader<K> delegate;
  private final Executor executor;

  /**
   * Create a new async dataset writer-reader.  The new async writer-reader uses the given executor
   * to execute operations against the supplied synchronous writer-reader.
   *
   * @param writerReader underlying synchronous writer-reader
   * @param executor executor that runs the operations
   */
  public ExecutorDrivenAsyncDatasetWriterReader(DatasetWriterReader<K> writerReader, Executor executor) {
    super(writerReader, executor);
    this.delegate = writerReader;
    this.executor = executor;
  }

  @Override
  public Operation<Boolean> add(K key, Iterable<Cell<?>> cells) {
    return operation(supplyAsync(() -> delegate.add(key, cells), executor));
  }

  @Override
  public Operation<Boolean> update(K key, UpdateOperation<? super K> transform) {
    return operation(supplyAsync(() -> delegate.update(key, transform), executor));
  }

  @Override
  public Operation<Boolean> delete(K key) {
    return operation(supplyAsync(() -> delegate.delete(key), executor));
  }

  @Override
  public AsyncReadWriteRecordAccessor<K> on(K key) {
    return new ExecutorDrivenAsyncReadWriteRecordAccessor<>(delegate.on(key), executor);
  }

  @Override
  public AsyncMutableRecordStream<K> records() {
    return new ExecutorDrivenAsyncMutableRecordStream<>(delegate.records(), executor);
  }
}
