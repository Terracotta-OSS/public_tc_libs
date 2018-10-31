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
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

class ExecutorDrivenAsyncReadWriteRecordAccessor<K extends Comparable<K>> extends ExecutorDrivenAsyncReadRecordAccessor<K> implements AsyncReadWriteRecordAccessor<K> {

  private final ReadWriteRecordAccessor<K> delegate;
  private final Executor executor;

  ExecutorDrivenAsyncReadWriteRecordAccessor(ReadWriteRecordAccessor<K> delegate, Executor executor) {
    super(delegate, executor);
    this.delegate = delegate;
    this.executor = executor;
  }

  @Override
  public AsyncConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
    return new ExecutorDrivenAsyncConditionalReadWriteRecordAccessor<>(delegate.iff(predicate), executor);
  }

  @Override
  public Operation<Void> upsert(Iterable<Cell<?>> cells) {
    return operation(runAsync(() -> delegate.upsert(cells), executor));
  }

  @Override
  public Operation<Optional<Record<K>>> add(Iterable<Cell<?>> cells) {
    return operation(supplyAsync(() -> delegate.add(cells), executor));
  }

  @Override
  public Operation<Optional<Tuple<Record<K>, Record<K>>>> update(UpdateOperation<? super K> transform) {
    return operation(supplyAsync(() -> delegate.update(transform), executor));
  }

  @Override
  public Operation<Optional<Record<K>>> delete() {
    return operation(supplyAsync(() -> delegate.delete(), executor));
  }
}
