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

import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 *
 * @author cdennis
 */
class ExecutorDrivenAsyncReadRecordAccessor<K extends Comparable<K>> implements AsyncReadRecordAccessor<K> {

  private final ReadRecordAccessor<K> delegate;
  private final Executor executor;

  ExecutorDrivenAsyncReadRecordAccessor(ReadRecordAccessor<K> delegate, Executor executor) {
    this.delegate = delegate;
    this.executor = executor;
  }

  @Override
  public AsyncConditionalReadRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
    return new ExecutorDrivenAsyncConditionalReadRecordAccessor<>(delegate.iff(predicate), executor);
  }

  @Override
  public <T> Operation<Optional<T>> read(Function<? super Record<K>, T> mapper) {
    return operation(supplyAsync(() -> delegate.read(mapper), executor));
  }
}
