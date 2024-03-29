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

import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import java.util.Optional;
import java.util.concurrent.Executor;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.supplyAsync;

class ExecutorDrivenAsyncConditionalReadWriteRecordAccessor<K extends Comparable<K>> extends ExecutorDrivenAsyncConditionalReadRecordAccessor<K> implements AsyncConditionalReadWriteRecordAccessor<K> {

  ExecutorDrivenAsyncConditionalReadWriteRecordAccessor(ConditionalReadWriteRecordAccessor<K> delegate, Executor executor) {
    super(delegate, executor);
  }

  @Override
  protected ConditionalReadWriteRecordAccessor<K> getDelegate() {
    return (ConditionalReadWriteRecordAccessor<K>) super.getDelegate();
  }

  @Override
  public Operation<Optional<Tuple<Record<K>, Record<K>>>> update(UpdateOperation<? super K> transform) {
    return operation(supplyAsync(() -> getDelegate().update(transform), getExecutor()));
  }

  @Override
  public Operation<Optional<Record<K>>> delete() {
    return operation(supplyAsync(() -> getDelegate().delete(), getExecutor()));
  }
}
