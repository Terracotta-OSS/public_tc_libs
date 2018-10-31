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

import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * Asynchronous version of {@code com.terracottatech.store.client.stream.MutableRecordStream}.
 */
class ExecutorDrivenAsyncMutableRecordStream<K extends Comparable<K>>
    extends ExecutorDrivenAsyncRecordStream<K>
    implements AsyncMutableRecordStream<K> {

  ExecutorDrivenAsyncMutableRecordStream(MutableRecordStream<K> stream, Executor executor) {
    super(stream, executor);
  }

  @Override
  protected MutableRecordStream<K> getStream() {
    return (MutableRecordStream<K>)super.getStream();
  }

  @Override
  public Operation<Void> mutate(UpdateOperation<? super K> transform) {
    return operation(runAsync(() -> getStream().mutate(transform), getExecutor()));
  }

  @Override
  public AsyncStream<Tuple<Record<K>, Record<K>>> mutateThen(UpdateOperation<? super K> transform) {
    return new ExecutorDrivenAsyncStream<>(getStream().mutateThen(transform), getExecutor());
  }

  @Override
  public Operation<Void> delete() {
    return operation(runAsync(getStream()::delete, getExecutor()));
  }

  @Override
  public AsyncStream<Record<K>> deleteThen() {
    return new ExecutorDrivenAsyncStream<>(getStream().deleteThen(), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> explain(Consumer<Object> consumer) {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().explain(consumer), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> filter(Predicate<? super Record<K>> predicate) {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().filter(predicate), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> distinct() {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().distinct(), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> sorted() {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().sorted(), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> sorted(Comparator<? super Record<K>> comparator) {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().sorted(comparator), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> peek(Consumer<? super Record<K>> action) {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().peek(action), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> limit(long maxSize) {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().limit(maxSize), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> skip(long n) {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().skip(n), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> sequential() {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().sequential(), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> parallel() {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().parallel(), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> unordered() {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().unordered(), getExecutor());
  }

  @Override
  public AsyncMutableRecordStream<K> onClose(Runnable closeHandler) {
    return new ExecutorDrivenAsyncMutableRecordStream<>(getStream().onClose(closeHandler), getExecutor());
  }
}
