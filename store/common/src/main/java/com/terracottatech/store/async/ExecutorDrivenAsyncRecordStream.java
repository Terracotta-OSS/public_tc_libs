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
import com.terracottatech.store.stream.RecordStream;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Asynchronous version of {@code com.terracottatech.store.client.stream.RecordStream}.
 */
class ExecutorDrivenAsyncRecordStream<K extends Comparable<K>>
    extends ExecutorDrivenAsyncStream<Record<K>>
    implements AsyncRecordStream<K> {

  ExecutorDrivenAsyncRecordStream(RecordStream<K> stream, Executor executor) {
    super(stream, executor);
  }

  @Override
  protected RecordStream<K> getStream() {
    return (RecordStream<K>)super.getStream();
  }

  @Override
  public AsyncRecordStream<K> explain(Consumer<Object> consumer) {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().explain(consumer), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> filter(Predicate<? super Record<K>> predicate) {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().filter(predicate), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> distinct() {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().distinct(), getExecutor());
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
  public AsyncRecordStream<K> peek(Consumer<? super Record<K>> action) {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().peek(action), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> limit(long maxSize) {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().limit(maxSize), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> skip(long n) {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().skip(n), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> sequential() {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().sequential(), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> parallel() {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().parallel(), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> unordered() {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().unordered(), getExecutor());
  }

  @Override
  public AsyncRecordStream<K> onClose(Runnable closeHandler) {
    return new ExecutorDrivenAsyncRecordStream<>(getStream().onClose(closeHandler), getExecutor());
  }
}
