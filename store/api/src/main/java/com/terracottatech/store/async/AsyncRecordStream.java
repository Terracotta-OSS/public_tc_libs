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
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Asynchronous equivalent to {@link RecordStream}.
 */
public interface AsyncRecordStream<K extends Comparable<K>> extends AsyncStream<Record<K>> {

  /**
   * An asynchronous equivalent to {@link RecordStream#explain(Consumer)}.
   *
   * @param consumer the explain consumer
   * @return the new stream
   */
  AsyncRecordStream<K> explain(Consumer<Object> consumer);

  @Override
  AsyncRecordStream<K> filter(Predicate<? super Record<K>> predicate);

  @Override
  AsyncRecordStream<K> distinct();

  @Override
  AsyncRecordStream<K> sorted();

  @Override
  AsyncRecordStream<K> sorted(Comparator<? super Record<K>> comparator);

  @Override
  AsyncRecordStream<K> peek(Consumer<? super Record<K>> action);

  @Override
  AsyncRecordStream<K> limit(long maxSize);

  @Override
  AsyncRecordStream<K> skip(long n);

  @Override
  AsyncRecordStream<K> sequential();

  @Override
  AsyncRecordStream<K> parallel();

  @Override
  AsyncRecordStream<K> unordered();

  @Override
  AsyncRecordStream<K> onClose(Runnable closeHandler);
}
