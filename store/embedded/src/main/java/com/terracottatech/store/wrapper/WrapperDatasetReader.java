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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ConditionalReadRecordAccessor;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.async.AsyncDatasetReader;
import com.terracottatech.store.async.ExecutorDrivenAsyncDatasetReader;
import com.terracottatech.store.stream.RecordStream;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Optional.ofNullable;

/**
 *
 * @author cdennis
 */
class WrapperDatasetReader<K extends Comparable<K>> implements DatasetReader<K> {

  private final SovereignDataset<K> backing;
  private final Executor asyncExecutor;

  WrapperDatasetReader(SovereignDataset<K> backing, Executor asyncExecutor) {
    this.backing = backing;
    this.asyncExecutor = asyncExecutor;
  }

  protected SovereignDataset<K> getSovereignBacking() {
    return backing;
  }

  @Override
  public Type<K> getKeyType() {
    return backing.getType();
  }

  @Override
  public Optional<Record<K>> get(K key) {
    return ofNullable(getSovereignBacking().get(key));
  }

  @Override
  public ReadRecordAccessor<K> on(K key) {
    return new ReadRecordAccessorImpl(key);
  }

  @Override
  public RecordStream<K> records() {
    return new WrapperRecordStream<>(getSovereignBacking().records());
  }

  @Override
  public AsyncDatasetReader<K> async() {
    return new ExecutorDrivenAsyncDatasetReader<>(this, asyncExecutor);
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    throw new UnsupportedOperationException("Events not yet supported for embedded");
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    throw new UnsupportedOperationException("Events not yet supported for embedded");
  }

  class ReadRecordAccessorImpl implements ReadRecordAccessor<K> {

    private final K key;

    ReadRecordAccessorImpl(K key) {
      this.key = key;
    }

    @Override
    public ConditionalReadRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
      return () -> get(key).filter(predicate);
    }

    @Override
    public <T> Optional<T> read(Function<? super Record<K>, T> mapper) {
      return get(key).map(mapper);
    }
  }
}
