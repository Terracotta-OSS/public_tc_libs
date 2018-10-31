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
package com.terracottatech.store.statistics;

import com.terracottatech.store.Type;
import org.terracotta.statistics.observer.OperationObserver;

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.async.AsyncDatasetReader;
import com.terracottatech.store.async.ExecutorDrivenAsyncDatasetReader;
import com.terracottatech.store.stream.RecordStream;

import java.util.Optional;
import java.util.concurrent.ForkJoinPool;

/**
 * DatasetReader wrapping interesting calls with an observer calculating statistics.
 */
public class StatisticsDatasetReader<K extends Comparable<K>> implements DatasetReader<K> {

  private final DatasetReader<K> underlying;
  protected final DatasetStatistics statistics;

  private final OperationObserver<DatasetOutcomes.GetOutcome> getObserver;
  protected final OperationObserver<DatasetOutcomes.StreamOutcome> streamObserver;

  public StatisticsDatasetReader(DatasetStatistics statistics, DatasetReader<K> underlying) {
    this.underlying = underlying;
    this.statistics = statistics;
    this.getObserver = statistics.getOperationStatistic(DatasetOutcomes.GetOutcome.class);
    this.streamObserver = statistics.getOperationStatistic(DatasetOutcomes.StreamOutcome.class);
  }

  protected DatasetReader<K> getUnderlying() {
    return underlying;
  }

  @Override
  public Type<K> getKeyType() {
    return underlying.getKeyType();
  }

  @Override
  public Optional<Record<K>> get(K key) {
    return DatasetStatistics.observeGet(getObserver, () -> underlying.get(key));
  }

  @Override
  public ReadRecordAccessor<K> on(K key) {
    return new StatisticsReadRecordAccessor<>(statistics, underlying.on(key));
  }

  @Override
  public RecordStream<K> records() {
    return DatasetStatistics.observeStream(streamObserver, () -> underlying.records());
  }

  @Override
  public AsyncDatasetReader<K> async() {
    return new ExecutorDrivenAsyncDatasetReader<>(this, ForkJoinPool.commonPool());
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    underlying.registerChangeListener(listener);
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    underlying.deregisterChangeListener(listener);
  }
}
