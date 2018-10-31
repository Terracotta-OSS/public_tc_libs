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

import org.terracotta.statistics.observer.OperationObserver;

import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.async.ExecutorDrivenAsyncDatasetWriterReader;
import com.terracottatech.store.stream.MutableRecordStream;

import java.util.concurrent.ForkJoinPool;

/**
 * DatasetWriterReader wrapping interesting calls with an observer calculating statistics.
 */
public class StatisticsDatasetWriterReader<K extends Comparable<K>> extends StatisticsDatasetReader<K> implements DatasetWriterReader<K> {

  private final OperationObserver<DatasetOutcomes.AddOutcome> addObserver;
  private final OperationObserver<DatasetOutcomes.DeleteOutcome> deleteObserver;
  private final OperationObserver<DatasetOutcomes.UpdateOutcome> updateObserver;

  public StatisticsDatasetWriterReader(DatasetStatistics statistics, DatasetWriterReader<K> underlying) {
    super(statistics, underlying);
    this.addObserver = statistics.getOperationStatistic(DatasetOutcomes.AddOutcome.class);
    this.deleteObserver = statistics.getOperationStatistic(DatasetOutcomes.DeleteOutcome.class);
    this.updateObserver = statistics.getOperationStatistic(DatasetOutcomes.UpdateOutcome.class);
  }

  @Override
  protected DatasetWriterReader<K> getUnderlying() {
    return (DatasetWriterReader<K>) super.getUnderlying();
  }

  @Override
  public boolean add(K key, Iterable<Cell<?>> cells) {
    return DatasetStatistics.observeBooleanCreate(addObserver, () -> getUnderlying().add(key, cells));
  }

  @Override
  public boolean update(K key, UpdateOperation<? super K> transform) {
    return DatasetStatistics.observeBooleanUpdate(updateObserver, () -> getUnderlying().update(key, transform));
  }

  @Override
  public boolean delete(K key) {
    return DatasetStatistics.observeBooleanDelete(deleteObserver, () -> getUnderlying().delete(key));
  }

  @Override
  public ReadWriteRecordAccessor<K> on(K key) {
    return new StatisticsReadWriteRecordAccessor<>(statistics, getUnderlying().on(key));
  }

  @Override
  public MutableRecordStream<K> records() {
    return DatasetStatistics.observeStream(streamObserver, () -> getUnderlying().records());
  }

  @Override
  public AsyncDatasetWriterReader<K> async() {
    return new ExecutorDrivenAsyncDatasetWriterReader<>(this, ForkJoinPool.commonPool());
  }

  @Override
  public void registerChangeListener(ChangeListener<K> listener) {
    getUnderlying().registerChangeListener(listener);
  }

  @Override
  public void deregisterChangeListener(ChangeListener<K> listener) {
    getUnderlying().deregisterChangeListener(listener);
  }
}
