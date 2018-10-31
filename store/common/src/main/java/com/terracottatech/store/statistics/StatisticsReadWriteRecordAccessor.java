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
import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * ReadWriteRecordAccessor wrapping interesting calls with an observer calculating statistics.
 */
public class StatisticsReadWriteRecordAccessor<K extends Comparable<K>> extends StatisticsReadRecordAccessor<K> implements ReadWriteRecordAccessor<K> {

  private final OperationObserver<DatasetOutcomes.AddOutcome> addObserver;
  private final OperationObserver<DatasetOutcomes.DeleteOutcome> deleteObserver;
  private final OperationObserver<DatasetOutcomes.UpdateOutcome> updateObserver;

  public StatisticsReadWriteRecordAccessor(DatasetStatistics statistics, ReadWriteRecordAccessor<K> accessor) {
    super(statistics, accessor);
    this.addObserver = statistics.getOperationStatistic(DatasetOutcomes.AddOutcome.class);
    this.deleteObserver = statistics.getOperationStatistic(DatasetOutcomes.DeleteOutcome.class);
    this.updateObserver = statistics.getOperationStatistic(DatasetOutcomes.UpdateOutcome.class);
  }

  @Override
  protected ReadWriteRecordAccessor<K> getUnderlying() {
    return (ReadWriteRecordAccessor<K>) super.getUnderlying();
  }

  @Override
  public ConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
    return new StatisticsConditionalReadWriteRecordAccessor<>(statistics, getUnderlying().iff(predicate));
  }

  @Override
  public Optional<Record<K>> add(Iterable<Cell<?>> cells) {
    return DatasetStatistics.observeCreate(addObserver, () -> getUnderlying().add(cells));
  }

  @Override
  public Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform) {
    return DatasetStatistics.observeUpdate(updateObserver, () -> getUnderlying().update(transform));
  }

  @Override
  public Optional<Record<K>> delete() {
    return DatasetStatistics.observeDelete(deleteObserver, () -> getUnderlying().delete());
  }
}
