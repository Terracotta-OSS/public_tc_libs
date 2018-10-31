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

import com.terracottatech.store.ConditionalReadRecordAccessor;
import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * ReadRecordAccessor wrapping interesting calls with an observer calculating statistics.
 */
public class StatisticsReadRecordAccessor<K extends Comparable<K>> implements ReadRecordAccessor<K> {

  private final ReadRecordAccessor<K> underlying;
  protected final DatasetStatistics statistics;

  private final OperationObserver<DatasetOutcomes.GetOutcome> getObserver;

  public StatisticsReadRecordAccessor(DatasetStatistics statistics, ReadRecordAccessor<K> underlying) {
    this.underlying = underlying;
    this.statistics = statistics;
    this.getObserver = statistics.getOperationStatistic(DatasetOutcomes.GetOutcome.class);
  }

  protected ReadRecordAccessor<K> getUnderlying() {
    return underlying;
  }

  @Override
  public ConditionalReadRecordAccessor<K> iff(Predicate<? super Record<K>> predicate) {
    return new StatisticsConditionalReadRecordAccessor<>(statistics, underlying.iff(predicate));
  }

  @Override
  public <T> Optional<T> read(Function<? super Record<K>, T> mapper) {
    return DatasetStatistics.observeGet(getObserver, () -> underlying.read(mapper));
  }
}
