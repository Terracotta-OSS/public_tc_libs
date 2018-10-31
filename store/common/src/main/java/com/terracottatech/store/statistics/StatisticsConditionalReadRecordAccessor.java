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
import com.terracottatech.store.Record;

import java.util.Optional;

/**
 * ConditionalReadRecordAccessor wrapping interesting calls with an observer calculating statistics.
 */
public class StatisticsConditionalReadRecordAccessor<K extends Comparable<K>> implements ConditionalReadRecordAccessor<K> {

  private final ConditionalReadRecordAccessor<K> underlying;

  private final OperationObserver<DatasetOutcomes.GetOutcome> getObserver;

  public StatisticsConditionalReadRecordAccessor(DatasetStatistics statistics, ConditionalReadRecordAccessor<K> underlying) {
    this.underlying = underlying;
    this.getObserver = statistics.getOperationStatistic(DatasetOutcomes.GetOutcome.class);
  }

  protected ConditionalReadRecordAccessor<K> getUnderlying() {
    return underlying;
  }

  @Override
  public Optional<Record<K>> read() {
    return DatasetStatistics.observeGet(getObserver, () -> underlying.read());
  }
}
