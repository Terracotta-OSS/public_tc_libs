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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.indexing.Indexing;

/**
 * Dataset wrapping another to provide statistics.
 */
public class StatisticsDataset<K extends Comparable<K>> implements Dataset<K> {

  private final StatisticsService statisticsService;
  private final DatasetStatistics statistics;
  private final Dataset<K> underlying;

  public StatisticsDataset(Dataset<K> underlying, String datasetName, StatisticsService statisticsService) {
    this.underlying = underlying;
    this.statisticsService = statisticsService;
    this.statistics = statisticsService.registerDataset(datasetName, this);
  }

  /**
   * Return the underlying dataset. It shouldn't be used directly if you want your statistics to stay accurate.
   *
   * @return the wrapped dataset
   */
  public Dataset<K> getUnderlying() {
    return underlying;
  }

  public DatasetStatistics getStatistics() {
    return statistics;
  }

  @Override
  public DatasetReader<K> reader() {
    return new StatisticsDatasetReader<>(statistics, underlying.reader());
  }

  @Override
  public DatasetWriterReader<K> writerReader() {
    return new StatisticsDatasetWriterReader<>(statistics, underlying.writerReader());
  }

  @Override
  public Indexing getIndexing() {
    return underlying.getIndexing();
  }

  @Override
  public void close() {
    statisticsService.unregisterDataset(statistics.getInstanceName());
    underlying.close();
  }
}
