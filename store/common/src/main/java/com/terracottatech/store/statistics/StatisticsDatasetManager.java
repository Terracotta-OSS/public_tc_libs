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
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.util.Exceptions;

import java.util.Map;

/**
 * StatisticsDatasetManager creating statistics aware dataset instances.
 */
public class StatisticsDatasetManager implements DatasetManager {

  private final StatisticsService statisticsService = new StatisticsService();
  private final DatasetManager underlying;

  public StatisticsDatasetManager(DatasetManager underlying) {
    this.underlying = underlying;
  }

  /**
   * Return the underlying dataset manager.
   *
   * @return the wrapped {@code DatasetManager}
   */
  public DatasetManager getUnderlying() {
    return underlying;
  }

  @Override
  public DatasetManagerConfiguration getDatasetManagerConfiguration() {
    return underlying.getDatasetManagerConfiguration();
  }

  @Override
  public <K extends Comparable<K>> boolean newDataset(String name, Type<K> keyType, DatasetConfiguration configuration) throws StoreException {
    return underlying.newDataset(name, keyType, configuration);
  }

  @Override
  public <K extends Comparable<K>> StatisticsDataset<K> getDataset(String name, Type<K> keyType) throws StoreException {
    return wrapIfNeeded(name, underlying.getDataset(name, keyType));
  }

  @Override
  public Map<String, Type<?>> listDatasets() throws StoreException {
    return underlying.listDatasets();
  }

  @Override
  public boolean destroyDataset(String name) throws StoreException {
    return underlying.destroyDataset(name);
  }

  @Override
  public void close() {
    Exceptions.suppress(underlying::close, statisticsService::close);
  }

  @Override
  public DatasetConfigurationBuilder datasetConfiguration() {
    return underlying.datasetConfiguration();
  }

  private <K extends Comparable<K>> StatisticsDataset<K> wrapIfNeeded(String name, Dataset<K> dataset) {
    if (!(dataset instanceof StatisticsDataset)) {
      return new StatisticsDataset<>(dataset, name, statisticsService);
    }
    return (StatisticsDataset<K>) dataset;
  }

  /**
   * Retrieve the {@link StatisticsService} used to keep all the statistics for this dataset manager.
   *
   * @return the statistics service
   */
  public StatisticsService getStatisticsService() {
    return statisticsService;
  }
}
