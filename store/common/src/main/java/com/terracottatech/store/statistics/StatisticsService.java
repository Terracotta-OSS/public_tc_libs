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
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides usage statistics on dataset instances.
 */
public class StatisticsService {

  private final ConcurrentMap<String, DatasetStatistics> statistics = new ConcurrentHashMap<>();

  /**
   * Register a dataset instance for statistics tracking.
   *
   * @param datasetName dataset name as passed to
   *                    {@link com.terracottatech.store.manager.DatasetManager#newDataset(String, Type, DatasetConfiguration)} or
   *                    {@link com.terracottatech.store.manager.DatasetManager#getDataset(String, Type)}
   * @param dataset dataset to track
   * @return dataset statistics, contains a name unique to the dataset instance
   */
  DatasetStatistics registerDataset(String datasetName, Dataset<?> dataset) {
    DatasetStatistics datasetStatistics = new DatasetStatistics(datasetName, dataset);
    statistics.put(datasetStatistics.getInstanceName(), datasetStatistics);
    return datasetStatistics;
  }

  /**
   * Unregister a dataset instance to stop statistics tracking.
   *
   * @param name unique dataset instance name
   */
  void unregisterDataset(String name) {
    statistics.remove(name);
  }

  /**
   * All the currently registered dataset instance names.
   *
   * @return currently registered dataset instance names
   */
  public Collection<String> getDatasetInstanceNames() {
    return statistics.keySet();
  }

  /**
   * Returns the statistics for a given dataset instance name.
   *
   * @param name unique dataset instance name
   * @return statistics for the dataset instance
   */
  public DatasetStatistics get(String name) {
    return statistics.get(name);
  }

  /**
   * Returns statistics for all currently registered datasets.
   *
   * @return statistics for all currently registered datasets
   */
  public Collection<DatasetStatistics> getDatasetStatistics() {
    return statistics.values();
  }

  /**
   * Cleanup all the referenced dataset statistics. This should be done only when {@code DatasetManager} is closed in
   * order to clear memory. Doing it on an active {@code DatasetManager} will have you loose track of your statistics.
   */
  void close() {
    statistics.clear();
  }
}
