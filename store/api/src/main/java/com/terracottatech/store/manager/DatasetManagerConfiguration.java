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

package com.terracottatech.store.manager;

import com.terracottatech.store.configuration.DatasetConfiguration;

import java.util.Map;

public interface DatasetManagerConfiguration {

  enum Type {
    CLUSTERED,
    EMBEDDED
  }

  Type getType();

  /**
   * Gives info of {@link com.terracottatech.store.Dataset}s configured in this
   * {@link DatasetManagerConfiguration} such as key type and {@link DatasetConfiguration}
   *
   * @return a map of Dataset name to its information
   */
  Map<String, DatasetInfo<?>> getDatasets();

  class DatasetInfo<K extends Comparable<K>> {
    private final com.terracottatech.store.Type<K> type;
    private final DatasetConfiguration datasetConfiguration;

    public DatasetInfo(com.terracottatech.store.Type<K> type, DatasetConfiguration datasetConfiguration) {
      this.type = type;
      this.datasetConfiguration = datasetConfiguration;
    }

    public com.terracottatech.store.Type<K> getType() {
      return type;
    }

    public DatasetConfiguration getDatasetConfiguration() {
      return datasetConfiguration;
    }
  }
}
