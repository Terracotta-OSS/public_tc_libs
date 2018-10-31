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
package com.terracottatech.store.common;

import com.terracottatech.store.Type;

public class DatasetEntityConfiguration<K extends Comparable<K>> {
  private final Type<K> keyType;
  private final String datasetName;
  private final ClusteredDatasetConfiguration configuration;

  public DatasetEntityConfiguration(Type<K> keyType, String datasetName, ClusteredDatasetConfiguration configuration) {
    this.keyType = keyType;
    this.datasetName = datasetName;
    this.configuration = configuration;
  }

  public Type<K> getKeyType() {
    return keyType;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public ClusteredDatasetConfiguration getDatasetConfiguration() {
    return configuration;
  }
}
