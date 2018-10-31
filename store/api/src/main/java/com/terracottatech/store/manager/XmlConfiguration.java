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

import java.net.URL;


/**
 * Helper class for converting XML configuration to {@link DatasetManagerConfiguration} and vice versa
 */
public interface XmlConfiguration {
  /**
   * Creates a {@link DatasetManagerConfiguration} with the given XML configuration
   *
   * @param configurationLocation XML configuration location
   * @return A {@link DatasetManagerConfiguration} that represents given XML configuration
   *
   * @throws DatasetManagerConfigurationException if there are any configuration issues
   */
  static DatasetManagerConfiguration parseDatasetManagerConfig(URL configurationLocation) throws DatasetManagerConfigurationException {
    return DatasetManagerConfigurationParser.getDatasetManagerConfigurationParser().parse(configurationLocation);
  }

  /**
   * Converts given {@link DatasetManagerConfiguration} to XML configuration
   *
   * @return Converted XML configuration in String form
   */
  static String toXml(DatasetManagerConfiguration datasetManagerConfiguration) {
    return DatasetManagerConfigurationParser.getDatasetManagerConfigurationParser().unparse(datasetManagerConfiguration);
  }
}
