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
package com.terracottatech.store.server.storage.configuration;

import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.configuration.PersistentStorageType;

import java.util.Optional;

public class StorageConfigurationFactory {

  public static StorageConfiguration create(DatasetEntityConfiguration<?> entityConfiguration) {
    ClusteredDatasetConfiguration datasetConfiguration = entityConfiguration.getDatasetConfiguration();

    String offheapResource = datasetConfiguration.getOffheapResource();
    Optional<String> diskResource = datasetConfiguration.getDiskResource();
    Optional<PersistentStorageType> storageType = datasetConfiguration.getPersistentStorageType();

    return diskResource.map(dr -> (StorageConfiguration) new PersistentStorageConfiguration(dr, offheapResource,
        storageType.orElse(PersistentStorageType.defaultEngine()) ))
            .orElse(new MemoryStorageConfiguration(offheapResource));
  }
}
