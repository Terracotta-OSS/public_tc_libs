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
package com.terracottatech.store.manager.config;

import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.DatasetManagerConfiguration.DatasetInfo;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration.ResourceConfiguration;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.PersistenceMode;

public class EmbeddedDatasetManagerConfigurationBuilder {
  private final Map<String, Long> offheapResources = new ConcurrentHashMap<>();
  private final Map<String, DiskResource> diskResources = new ConcurrentHashMap<>();
  private final Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = new ConcurrentHashMap<>();

  public EmbeddedDatasetManagerConfigurationBuilder() {
    this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
  }

  public EmbeddedDatasetManagerConfigurationBuilder(Map<String, Long> offheapResources,
                                                    Map<String, DiskResource> diskResources) {
    this.offheapResources.putAll(offheapResources);
    this.diskResources.putAll(diskResources);
  }

  private EmbeddedDatasetManagerConfigurationBuilder(EmbeddedDatasetManagerConfigurationBuilder original,
                                                     Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets) {
    this.offheapResources.putAll(original.offheapResources);
    this.diskResources.putAll(original.diskResources);
    this.datasets.putAll(datasets);
  }

  public EmbeddedDatasetManagerConfigurationBuilder offheap(String resource, long unitCount, MemoryUnit memoryUnit) {
    long bytes = memoryUnit.toBytes(unitCount);

    Map<String, Long> newOffheapResources = new HashMap<>(offheapResources);
    newOffheapResources.put(resource, bytes);

    return new EmbeddedDatasetManagerConfigurationBuilder(newOffheapResources, diskResources);
  }

  public EmbeddedDatasetManagerConfigurationBuilder disk(String resource, Path dataRootDirectory,
                                                         PersistenceMode persistenceMode, FileMode fileMode) {
    DiskResource diskResource = new DiskResource(dataRootDirectory, persistenceMode.getStorageType(), fileMode);

    Map<String, DiskResource> newDiskResources = new HashMap<>(diskResources);
    newDiskResources.put(resource, diskResource);

    return new EmbeddedDatasetManagerConfigurationBuilder(offheapResources, newDiskResources);
  }

  public EmbeddedDatasetManagerConfigurationBuilder disk(String resource, Path dataRootDirectory, FileMode fileMode) {
    DiskResource diskResource = new DiskResource(dataRootDirectory, null, fileMode);

    Map<String, DiskResource> newDiskResources = new HashMap<>(diskResources);
    newDiskResources.put(resource, diskResource);

    return new EmbeddedDatasetManagerConfigurationBuilder(offheapResources, newDiskResources);
  }

  public EmbeddedDatasetManagerConfigurationBuilder datasetConfigurations(Map<String, DatasetInfo<?>> datasets) {
    return new EmbeddedDatasetManagerConfigurationBuilder(this, datasets);
  }

  public EmbeddedDatasetManagerConfiguration build() {
    return new ResourceConfiguration(offheapResources, diskResources).withDatasetsConfiguration(datasets);
  }
}