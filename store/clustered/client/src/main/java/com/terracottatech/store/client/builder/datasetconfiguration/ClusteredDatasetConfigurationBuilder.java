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
package com.terracottatech.store.client.builder.datasetconfiguration;

import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.BaseDiskDurability;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

public class ClusteredDatasetConfigurationBuilder implements AdvancedDatasetConfigurationBuilder {
  private final String offheapResource;
  private final String diskResource;
  private final PersistentStorageType storageType;
  private final Map<CellDefinition<?>, IndexSettings> indexes;
  private final Integer concurrencyHint;
  private final DiskDurability durability;

  public ClusteredDatasetConfigurationBuilder() {
    this(null, null, emptyMap(), null, null, null);
  }

  private ClusteredDatasetConfigurationBuilder(String offheapResource, String diskResource, Map<CellDefinition<?>,
      IndexSettings> indexes, Integer concurrencyHint, DiskDurability durability, PersistentStorageType storageType) {
    this.offheapResource = offheapResource;
    this.diskResource = diskResource;
    this.indexes = indexes;
    this.concurrencyHint = concurrencyHint;
    this.durability = durability;
    this.storageType = storageType;
  }

  @Override
  public DatasetConfiguration build() {
    if (offheapResource == null) {
      throw new IllegalStateException("No offheap resource has been specified");
    }

    return new ClusteredDatasetConfiguration(offheapResource, diskResource, indexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder offheap(String resourceName) {
    return new ClusteredDatasetConfigurationBuilder(resourceName, diskResource, indexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder disk(String resourceName) {
    return new ClusteredDatasetConfigurationBuilder(offheapResource, resourceName, indexes, concurrencyHint, durability, null);
  }

  @Override
  public DatasetConfigurationBuilder disk(String resourceName, PersistentStorageType storageType) {
    return new ClusteredDatasetConfigurationBuilder(offheapResource, resourceName, indexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder index(CellDefinition<?> cellDefinition, IndexSettings settings) {
    Map<CellDefinition<?>, IndexSettings> newIndexes = new HashMap<>(indexes);
    newIndexes.put(cellDefinition, settings);
    return new ClusteredDatasetConfigurationBuilder(offheapResource, diskResource, newIndexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder durabilityEventual() {
    return new ClusteredDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                    BaseDiskDurability.OS_DETERMINED, storageType);
  }

  @Override
  public DatasetConfigurationBuilder durabilityEveryMutation() {
    return new ClusteredDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                    BaseDiskDurability.ALWAYS, storageType);
  }

  @Override
  public DatasetConfigurationBuilder durabilityTimed(long duration, TimeUnit units) {
    return new ClusteredDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                    BaseDiskDurability.timed(duration, units), storageType);
  }

  @Override
  public AdvancedDatasetConfigurationBuilder advanced() {
    return new ClusteredDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                    durability, storageType);
  }

  @Override
  public AdvancedDatasetConfigurationBuilder concurrencyHint(int hint) {
    return new ClusteredDatasetConfigurationBuilder(offheapResource, diskResource, indexes, hint, durability, storageType);
  }
}
