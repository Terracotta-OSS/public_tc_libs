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
package com.terracottatech.store.builder;

import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.BaseDiskDurability;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class EmbeddedDatasetConfigurationBuilder implements AdvancedDatasetConfigurationBuilder {
  private final String offheapResource;
  private final String diskResource;
  private final Map<CellDefinition<?>, IndexSettings> indexes;
  private final Integer concurrencyHint;
  private final DiskDurability durability;
  private final PersistentStorageType storageType;

  public EmbeddedDatasetConfigurationBuilder() {
    this(null, null, new HashMap<>(), null, null, null);
  }

  private EmbeddedDatasetConfigurationBuilder(String offheapResource, String diskResource, Map<CellDefinition<?>,
    IndexSettings> indexes, Integer concurrencyHint, DiskDurability durability, PersistentStorageType storageType) {
    this.offheapResource = offheapResource;
    this.diskResource = diskResource;
    this.indexes = Collections.unmodifiableMap(indexes);
    this.concurrencyHint = concurrencyHint;
    this.durability = durability;
    this.storageType = storageType;
  }

  @Override
  public DatasetConfiguration build() {
    if (offheapResource == null) {
      throw new StoreRuntimeException("No offheap resource has been specified");
    }

    return new EmbeddedDatasetConfiguration(offheapResource, diskResource, indexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder offheap(String offheapResource) {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder disk(String diskResource) {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder disk(String diskResource, PersistentStorageType storageType) {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder index(CellDefinition<?> cellDefinition, IndexSettings settings) {
    Map<CellDefinition<?>, IndexSettings> newIndexes = new HashMap<>(indexes);
    newIndexes.put(cellDefinition, settings);
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, newIndexes, concurrencyHint, durability, storageType);
  }

  @Override
  public DatasetConfigurationBuilder durabilityEventual() {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                   BaseDiskDurability.OS_DETERMINED, storageType);
  }

  @Override
  public DatasetConfigurationBuilder durabilityEveryMutation() {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                   BaseDiskDurability.ALWAYS, storageType);
  }

  @Override
  public DatasetConfigurationBuilder durabilityTimed(long duration, TimeUnit units) {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                   BaseDiskDurability.timed(duration, units), storageType);
  }

  @Override
  public AdvancedDatasetConfigurationBuilder advanced() {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, concurrencyHint,
                                                          durability, storageType);
  }

  @Override
  public AdvancedDatasetConfigurationBuilder concurrencyHint(int hint) {
    return new EmbeddedDatasetConfigurationBuilder(offheapResource, diskResource, indexes, hint, durability, storageType);
  }
}
