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
package com.terracottatech.store.configuration;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.Map;
import java.util.Optional;

public abstract class AbstractDatasetConfiguration implements AdvancedDatasetConfiguration {

  private final String offheapResource;
  private final String diskResource;
  private final Map<CellDefinition<?>, IndexSettings> indexes;
  private final Integer concurrencyHint;
  private final DiskDurability diskDurability;
  private final PersistentStorageType storageType;

  public AbstractDatasetConfiguration(String offheapResource,
                                      String diskResource,
                                      Map<CellDefinition<?>, IndexSettings> indexes,
                                      Integer concurrencyHint,
                                      DiskDurability durability,
                                      PersistentStorageType storageType) {
    this.offheapResource = offheapResource;
    this.diskResource = diskResource;
    this.indexes = indexes;
    this.diskDurability = durability;
    if (concurrencyHint != null && concurrencyHint <= 0) {
      throw new IllegalArgumentException("concurrencyHint must be greater than 0 - invalid value is " + concurrencyHint);
    }
    this.concurrencyHint = concurrencyHint;
    this.storageType = storageType;
  }

  public String getOffheapResource() {
    return offheapResource;
  }

  public Optional<String> getDiskResource() {
    return Optional.ofNullable(diskResource);
  }

  public Map<CellDefinition<?>, IndexSettings> getIndexes() {
    return indexes;
  }

  public Optional<DiskDurability> getDiskDurability() {
    return Optional.ofNullable(diskDurability);
  }

  @Override
  public Optional<Integer> getConcurrencyHint() {
    if (concurrencyHint != null) {
      return Optional.of(concurrencyHint);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Optional<PersistentStorageType> getPersistentStorageType() {
    return Optional.ofNullable(storageType);
  }
}
