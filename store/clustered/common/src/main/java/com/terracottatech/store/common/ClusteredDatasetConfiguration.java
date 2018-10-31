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

import com.terracottatech.store.configuration.AbstractDatasetConfiguration;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.Map;

public class ClusteredDatasetConfiguration extends AbstractDatasetConfiguration {
  public ClusteredDatasetConfiguration(String offheapResource, String diskResource, Map<CellDefinition<?>, IndexSettings> indexes) {
    this(offheapResource, diskResource, indexes, null);
  }

  public ClusteredDatasetConfiguration(String offheapResource, String diskResource, Map<CellDefinition<?>, IndexSettings> indexes, Integer concurrencyHint) {
    super(offheapResource, diskResource, indexes, concurrencyHint, null, null);
  }

  public ClusteredDatasetConfiguration(String offheapResource, String diskResource, Map<CellDefinition<?>,
    IndexSettings> indexes, Integer concurrencyHint, DiskDurability durability) {
    super(offheapResource, diskResource, indexes, concurrencyHint, durability, null);
  }

  public ClusteredDatasetConfiguration(String offheapResource, String diskResource, Map<CellDefinition<?>,
      IndexSettings> indexes, Integer concurrencyHint, DiskDurability durability, PersistentStorageType storageType) {
    super(offheapResource, diskResource, indexes, concurrencyHint, durability, storageType);
  }
}
