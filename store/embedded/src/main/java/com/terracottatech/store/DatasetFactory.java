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
package com.terracottatech.store;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import com.terracottatech.store.common.ExceptionFreeAutoCloseable;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DatasetFactory implements ExceptionFreeAutoCloseable, DatasetDiscoveryListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetFactory.class);

  private final StorageFactory storageFactory;
  private final ConcurrentMap<UUID, Tuple<String, SovereignStorage<?, ?>>> datasetStorage = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  public DatasetFactory(StorageFactory storageFactory) {
    this.storageFactory = storageFactory;
  }

  @Override
  public <K extends Comparable<K>> void foundExistingDataset(String diskResourceName, SovereignDataset<K> dataset) {
    UUID datasetUUID = dataset.getUUID();
    SovereignStorage<?, ?> storage = dataset.getStorage();
    String storageKey = StorageFactory.buildStorageKey(diskResourceName);
    Tuple<String, SovereignStorage<?, ?>> storageTuple = Tuple.of(storageKey, storage);
    datasetStorage.put(datasetUUID, storageTuple);
  }

  public <K extends Comparable<K>> SovereignDataset<K> create(String alias, Type<K> keyType, EmbeddedDatasetConfiguration configuration) throws StoreException {
    SovereignStorage<?, ?> storage = storageFactory.getStorage(configuration);

    String offheapResource = configuration.getOffheapResource();

    SovereignBuilder<K, SystemTimeReference> sovereignBuilder = new SovereignBuilder<>(keyType, SystemTimeReference.class)
        .storage(storage)
        .alias(alias)
        .offheapResourceName(offheapResource)
        .timeReferenceGenerator(new SystemTimeReference.Generator())
        .offheap();
    configuration.getConcurrencyHint().ifPresent(sovereignBuilder::concurrency);
    configuration.getDiskDurability()
      .ifPresent((d) -> sovereignBuilder.diskDurability(SovereignDatasetDiskDurability.convert(d)));
    SovereignDataset<K> dataset;
    try {
      dataset = sovereignBuilder.build();
    } catch (RuntimeException e) {
      throw new StoreException("Unable to create dataset '" + alias + "': " + e, e);
    }

    UUID uuid = dataset.getUUID();
    try {
      for (Map.Entry<CellDefinition<?>, IndexSettings> index : configuration.getIndexes().entrySet()) {
        createPredefinedIndex(dataset, index.getKey(), index.getValue());
      }
    } catch (Throwable t) {
      try {
        storage.destroyDataSet(uuid);
        if (datasetStorage.isEmpty()) {
          String storageKey = StorageFactory.buildStorageKey(configuration);
          storageFactory.shutdownStorage(storageKey, storage);
        }
      } catch (IOException e) {
        LOGGER.error("Failed to destroy dataset '{}' (UUID: {}) after indexing failed", alias, uuid, e, e);
      }
      throw t;
    }

    String storageKey = StorageFactory.buildStorageKey(configuration);
    Tuple<String, SovereignStorage<?, ?>> previousValue = datasetStorage.put(uuid, Tuple.of(storageKey, storage));
    assert (previousValue == null);
    return dataset;
  }

  private <K extends Comparable<K>, T extends Comparable<T>>
  void createPredefinedIndex(SovereignDataset<K> dataset, CellDefinition<?> cellDefinition, IndexSettings indexDefinition)
      throws StoreException {
    try {
      @SuppressWarnings("unchecked") CellDefinition<T> definition = (CellDefinition<T>)cellDefinition;
      dataset.getIndexing().createIndex(definition, convertIndexSettings(indexDefinition)).call();
    } catch (Exception e) {
      throw new StoreException("Failed to create index for '" + dataset.getAlias()
          + "' [" + indexDefinition + ':' + cellDefinition + ']', e);
    }
  }

  public void destroy(SovereignDataset<?> dataset) throws StoreException {
    UUID uuid = dataset.getUUID();

    Tuple<String, SovereignStorage<?, ?>> storageTuple = datasetStorage.remove(uuid);
    String storageKey = storageTuple.getFirst();
    SovereignStorage<?, ?> storage = storageTuple.getSecond();
    assert (storage != null);

    try {
      storage.destroyDataSet(uuid);
      if (datasetStorage.isEmpty()) {
        storageFactory.shutdownStorage(storageKey, storage);
      }
    } catch (IOException e) {
      throw new StoreException("Failed to destroy dataset", e);
    }
  }

  @Override
  public void close() {
    boolean closing = closed.compareAndSet(false, true);

    if (closing) {
      storageFactory.close();
    }
  }

  private static SovereignIndexSettings convertIndexSettings(IndexSettings setting) {
    if (setting.equals(IndexSettings.btree())) {
      return SovereignIndexSettings.BTREE;
    } else {
      throw new IllegalArgumentException("Unsupported index setting: " + setting);
    }
  }

  public void checkConfiguration(EmbeddedDatasetConfiguration configuration) {
    storageFactory.checkConfiguration(configuration);
  }
}
