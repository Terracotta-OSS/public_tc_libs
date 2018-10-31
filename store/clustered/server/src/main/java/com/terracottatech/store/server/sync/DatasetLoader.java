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
package com.terracottatech.store.server.sync;

import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription;
import com.terracottatech.sovereign.impl.memory.AbstractRecordContainer;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignPrimaryMap;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.server.RawDataset;
import com.terracottatech.store.server.messages.replication.BatchedDataSyncMessage;
import com.terracottatech.store.server.messages.replication.MetadataSyncMessage;
import com.terracottatech.store.server.replication.ReplicationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.terracottatech.store.server.concurrency.ConcurrencyShardMapper.concurrencyKeyToShardIndex;

public class DatasetLoader<K extends Comparable<K>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetLoader.class);

  private final SovereignDatasetImpl<K> dataset;
  private final SovereignPrimaryMap<K> primary;
  private final Map<CellDefinition<? extends Comparable<?>>, IndexSettings> pendingIndexDescriptions = new ConcurrentHashMap<>();

  public DatasetLoader(RawDataset<K> dataset) {
    this.dataset = dataset.getDataset();
    this.primary = dataset.getDataset().getPrimary();
  }

  public void endSync(int concurrencyKey) {
    int shardIdx = concurrencyKeyToShardIndex(concurrencyKey);
    if (shardIdx < dataset.getContainer().getShards().size()) {
      dataset.flush();
      try {
        dataset.getIndexing().buildIndexFor(shardIdx);
      } catch (IOException e) {
        LOGGER.error("Failed to build index while syncing", e);
      }
    }
  }

  public void load(BatchedDataSyncMessage batchedDataSyncMessage) {
    int shardIdx = batchedDataSyncMessage.getShardIdx();
    AbstractRecordContainer<K> shard = getShard(shardIdx);
    batchedDataSyncMessage.getDataTuples().forEach(dataTuple -> {
      long index = dataTuple.index();
      if (dataTuple.getData() == null) {
        K key = shard.deleteIfPresent(index);
        if (key != null) {
          primary.remove(null, key, new PersistentMemoryLocator(index, null));
        }
      } else {
        K key = shard.restore(dataTuple.index(), dataTuple.getData());
        primary.reinstall(key, new PersistentMemoryLocator(index, null));
      }
    });
  }

  private AbstractRecordContainer<K> getShard(int shardIdx) {
    return this.dataset.getContainer().getShards().get(shardIdx);
  }

  public void loadMetaData(MetadataSyncMessage metadataSyncMessage) {
    MetadataKey.Tag<?> tag = MetadataKey.Tag.values()[metadataSyncMessage.getTagKey()];
    Object metadata = metadataSyncMessage.getMetadata();
    if (!tag.equals(MetadataKey.Tag.DATASET_DESCR)) {
      ReplicationUtil.setMetadataAndInstallCallbacks(this.dataset, tag, metadata);
    } else {
      SovereignDatasetDescriptionImpl<?, ?> description = (SovereignDatasetDescriptionImpl) metadataSyncMessage.getMetadata();
      List<SimpleIndexDescription<?>> indexDescriptions = description.getIndexDescriptions();
      indexDescriptions.forEach(desc -> {
        try {
          dataset.getIndexing().createIndex(desc.getCellDefinition(), desc.getIndexSettings()).call();
        } catch (Exception e) {
          LOGGER.error("Failed to create index during passive sync on dataset {}, for cell {}", description.getAlias(),
                  desc.getCellDefinition());
        }
      });
    }
  }

  public void addIndex(final CellDefinition<? extends Comparable<?>> cellDefinition, final IndexSettings indexSettings) {
    pendingIndexDescriptions.put(cellDefinition, indexSettings);
  }

  public void deleteIndex(final CellDefinition<?> cellDefinition, final IndexSettings indexSettings) {
    pendingIndexDescriptions.remove(cellDefinition);
  }

  public Map<CellDefinition<? extends Comparable<?>>, IndexSettings> getPendingIndexDescriptions() {
    return pendingIndexDescriptions;
  }

}
