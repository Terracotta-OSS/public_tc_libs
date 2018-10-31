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
package com.terracottatech.store.server.replication;

import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexing;
import com.terracottatech.sovereign.impl.memory.AbstractRecordContainer;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.RecordBufferStrategy;
import com.terracottatech.sovereign.impl.memory.ShardedRecordContainer;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.impl.model.SovereignPrimaryMap;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.crud.AddRecordFullResponse;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.NullCRUDReplicationResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordSimplifiedResponse;
import com.terracottatech.store.server.RawDataset;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.messages.replication.DataReplicationMessage;
import com.terracottatech.store.server.messages.replication.MetadataReplicationMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

public class DatasetReplicationListener<K extends Comparable<K>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetReplicationListener.class);

  private final SovereignDatasetImpl<K> dataset;
  private final SovereignPrimaryMap<K> primary;
  private final SimpleIndexing<K> indexing;
  private final RecordBufferStrategy<K> bufferStrategy;
  private final Set<Integer> shards = new HashSet<>();

  public DatasetReplicationListener(RawDataset<K> dataset) {
    SovereignDatasetImpl<K> sovereignDataset = dataset.getDataset();
    this.dataset = sovereignDataset;
    this.primary = sovereignDataset.getPrimary();
    this.indexing = sovereignDataset.getIndexing();
    this.bufferStrategy = sovereignDataset.getRuntime().getBufferStrategy();
    IntStream.range(0, this.dataset.getContainer().getShards().size()).forEach(shards::add);
  }

  public void handleReplicatedMetadata(MetadataReplicationMessage metadataReplicationMessage) {
    MetadataKey.Tag<?> tag = MetadataKey.Tag.values()[metadataReplicationMessage.getIndex()];
    Object metadata = metadataReplicationMessage.getMetadata();
    ReplicationUtil.setMetadataAndInstallCallbacks(this.dataset, tag, metadata);
  }

  public void prepareForSync() {
    shards.clear();
  }

  public void startAcceptingCRUDReplicationForShard(int shardIdx) {
    LOGGER.debug("Start accepting ops for shard {} for dataset {}", shardIdx, dataset.getAlias());
    shards.add(shardIdx);
  }

  public DatasetEntityResponse handleReplicatedData(DataReplicationMessage dataReplicationMessage) {
    long index = dataReplicationMessage.getIndex();
    ByteBuffer data = dataReplicationMessage.getData();

    boolean shouldRespond = dataReplicationMessage instanceof CRUDDataReplicationMessage;
    boolean respondInFull = shouldRespond && ((CRUDDataReplicationMessage) dataReplicationMessage).isRespondInFull();

    ShardedRecordContainer<K, ?> recordContainer = this.dataset.getContainer();
    int shardIdx = dataset.getRuntime().getShardEngine().extractShardIndexLambda().transmute(index);
    if (shouldRespond && !shards.contains(shardIdx)) {
      LOGGER.debug("Ignoring index {} for shard {} for op {} on dataset {}", index, shardIdx, data == null, dataset.getAlias());
      return new NullCRUDReplicationResponse<>(index, ((CRUDDataReplicationMessage) dataReplicationMessage).getCurrentTransactionId());
    }
    try (ContextImpl context = recordContainer.start(false)) {
      PersistentMemoryLocator locator = new PersistentMemoryLocator(index, null);
      SovereignPersistentRecord<K> record = recordContainer.get(locator);
      if (data == null) {
        if (record == null) {
          throw new IllegalStateException("Passive cannot delete a non-existent record " + index + " for shard " + shardIdx + "for dataset " + dataset.getAlias());
        }
        LOGGER.debug("Deleting at index {} for dataset {}", index, this.dataset.getAlias());

        recordContainer.delete(locator);
        primary.remove(context, record.getKey(), locator);
        indexing.recordIndex(context, record, locator, null, null);
        return createDeleteResponse(shouldRespond, respondInFull, record);
      } else {
        SovereignPersistentRecord<K> recordFromData = bufferStrategy.fromByteBuffer(data);

        AbstractRecordContainer<K> abstractRecordContainer = recordContainer.shardForSlot(index);
        K restoredKey = abstractRecordContainer.restore(index, data);
        primary.reinstall(restoredKey, locator);
        if (record == null) {
          //this is add
          indexing.recordIndex(context, null, null, recordFromData, locator);
          return createAddResponse(shouldRespond, respondInFull);
        } else {
          //this is an update
          indexing.recordIndex(context, record, locator, recordFromData, locator);
          SovereignPersistentRecord<K> newRecord = recordContainer.get(locator);
          return createUpdateResponse(shouldRespond, respondInFull, record, newRecord);
        }
      }
    }
  }

  private DatasetEntityResponse createAddResponse(boolean shouldRespond, boolean respondInFull) {
    if (!shouldRespond) {
      return null;
    }

    if (respondInFull) {
      return new AddRecordFullResponse<>();
    } else {
      return new AddRecordSimplifiedResponse(true);
    }
  }

  private DatasetEntityResponse createUpdateResponse(boolean shouldRespond, boolean respondInFull, SovereignPersistentRecord<K> oldRecord, SovereignPersistentRecord<K> newRecord) {
    if (!shouldRespond) {
      return null;
    }

    if (respondInFull) {
      return new PredicatedUpdateRecordFullResponse<>(oldRecord.getKey(), oldRecord.getMSN(), oldRecord, newRecord.getMSN(), newRecord);
    } else {
      return new PredicatedUpdateRecordSimplifiedResponse(true);
    }

  }

  private DatasetEntityResponse createDeleteResponse(boolean shouldRespond, boolean respondInFull, SovereignPersistentRecord<K> deletedRecord) {
    if (!shouldRespond) {
      return null;
    }

    if (respondInFull) {
      return new PredicatedDeleteRecordFullResponse<>(deletedRecord.getKey(), deletedRecord.getMSN(), deletedRecord);
    } else {
      return new PredicatedDeleteRecordSimplifiedResponse(true);
    }
  }
}
