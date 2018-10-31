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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.memory.ShardIterator;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.server.messages.replication.BatchedDataSyncMessage;
import com.terracottatech.store.server.messages.replication.MetadataSyncMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PassiveSynchronizationChannel;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class DatasetSynchronizer<K extends Comparable<K>> {

  private static final int INDEX_SIZE = 8;

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetSynchronizer.class);
  private static final long DATA_SIZE_THRESHOLD = Long.getLong("store.sync.data.size.threshold", 4194304L);

  private final ConcurrentMap<PassiveSynchronizationChannel<?>, ChangeListener> onGoingPassiveSyncs = new ConcurrentHashMap<>();
  private final Supplier<SovereignDataset<K>> datasetSupplier;

  public DatasetSynchronizer(Supplier<SovereignDataset<K>> datasetSupplier) {
    this.datasetSupplier = datasetSupplier;
  }

  public void startSync(PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel) {
    synchronizationChannel.synchronizeToPassive(new MetadataSyncMessage(getDataset().getDescription(),
            MetadataKey.Tag.DATASET_DESCR.ordinal()));
  }

  public void synchronizeShard(PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel, int shardIndex) {
    if (shardIndex == 0 && !onGoingPassiveSyncs.containsKey(synchronizationChannel)) {
      onGoingPassiveSyncs.put(synchronizationChannel, new ChangeListener());
    }
    ChangeListener listener = onGoingPassiveSyncs.get(synchronizationChannel);
    if (listener == null) {
      throw new IllegalStateException("Cannot synchronize with channel as it never started" + synchronizationChannel);
    }

    LOGGER.info("Non blocking sync shard {} to passive for dataset : {}", shardIndex, this.getDataset().getAlias());
    getDataset().addChangeListener(shardIndex, listener);
    ShardIterator shardIterator = getDataset().iterate(shardIndex);
    sendBatchesToPassive(synchronizationChannel, shardIndex, shardIterator);
  }

  private SovereignDatasetImpl<K> getDataset() {
    return (SovereignDatasetImpl<K>) this.datasetSupplier.get();
  }

  public void synchronizeRecordedMutations(PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel, int shardIndex) {
    if (!onGoingPassiveSyncs.containsKey(synchronizationChannel)) {
      throw new IllegalStateException("Cannot synchronize with channel as it never started" + synchronizationChannel);
    }

    LOGGER.info("Blocking sync shard {} to passive for dataset : {}", shardIndex, this.getDataset().getAlias());
    getDataset().consumeMutationsThenRemoveListener(shardIndex, onGoingPassiveSyncs.get(synchronizationChannel), (bufferDataTuples) -> {
      sendBatchesToPassive(synchronizationChannel, shardIndex, bufferDataTuples.iterator());

      synchronizationChannel.synchronizeToPassive(new MetadataSyncMessage(getDataset().getRuntime().getSchema()
                                                                            .getBackend().getPersistable(),
              MetadataKey.Tag.SCHEMA.ordinal()));

      synchronizationChannel.synchronizeToPassive(new MetadataSyncMessage(getDataset().getRuntime().getSequence(),
              MetadataKey.Tag.CACHING_SEQUENCE.ordinal()));

    });

    if (shardIndex == (getDataset().getContainer().getShards().size() - 1)) {
      onGoingPassiveSyncs.remove(synchronizationChannel);
    }

  }

  private void sendBatchesToPassive(PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel, int shardIndex, Iterator<BufferDataTuple> tupleIterator) {
    long payloadSize = 0;
    List<BufferDataTuple> payload = new LinkedList<>();
    while (tupleIterator.hasNext()) {
      BufferDataTuple data = tupleIterator.next();
      int dataSize = data.getData() == null? INDEX_SIZE : data.getData().remaining() + INDEX_SIZE;
      if ((payloadSize + dataSize) > DATA_SIZE_THRESHOLD) {
        synchronizationChannel.synchronizeToPassive(new BatchedDataSyncMessage(payload, shardIndex));
        payload = new LinkedList<>();
        payloadSize = 0;
      }
      payloadSize += dataSize;
      payload.add(data);
    }
    if (!payload.isEmpty()) {
      synchronizationChannel.synchronizeToPassive(new BatchedDataSyncMessage(payload, shardIndex));
    }
  }

  //Only for tests
  ConcurrentMap<PassiveSynchronizationChannel<?>, ChangeListener> getOnGoingPassiveSyncs() {
    return onGoingPassiveSyncs;
  }
}
