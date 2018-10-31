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
package com.terracottatech.store.server.concurrency;

import com.terracottatech.store.server.messages.replication.SyncBoundaryMessage;
import org.terracotta.entity.ConcurrencyStrategy;

import com.terracottatech.sovereign.impl.memory.KeySlotShardEngine;
import com.terracottatech.store.server.messages.replication.DataReplicationMessage;
import com.terracottatech.store.server.messages.replication.MetadataReplicationMessage;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;
import com.terracottatech.store.server.replication.ReplicationUtil;

import java.util.Set;

public class ReplicationConcurrencyStrategy implements ConcurrencyStrategy<ReplicationMessage> {

  private final ConcurrencyShardMapper concurrencyShardMapper;

  public ReplicationConcurrencyStrategy(ConcurrencyShardMapper concurrencyShardMapper) {
    this.concurrencyShardMapper = concurrencyShardMapper;
  }

  @Override
  public int concurrencyKey(ReplicationMessage replicationMessage) {
    if (replicationMessage instanceof MetadataReplicationMessage) {
      return MANAGEMENT_KEY;
    } else if (replicationMessage instanceof DataReplicationMessage) {
      return concurrencyShardMapper.getConcurrencyKey(((DataReplicationMessage) replicationMessage).getIndex());
    } else if (replicationMessage instanceof SyncBoundaryMessage) {
      return ConcurrencyShardMapper.shardIndexToConcurrencyKey(((SyncBoundaryMessage) replicationMessage).getShardIndex());
    } else {
      throw new AssertionError(ReplicationConcurrencyStrategy.class + " does not support message type: " + replicationMessage.getClass());
    }
  }

  @Override
  public Set<Integer> getKeysForSynchronization() {
    throw new UnsupportedOperationException("This concurrency strategy is not meant to be used directly by the entity.");
  }
}
