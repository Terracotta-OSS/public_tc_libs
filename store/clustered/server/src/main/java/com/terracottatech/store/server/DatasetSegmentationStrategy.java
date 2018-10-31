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
package com.terracottatech.store.server;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessage;
import com.terracottatech.store.common.messages.crud.KeyMessage;
import com.terracottatech.store.server.concurrency.ConcurrencyShardMapper;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;

import java.util.function.ToIntFunction;

public class DatasetSegmentationStrategy implements ToIntFunction<DatasetEntityMessage> {

  private final ConcurrencyShardMapper concurrencyShardMapper;

  public DatasetSegmentationStrategy(ConcurrencyShardMapper concurrencyShardMapper) {
    this.concurrencyShardMapper = concurrencyShardMapper;
  }

  @Override
  public int applyAsInt(DatasetEntityMessage entityMessage) {
    if (entityMessage instanceof DatasetOperationMessage) {
      switch (((DatasetOperationMessage) entityMessage).getType()) {
        case ADD_RECORD_MESSAGE:
        case PREDICATED_UPDATE_RECORD_MESSAGE:
        case PREDICATED_DELETE_RECORD_MESSAGE:
          return concurrencyShardMapper.getShardIndex(((KeyMessage) entityMessage).getKey());
        case INDEX_DESTROY_MESSAGE:
        case INDEX_CREATE_MESSAGE:
        case EXECUTE_TERMINATED_PIPELINE_MESSAGE:
          return concurrencyShardMapper.getShardCount();
      }
    } else if (entityMessage instanceof ReplicationMessage) {
      switch (((ReplicationMessage) entityMessage).getType()) {
        case CRUD_DATA_REPLICATION_MESSAGE:
          return concurrencyShardMapper.getShardIndex(((CRUDDataReplicationMessage) entityMessage).getIndex());
      }
    }
    throw new AssertionError("The entity message " + entityMessage + " is not trackable");
  }
}
