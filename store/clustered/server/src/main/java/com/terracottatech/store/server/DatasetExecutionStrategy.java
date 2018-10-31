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

import com.terracottatech.store.common.messages.DatasetOperationMessage;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateMessage;
import com.terracottatech.store.common.messages.indexing.IndexDestroyMessage;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.server.messages.ServerServerMessage;
import org.terracotta.entity.ExecutionStrategy;

public class DatasetExecutionStrategy implements ExecutionStrategy<DatasetEntityMessage> {
  @Override
  public Location getExecutionLocation(DatasetEntityMessage message) {
    if (message instanceof ServerServerMessage) {
      return Location.PASSIVE;
    } else if (message.getType() == ReplicationMessageType.DATA_REPLICATION_MESSAGE) {
      return Location.BOTH;
    } else if (message instanceof ReplicationMessage) {
      return Location.PASSIVE;
    } else if (message instanceof ExecuteTerminatedPipelineMessage && ((ExecuteTerminatedPipelineMessage) message).isMutative()) {
      return Location.BOTH;
    } else if (message instanceof IndexCreateMessage || message instanceof IndexDestroyMessage) {
      return Location.BOTH;
    } else if (message instanceof DatasetOperationMessage) {
      return Location.ACTIVE;
    } else {
      throw new UnsupportedOperationException("Unsupported message");
    }
  }
}
