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
package com.terracottatech.store.server.messages.replication;

import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.ServerServerMessage;
import com.terracottatech.store.server.messages.ServerServerMessageType;
import org.terracotta.runnel.Struct;

public class SyncBoundaryMessage extends ReplicationMessage {

  private final int shardIndex;

  public SyncBoundaryMessage(int shardIndex) {
    this.shardIndex = shardIndex;
  }

  public int getShardIndex() {
    return shardIndex;
  }

  @Override
  public ReplicationMessageType getType() {
    return ReplicationMessageType.SYNC_BOUNDARY_MESSAGE;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.int32("shardIdx", 10).build();
  }

  public static void encode(DatasetStructEncoder encoder, ReplicationMessage message) {
    encoder.int32("shardIdx", ((SyncBoundaryMessage)message).getShardIndex());
  }

  public static SyncBoundaryMessage decode(DatasetStructDecoder decoder) {
    int shardIdx = decoder.int32("shardIdx");
    return new SyncBoundaryMessage(shardIdx);
  }
}
