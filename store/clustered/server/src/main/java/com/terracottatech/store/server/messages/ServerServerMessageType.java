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
package com.terracottatech.store.server.messages;

import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.MessageComponent;
import com.terracottatech.store.server.messages.replication.BatchedDataSyncMessage;
import com.terracottatech.store.server.messages.replication.MessageTrackerSyncMessage;
import com.terracottatech.store.server.messages.replication.MetadataSyncMessage;
import org.terracotta.runnel.Struct;

import java.util.function.BiConsumer;
import java.util.function.Function;

public enum ServerServerMessageType implements MessageComponent<ServerServerMessage> {

  BATCHED_SYNC_MESSAGE(BatchedDataSyncMessage::struct, BatchedDataSyncMessage::encode, BatchedDataSyncMessage::decode),
  METADATA_SYNC_MESSAGE(MetadataSyncMessage::struct, MetadataSyncMessage::encode, MetadataSyncMessage::decode),
  MESSAGE_TRACKER_SYNC_MESSAGE(MessageTrackerSyncMessage::struct, MessageTrackerSyncMessage::encode, MessageTrackerSyncMessage::decode);

  private final Function<DatasetStructBuilder, Struct> structGenerator;
  private final BiConsumer<DatasetStructEncoder, ServerServerMessage> encodingFunction;
  private final Function<DatasetStructDecoder, ? extends ServerServerMessage> decodingFunction;

  ServerServerMessageType(
          Function<DatasetStructBuilder, Struct> structGenerator,
          BiConsumer<DatasetStructEncoder,  ServerServerMessage> encodingFunction,
          Function<DatasetStructDecoder, ? extends ServerServerMessage> decodingFunction) {
    this.structGenerator = structGenerator;
    this.encodingFunction = encodingFunction;
    this.decodingFunction = decodingFunction;
  }

  @Override
  public Struct struct(DatasetStructBuilder builder) {
    return structGenerator.apply(builder);
  }

  @Override
  public void encode(DatasetStructEncoder encoder, ServerServerMessage message) {
    encodingFunction.accept(encoder, message);
  }

  @Override
  public ServerServerMessage decode(DatasetStructDecoder decoder) {
    return decodingFunction.apply(decoder);
  }
}
