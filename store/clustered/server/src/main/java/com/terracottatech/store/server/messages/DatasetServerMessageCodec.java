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

import com.terracottatech.store.common.messages.AbstractMessageCodec;
import com.terracottatech.store.common.messages.DatasetOperationMessageCodec;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.MessageComponent;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;

import java.util.HashMap;
import java.util.Map;

public class DatasetServerMessageCodec extends AbstractMessageCodec {

  private static final Map<Integer, ReplicationMessageType> REPLICATION_MESSAGE_COMPONENTS = new HashMap<>();
  static {
    REPLICATION_MESSAGE_COMPONENTS.put(20, ReplicationMessageType.METADATA_REPLICATION_MESSAGE);
    REPLICATION_MESSAGE_COMPONENTS.put(21, ReplicationMessageType.DATA_REPLICATION_MESSAGE);
    REPLICATION_MESSAGE_COMPONENTS.put(23, ReplicationMessageType.CRUD_DATA_REPLICATION_MESSAGE);
    REPLICATION_MESSAGE_COMPONENTS.put(26, ReplicationMessageType.SYNC_BOUNDARY_MESSAGE);
  }

  public DatasetServerMessageCodec(IntrinsicCodec intrinsicCodec) {
    super(intrinsicCodec,
            getMessageComponentClass(), merge(DatasetOperationMessageCodec.getMessageComponents()),
            DatasetEntityResponseType.class, DatasetOperationMessageCodec.getResponseComponents());
  }

  @SuppressWarnings("unchecked")
  private static Class<MessageComponent<?>> getMessageComponentClass() {
    return (Class<MessageComponent<?>>) (Class) MessageComponent.class;
  }

  private static Map<Integer, MessageComponent<?>> merge(Map<Integer, ? extends MessageComponent<?>> map) {
    Map<Integer, MessageComponent<?>> merged = new HashMap<>(DatasetServerMessageCodec.REPLICATION_MESSAGE_COMPONENTS);
    merged.putAll(map);
    if (merged.size() != map.size() + DatasetServerMessageCodec.REPLICATION_MESSAGE_COMPONENTS.size()) {
      throw new IllegalArgumentException("Maps intersect!");
    } else {
      return merged;
    }
  }
}
