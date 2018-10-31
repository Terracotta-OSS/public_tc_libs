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
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.MessageComponent;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.SyncMessageCodec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SyncDatasetCodec extends AbstractMessageCodec implements SyncMessageCodec<DatasetEntityMessage> {

  private static final Map<Integer, ServerServerMessageType> SERVER_COMPONENTS = new HashMap<>();
  static {
    SERVER_COMPONENTS.put(0, ServerServerMessageType.METADATA_SYNC_MESSAGE);
    SERVER_COMPONENTS.put(1, ServerServerMessageType.BATCHED_SYNC_MESSAGE);
    SERVER_COMPONENTS.put(2, ServerServerMessageType.MESSAGE_TRACKER_SYNC_MESSAGE);
  }

  public SyncDatasetCodec(IntrinsicCodec intrinsicCodec) {
    super(intrinsicCodec, ServerServerMessageType.class, SERVER_COMPONENTS, MessageComponent.class, Collections.emptyMap());
  }

  @Override
  public byte[] encode(int concurrencyKey, DatasetEntityMessage datasetEntityMessage) throws MessageCodecException {
    return encodeMessage(datasetEntityMessage);
  }

  @Override
  public DatasetEntityMessage decode(int concurrencyKey, byte[] bytes) throws MessageCodecException {
    return decodeMessage(bytes);
  }
}
