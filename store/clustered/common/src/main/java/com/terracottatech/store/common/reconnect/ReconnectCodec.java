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
package com.terracottatech.store.common.reconnect;

import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import org.terracotta.runnel.Struct;

import java.nio.ByteBuffer;
import java.util.UUID;

public class ReconnectCodec {
  private static final Struct reconnectStruct = new DatasetStructBuilder(null, null)
          .bool("sendChangeEvents", 10)
          .uuid("stableClientId", 30)
          .build();

  public byte[] encode(ReconnectState reconnectState) {
    return new DatasetStructEncoder(reconnectStruct.encoder(), null, null)
            .bool("sendChangeEvents", reconnectState.sendChangeEvents())
            .uuid("stableClientId", reconnectState.getStableClientId())
            .getUnderlying().encode().array();
  }

  public ReconnectState decode(byte[] data) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    DatasetStructDecoder decoder = new DatasetStructDecoder(reconnectStruct.decoder(byteBuffer), null, null);
    Boolean sendChangeEvents = decoder.bool("sendChangeEvents");
    UUID stableClientId = decoder.uuid("stableClientId");

    return new ReconnectState(sendChangeEvents, stableClientId);
  }
}
