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
package com.terracottatech.store.common.messages;

import org.terracotta.runnel.Struct;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Used during creation of a {@code DatasetEntity} to establish a client identifier with the server.
 */
public class IdentifyClientMessage extends ManagementMessage {
  private final UUID requestedClientId;

  public IdentifyClientMessage(UUID requestedClientId) {
    this.requestedClientId = requireNonNull(requestedClientId, "requestedClientId");
  }

  public UUID getRequestedClientId() {
    return requestedClientId;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.IDENTIFY_CLIENT_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("IdentifyClientMessage{");
    sb.append("requestedClientId=").append(requestedClientId);
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("requestedClientId", 10).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    IdentifyClientMessage msg = (IdentifyClientMessage)message;
    encoder.uuid("requestedClientId", msg.getRequestedClientId());
  }

  public static IdentifyClientMessage decode(DatasetStructDecoder decoder) {
    return new IdentifyClientMessage(decoder.uuid("requestedClientId"));
  }
}
