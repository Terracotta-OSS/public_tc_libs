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
package com.terracottatech.store.common.messages.indexing;

import org.terracotta.runnel.Struct;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.UniversalMessage;

import java.util.UUID;

/**
 * Message to request the completion status of an index creation request started by
 * a {@link IndexCreateMessage}.
 *
 * @see IndexCreateAcceptedResponse
 */
public class IndexCreateStatusMessage extends UniversalMessage {

  private final String creationRequestId;
  private final UUID stableClientId;

  /**
   * Creates a new {@code IndexCreateStatusMessage}.
   * @param creationRequestId the creation request identifier obtained from
   *                          {@link IndexCreateAcceptedResponse#getCreationRequestId()}
   */
  public IndexCreateStatusMessage(String creationRequestId, UUID stableClientId) {
    this.creationRequestId = creationRequestId;
    this.stableClientId = stableClientId;
  }

  public String getCreationRequestId() {
    return creationRequestId;
  }

  public UUID getStableClientId() {
    return stableClientId;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.INDEX_CREATE_STATUS_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("IndexCreateStatusMessage{");
    sb.append("creationRequestId='").append(creationRequestId).append('\'');
    sb.append(", stableClientId=").append(stableClientId);
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    datasetStructBuilder
        .string("creationRequestId", 10)
        .uuid("stableClientId", 20);
    return datasetStructBuilder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    IndexCreateStatusMessage statusMessage = (IndexCreateStatusMessage)message;
    encoder
        .string("creationRequestId", statusMessage.getCreationRequestId())
        .uuid("stableClientId", statusMessage.getStableClientId());
  }

  public static IndexCreateStatusMessage decode(DatasetStructDecoder decoder) {
    String creationRequestId = decoder.string("creationRequestId");
    UUID stableClientId = decoder.uuid("stableClientId");
    return new IndexCreateStatusMessage(creationRequestId, stableClientId);
  }
}
