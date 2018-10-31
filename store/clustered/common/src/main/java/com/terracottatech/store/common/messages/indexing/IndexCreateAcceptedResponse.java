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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;

/**
 * Indicates that a {@link IndexCreateMessage} was accepted and provides the identifier to use
 * for obtaining the creation completion status.
 *
 * @see IndexCreateMessage
 */
public class IndexCreateAcceptedResponse extends DatasetEntityResponse {

  private final String creationRequestId;

  public IndexCreateAcceptedResponse(String creationRequestId) {
    this.creationRequestId = creationRequestId;
  }

  public String getCreationRequestId() {
    return creationRequestId;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.INDEX_CREATE_ACCEPT_RESPONSE;
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    datasetStructBuilder
        .string("creationRequestId", 10);
    return datasetStructBuilder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    IndexCreateAcceptedResponse acceptedResponse = (IndexCreateAcceptedResponse)message;
    encoder
        .string("creationRequestId", acceptedResponse.getCreationRequestId());

  }

  public static IndexCreateAcceptedResponse decode(DatasetStructDecoder decoder) {
    String creationRequestId = decoder.string("creationRequestId");
    return new IndexCreateAcceptedResponse(creationRequestId);
  }
}
