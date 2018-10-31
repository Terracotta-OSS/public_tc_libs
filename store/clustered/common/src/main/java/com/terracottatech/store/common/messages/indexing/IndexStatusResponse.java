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
import org.terracotta.runnel.decoding.Enm;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.indexing.Index;

/**
 * The response to a {@link IndexStatusMessage}.
 */
public class IndexStatusResponse extends DatasetEntityResponse {

  private final Index.Status status;

  public IndexStatusResponse(Index.Status status) {
    this.status = status;
  }

  public Index.Status getStatus() {
    return status;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.INDEX_STATUS_RESPONSE;
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    datasetStructBuilder
        .enm("status", 10, IndexingCodec.INDEX_STATUS_ENUM_MAPPING);
    return datasetStructBuilder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    IndexStatusResponse statusResponse = (IndexStatusResponse)message;
    encoder.enm("status", statusResponse.getStatus());
  }

  public static IndexStatusResponse decode(DatasetStructDecoder decoder) {
    Enm<Index.Status> statusEnm = decoder.enm("status");
    return new IndexStatusResponse(statusEnm.get());
  }
}
