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
package com.terracottatech.store.common.messages.crud;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import org.terracotta.runnel.Struct;

public class NullCRUDReplicationResponse<K extends Comparable<K>> extends DatasetEntityResponse {

  private final long currentMsgId;
  private final long index;

  public NullCRUDReplicationResponse(long index, long currentMsgId) {
    this.currentMsgId = currentMsgId;
    this.index = index;
  }

  public long getCurrentMsgId() {
    return currentMsgId;
  }

  public long getIndex() {
    return index;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.NULL_CRUD_REPLICATION_RESPONSE;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
            .int64("currentMsgId", 10)
            .int64("index", 20)
            .build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    encoder
            .int64("currentMsgId", ((NullCRUDReplicationResponse) response).getCurrentMsgId())
            .int64("index", ((NullCRUDReplicationResponse) response).getIndex());
  }

  public static NullCRUDReplicationResponse<?> decode(DatasetStructDecoder decoder) {
    Long currentMsgId = decoder.int64("currentMsgId");
    Long index = decoder.int64("index");
    return new NullCRUDReplicationResponse<>(index, currentMsgId);
  }
}
