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
import org.terracotta.runnel.Struct;

import java.nio.ByteBuffer;

public class DataReplicationMessage extends ReplicationMessage {

  protected static final String INDEX = "index";
  protected static final String DATA = "data";

  private final long index;
  private final ByteBuffer data;

  public DataReplicationMessage(long index, ByteBuffer data) {
    this.index = index;
    this.data = data;
  }

  public long getIndex() {
    return this.index;
  }

  public ByteBuffer getData() {
    return this.data;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.int64(INDEX, 170)
        .byteBuffer(DATA, 180)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, ReplicationMessage message) {
    DataReplicationMessage dataReplicationMessage = (DataReplicationMessage) message;
    encoder.int64(INDEX, dataReplicationMessage.getIndex());
    if (dataReplicationMessage.getData() != null) {
      encoder.byteBuffer(DATA, dataReplicationMessage.getData());
    }
  }

  public static DataReplicationMessage decode(DatasetStructDecoder decoder) {
    return new DataReplicationMessage(decoder.int64(INDEX), decoder.byteBuffer(DATA));
  }

  @Override
  public ReplicationMessageType getType() {
    return ReplicationMessageType.DATA_REPLICATION_MESSAGE;
  }

}
