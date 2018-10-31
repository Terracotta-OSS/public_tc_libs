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

public class CRUDDataReplicationMessage extends DataReplicationMessage {
  private static final String RESPOND_IN_FULL = "respondInFull";
  private static final String CLIENT_ID = "clientId";
  private static final String CURRENT_TXN_ID = "currentTransactionId";
  private static final String OLDEST_TXN_ID = "oldestTransactionId";

  private final boolean respondInFull;
  private final long clientId;
  private final long currentTransactionId;
  private final long oldestTransactionId;

  public CRUDDataReplicationMessage(long index, ByteBuffer data, boolean respondInFull, long clientId,
                                    long currentTransactionId, long oldestTransactionId) {
    super(index, data);
    this.respondInFull = respondInFull;
    this.clientId = clientId;
    this.currentTransactionId = currentTransactionId;
    this.oldestTransactionId = oldestTransactionId;
  }

  public boolean isRespondInFull() {
    return respondInFull;
  }

  public long getClientId() {
    return clientId;
  }

  public long getCurrentTransactionId() {
    return currentTransactionId;
  }

  public long getOldestTransactionId() {
    return oldestTransactionId;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
        .int64(INDEX, 170)
        .byteBuffer(DATA, 180)
        .bool(RESPOND_IN_FULL, 190)
        .int64(CLIENT_ID, 200)
        .int64(CURRENT_TXN_ID, 210)
        .int64(OLDEST_TXN_ID, 220)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, ReplicationMessage message) {
    CRUDDataReplicationMessage dataReplicationMessage = (CRUDDataReplicationMessage) message;
    encoder = encoder.int64(INDEX, dataReplicationMessage.getIndex());

    if (dataReplicationMessage.getData() != null) {
      encoder.byteBuffer(DATA, dataReplicationMessage.getData());
    }

    encoder
        .bool(RESPOND_IN_FULL, dataReplicationMessage.isRespondInFull())
        .int64(CLIENT_ID, dataReplicationMessage.getClientId())
        .int64(CURRENT_TXN_ID, dataReplicationMessage.getCurrentTransactionId())
        .int64(OLDEST_TXN_ID, dataReplicationMessage.getOldestTransactionId());
  }

  public static CRUDDataReplicationMessage decode(DatasetStructDecoder decoder) {
    Long index = decoder.int64(INDEX);
    ByteBuffer data = decoder.byteBuffer(DATA);
    Boolean respondInFull = decoder.bool(RESPOND_IN_FULL);
    Long clientId = decoder.int64(CLIENT_ID);
    Long currentTransactionId = decoder.int64(CURRENT_TXN_ID);
    Long oldestTransactionId = decoder.int64(OLDEST_TXN_ID);

    return new CRUDDataReplicationMessage(index, data, respondInFull, clientId, currentTransactionId, oldestTransactionId);
  }

  @Override
  public ReplicationMessageType getType() {
    return ReplicationMessageType.CRUD_DATA_REPLICATION_MESSAGE;
  }
}
