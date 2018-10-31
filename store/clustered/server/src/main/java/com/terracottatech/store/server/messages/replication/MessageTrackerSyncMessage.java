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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.server.ServerIntrinsicDescriptors;
import com.terracottatech.store.server.messages.DatasetServerMessageCodec;
import com.terracottatech.store.server.messages.ServerServerMessage;
import com.terracottatech.store.server.messages.ServerServerMessageType;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;
import org.terracotta.runnel.encoding.StructEncoderFunction;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MessageTrackerSyncMessage extends PassiveSyncMessage {

  private static final String CLIENT_ID = "clientId";
  private static final String TXN_ID = "transactionId";
  private static final String RESPONSE = "response";
  private static final String TUPLES = "tuples";
  private static final String SEGMENT_IDX = "segmentIdx";

  private static final DatasetServerMessageCodec DATASET_CODEC = new DatasetServerMessageCodec(new IntrinsicCodec(ServerIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS));

  private static final Struct TUPLE_STRUCT = StructBuilder.newStructBuilder()
      .int64(TXN_ID, 10)
      .byteBuffer(RESPONSE, 20)
      .build();

  private final int segmentIndex;
  private final Map<Long, DatasetEntityResponse> trackedResponses;
  private final long clientId;

  public MessageTrackerSyncMessage(long clientId, Map<Long, DatasetEntityResponse> trackedResponses, int segmentIndex) {
    this.clientId = clientId;
    this.trackedResponses = trackedResponses;
    this.segmentIndex = segmentIndex;
  }

  public int getSegmentIndex() {
    return segmentIndex;
  }

  public Map<Long, DatasetEntityResponse> getTrackedResponses() {
    return trackedResponses;
  }

  public long getClientId() {
    return clientId;
  }

  @Override
  public ServerServerMessageType getType() {
    return ServerServerMessageType.MESSAGE_TRACKER_SYNC_MESSAGE;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.getUnderlying().int64(CLIENT_ID, 10)
        .structs(TUPLES, 20, TUPLE_STRUCT)
        .int32(SEGMENT_IDX, 30)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, ServerServerMessage message) {
    MessageTrackerSyncMessage syncMessage = (MessageTrackerSyncMessage) message;
    encoder.getUnderlying().int64(CLIENT_ID, syncMessage.clientId)
        .structs(TUPLES, syncMessage.trackedResponses.entrySet(), new StructEncoderFunction<Map.Entry<Long, DatasetEntityResponse>>() {
          @Override
          public void encode(StructEncoder<?> structEncoder, Map.Entry<Long, DatasetEntityResponse> entry) {
            try {
              ByteBuffer encodedResponse = ByteBuffer.wrap(DATASET_CODEC.encodeResponse(entry.getValue()));
              structEncoder.int64(TXN_ID, entry.getKey()).byteBuffer(RESPONSE, encodedResponse);
            } catch (MessageCodecException e) {
              throw new RuntimeException("Failed to encode " + entry.getValue(), e);
            }
          }
        })
        .int32(SEGMENT_IDX, syncMessage.segmentIndex);
  }

  public static MessageTrackerSyncMessage decode(DatasetStructDecoder decoder) {
    long clientId = decoder.int64(CLIENT_ID);
    Map<Long, DatasetEntityResponse> responses = new HashMap<>();
    StructArrayDecoder<?> structs = decoder.getUnderlying().structs(TUPLES);
    while (structs.hasNext()) {
      StructDecoder<?> tupleDecoder = structs.next();
      Long txnId = tupleDecoder.int64(TXN_ID);
      ByteBuffer byteBuffer = tupleDecoder.byteBuffer(RESPONSE);
      byte[] encodedResponse = new byte[byteBuffer.remaining()];
      byteBuffer.get(encodedResponse);
      try {
        DatasetEntityResponse datasetEntityResponse = DATASET_CODEC.decodeResponse(encodedResponse);
        responses.put(txnId, datasetEntityResponse);
      } catch (MessageCodecException e) {
        throw new RuntimeException("Failed to decode response", e);
      }
    }
    Integer segmentIndex = decoder.int32(SEGMENT_IDX);
    return new MessageTrackerSyncMessage(clientId, responses, segmentIndex);
  }

}
