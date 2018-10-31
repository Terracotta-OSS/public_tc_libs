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

import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.server.messages.ServerServerMessage;
import com.terracottatech.store.server.messages.ServerServerMessageType;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;
import org.terracotta.runnel.encoding.StructEncoderFunction;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class BatchedDataSyncMessage extends PassiveSyncMessage {

  private static final Struct TUPLE_STRUCT = StructBuilder.newStructBuilder()
          .int64("index", 10)
          .byteBuffer("data", 20)
          .build();

  private final List<BufferDataTuple> dataTuples;
  private final int shardIdx;

  public BatchedDataSyncMessage(List<BufferDataTuple> dataTuples, int shardIdx) {
    this.dataTuples = dataTuples;
    this.shardIdx = shardIdx;
  }

  public List<BufferDataTuple> getDataTuples() {
    return dataTuples;
  }

  public int getShardIdx() {
    return shardIdx;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.getUnderlying().int32("shardIdx", 10)
            .structs("tuples", 20, TUPLE_STRUCT)
            .build();
  }

  public static void encode(DatasetStructEncoder encoder, ServerServerMessage message) {
    BatchedDataSyncMessage batchedDataSyncMessage = (BatchedDataSyncMessage) message;
    encoder.getUnderlying().int32("shardIdx", batchedDataSyncMessage.getShardIdx())
            .structs("tuples", batchedDataSyncMessage.dataTuples, new StructEncoderFunction<BufferDataTuple>() {
              @Override
              public void encode(StructEncoder<?> structEncoder, BufferDataTuple dataTuple) {
                structEncoder = structEncoder.int64("index", dataTuple.index());
                if (dataTuple.getData() != null) {
                  structEncoder.byteBuffer("data", dataTuple.getData());
                }
              }
            });
  }

  public static BatchedDataSyncMessage decode(DatasetStructDecoder decoder) {
    int shardIdx = decoder.int32("shardIdx");
    StructArrayDecoder<?> tuples = decoder.getUnderlying().structs("tuples");
    List<BufferDataTuple> dataTuples = new LinkedList<>();
    while (tuples.hasNext()) {
      StructDecoder<?> next = tuples.next();
      final Long index = next.int64("index");
      final ByteBuffer byteBuffer = next.byteBuffer("data");
      dataTuples.add(new BufferDataTuple() {
        @Override
        public long index() {
          return index;
        }

        @Override
        public ByteBuffer getData() {
          return byteBuffer;
        }
      });
    }
    return new BatchedDataSyncMessage(dataTuples, shardIdx);
  }

  @Override
  public ServerServerMessageType getType() {
    return ServerServerMessageType.BATCHED_SYNC_MESSAGE;
  }
}
