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

package com.terracottatech.store.common.messages.stream.batched;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import org.terracotta.runnel.Struct;

import java.util.UUID;

public class BatchFetchMessage extends PipelineProcessorMessage {

  private final int requestedSize;

  public BatchFetchMessage(UUID streamId, int requestedSize) {
    super(streamId);
    this.requestedSize = requestedSize;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("streamId", 10)
            .int32("requestedSize", 20).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    BatchFetchMessage batchFetchMessage = (BatchFetchMessage) message;
    encoder.uuid("streamId", batchFetchMessage.getStreamId())
            .int32("requestedSize", batchFetchMessage.getRequestedSize());
  }

  public static BatchFetchMessage decode(DatasetStructDecoder decoder) {
    return new BatchFetchMessage(decoder.uuid("streamId"), decoder.int32("requestedSize"));
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.BATCH_FETCH_MESSAGE;
  }

  public int getRequestedSize() {
    return requestedSize;
  }
}
