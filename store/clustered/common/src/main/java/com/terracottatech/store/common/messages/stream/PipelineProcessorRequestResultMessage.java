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
package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import org.terracotta.runnel.Struct;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;

import java.util.UUID;

/**
 * Message from client-to-server requesting the actual response from the previous stream request for which a
 * {@link PipelineRequestProcessingResponse} was received.
 *
 * @see PipelineRequestProcessingResponse
 */
public class PipelineProcessorRequestResultMessage extends PipelineProcessorMessage {
  public PipelineProcessorRequestResultMessage(UUID streamId) {
    super(streamId);
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.PIPELINE_REQUEST_RESULT_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PipelineProcessorRequestResultMessage{");
    sb.append("streamId=").append(getStreamId());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("streamId", 10).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    encoder.uuid("streamId", ((PipelineProcessorRequestResultMessage) message).getStreamId());
  }

  public static PipelineProcessorRequestResultMessage decode(DatasetStructDecoder decoder) {
    return new PipelineProcessorRequestResultMessage(decoder.uuid("streamId"));
  }
}
