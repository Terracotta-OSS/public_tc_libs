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

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import org.terracotta.runnel.Struct;

import java.util.UUID;

/**
 * Message requesting the closure of a remote {@code Stream}.
 */
public class PipelineProcessorCloseMessage extends PipelineProcessorMessage {

  /**
   * Creates a new {@code PipelineProcessorCloseMessage}.
   *
   * @param streamId the identifier of the remote stream to close; obtained from
   *                 {@link PipelineProcessorOpenResponse#getStreamId()}
   */
  public PipelineProcessorCloseMessage(UUID streamId) {
    super(streamId);
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.PIPELINE_PROCESSOR_CLOSE_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PipelineProcessorCloseMessage{");
    sb.append("streamId=").append(getStreamId());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("streamId", 10).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    PipelineProcessorCloseMessage closeMessage = (PipelineProcessorCloseMessage) message;
    encoder.uuid("streamId", closeMessage.getStreamId());
  }

  public static PipelineProcessorCloseMessage decode(DatasetStructDecoder decoder) {
    return new PipelineProcessorCloseMessage(decoder.uuid("streamId"));
  }
}
