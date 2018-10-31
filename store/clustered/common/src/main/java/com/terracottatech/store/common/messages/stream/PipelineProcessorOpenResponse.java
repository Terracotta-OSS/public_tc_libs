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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchMessage;
import org.terracotta.runnel.Struct;

import java.util.UUID;

import static com.terracottatech.store.common.messages.stream.RemoteStreamType.STREAM_TYPE_ENUM_MAPPING;

/**
 * The affirmative response for {@link PipelineProcessorOpenMessage}.  The remote stream identifier
 * supplied by this message is used in {@link TryAdvanceFetchMessage TryAdvance...} messages and
 * {@link PipelineProcessorCloseMessage}.
 *
 * @see PipelineProcessorOpenMessage
 */
public class PipelineProcessorOpenResponse extends PipelineProcessorResponse {

  private final RemoteStreamType sourceType;

  public PipelineProcessorOpenResponse(UUID streamId, RemoteStreamType sourceType) {
    super(streamId);
    this.sourceType = sourceType;
  }

  public RemoteStreamType getSourceType() {
    return sourceType;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.PIPELINE_PROCESSOR_OPEN_RESPONSE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PipelineProcessorOpenResponse{");
    sb.append("streamId=").append(getStreamId());
    sb.append(", sourceType=").append(getSourceType());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
            .uuid("streamId", 10)
            .enm("sourceType", 20, STREAM_TYPE_ENUM_MAPPING)
            .build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    encoder.uuid("streamId", ((PipelineProcessorOpenResponse) message).getStreamId())
            .enm("sourceType", ((PipelineProcessorOpenResponse) message).getSourceType());
  }

  public static PipelineProcessorOpenResponse decode(DatasetStructDecoder decoder) {
    return new PipelineProcessorOpenResponse(decoder.uuid("streamId"), decoder.<RemoteStreamType>enm("sourceType").get());
  }
}
