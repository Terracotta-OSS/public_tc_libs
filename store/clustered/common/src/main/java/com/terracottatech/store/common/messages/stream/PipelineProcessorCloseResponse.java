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
import org.terracotta.runnel.Struct;

import java.util.Optional;
import java.util.UUID;

import static java.util.Optional.ofNullable;

/**
 * Message indicating the successful closure of the remote {@code Stream} requested by a
 * paired {@link PipelineProcessorCloseMessage}.
 *
 * @see PipelineProcessorCloseMessage
 * @see PipelineProcessorOpenMessage
 */
public class PipelineProcessorCloseResponse extends PipelineProcessorResponse {

  private final String explanation;

  public PipelineProcessorCloseResponse(UUID streamId, String explanation) {
    super(streamId);
    this.explanation = explanation;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.PIPELINE_PROCESSOR_CLOSE_RESPONSE;
  }

  public Optional<String> getExplanation() {
    return ofNullable(explanation);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PipelineProcessorCloseResponse{");
    sb.append("streamId=").append(getStreamId());
    getExplanation().ifPresent(e -> sb.append("\nexplanation=").append(e));
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("streamId", 10).getUnderlying().string("explanation", 20).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    encoder.uuid("streamId", ((PipelineProcessorCloseResponse) message).getStreamId());
    encoder.getUnderlying().string("explanation", ((PipelineProcessorCloseResponse) message).getExplanation().orElse(null));
  }

  public static DatasetEntityResponse decode(DatasetStructDecoder decoder) {
    return new PipelineProcessorCloseResponse(decoder.uuid("streamId"), decoder.getUnderlying().string("explanation"));
  }
}
