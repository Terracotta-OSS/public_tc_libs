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

package com.terracottatech.store.common.messages.stream.inline;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.stream.PipelineProcessorResponse;
import org.terracotta.runnel.Struct;

import java.util.UUID;

/**
 * Message from server-to-client indicating the remote {@code Stream} is exhausted.
 *
 * @see TryAdvanceFetchMessage
 */
public class TryAdvanceFetchExhaustedResponse extends PipelineProcessorResponse {

  public TryAdvanceFetchExhaustedResponse(UUID streamId) {
    super(streamId);
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.TRY_ADVANCE_FETCH_EXHAUSTED_RESPONSE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TryAdvanceFetchExhaustedResponse{");
    sb.append("streamId=").append(getStreamId());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("streamId", 10).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    encoder.uuid("streamId", ((TryAdvanceFetchExhaustedResponse) message).getStreamId());
  }

  public static DatasetEntityResponse decode(DatasetStructDecoder decoder) {
    return new TryAdvanceFetchExhaustedResponse(decoder.uuid("streamId"));
  }
}
