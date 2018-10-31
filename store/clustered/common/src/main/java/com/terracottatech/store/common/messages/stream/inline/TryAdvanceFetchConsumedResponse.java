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

import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorResponse;
import org.terracotta.runnel.Struct;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * Message from server-to-client indicating processing is complete for the next stream element in
 * from a remote {@code Stream}.  This message is used in partially portable mutative pipelines
 * where the pipeline is terminated by a mutative {@link Consumer}.
 */
public class TryAdvanceFetchConsumedResponse extends PipelineProcessorResponse {
  /**
   * Creates a new {@code TryAdvanceFetchConsumedResponse}.
   *
   * @param streamId the remote stream identifier generally obtained from the paired {@link PipelineProcessorMessage}
   */
  public TryAdvanceFetchConsumedResponse(UUID streamId) {
    super(streamId);
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.TRY_ADVANCE_FETCH_CONSUMED_RESPONSE;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    builder.uuid("streamId", 10);
    return builder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    TryAdvanceFetchConsumedResponse fetchResponse = (TryAdvanceFetchConsumedResponse) response;
    encoder.uuid("streamId", fetchResponse.getStreamId());
  }

  public static TryAdvanceFetchConsumedResponse decode(DatasetStructDecoder decoder) {
    UUID streamId = decoder.uuid("streamId");
    return new TryAdvanceFetchConsumedResponse(streamId);
  }
}
