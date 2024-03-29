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

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenResponse;
import org.terracotta.runnel.Struct;

import java.util.UUID;

/**
 * Message from client-to-server requesting to advance the remote {@code Spliterator}
 * to the next element and apply the remoted portion of a {@code Stream} pipeline against
 * the element.
 *
 * @see TryAdvanceFetchApplyResponse
 * @see TryAdvanceFetchExhaustedResponse
 * @see TryAdvanceReleaseMessage
 */
public class TryAdvanceFetchMessage extends PipelineProcessorMessage {

  /**
   * Creates a new {@code TryAdvanceFetchMessage}.
   *
   * @param streamId the identifier of the remote stream to advance; obtained from
   *                 {@link PipelineProcessorOpenResponse#getStreamId()}
   */
  public TryAdvanceFetchMessage(UUID streamId) {
    super(streamId);
  }

  /**
   * Copy constructor intended for subclass construction.
   * @param fetchMessage a {@code TryAdvanceFetchMessage} instance created from {@link #decode(DatasetStructDecoder)}
   */
  protected TryAdvanceFetchMessage(TryAdvanceFetchMessage fetchMessage) {
    this(fetchMessage.getStreamId());
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.TRY_ADVANCE_FETCH_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TryAdvanceFetchMessage{");
    sb.append("streamId=").append(getStreamId());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("streamId", 10).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    encoder.uuid("streamId", ((TryAdvanceFetchMessage) message).getStreamId());
  }

  public static TryAdvanceFetchMessage decode(DatasetStructDecoder decoder) {
    return new TryAdvanceFetchMessage(decoder.uuid("streamId"));
  }
}
