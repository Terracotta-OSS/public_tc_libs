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

import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenResponse;
import org.terracotta.runnel.Struct;

import com.terracottatech.store.Cell;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;

import java.util.UUID;

/**
 * Message from client-to-server requesting to advance the remote {@code Spliterator}
 * to the next element and apply the remoted portion of a {@code Stream} pipeline against
 * the element.
 *
 * @see TryAdvanceFetchMessage
 * @see TryAdvanceFetchWaypointResponse
 * @see TryAdvanceFetchApplyResponse
 * @see TryAdvanceFetchExhaustedResponse
 * @see TryAdvanceReleaseMessage
 */
public class TryAdvanceMutateMessage extends TryAdvanceFetchMessage {

  /**
   * The waypoint identifier for this message.
   */
  private final int waypointId;

  /**
   * The mutation cell collection; may be {@code null} iff the mutation transform is portable but the
   * mutable pipeline operation is preceded by a non-portable operation.
   */
  private final Iterable<Cell<?>> cells;

  /**
   * Creates a new {@code TryAdvanceMutateMessage}.
   *  @param streamId the identifier of the remote stream to advance; obtained from
   *                 {@link PipelineProcessorOpenResponse#getStreamId()}
   * @param waypointId the waypoint identifier of the mutation insertion point in the remote pipeline
   * @param cells the collection of {@link Cell} instances making up the updated {@code Record} image;
   *              may be {@code null} to indicate the collection is to be computed using a portable
   *              transformation already present in the pipeline
   */
  public TryAdvanceMutateMessage(UUID streamId, int waypointId, Iterable<Cell<?>> cells) {
    super(streamId);
    this.waypointId = waypointId;
    this.cells = cells;
  }

  private TryAdvanceMutateMessage(TryAdvanceFetchMessage fetchMessage, int waypointId, Iterable<Cell<?>> cells) {
    super(fetchMessage);
    this.waypointId = waypointId;
    this.cells = cells;
  }

  public final int getWaypointId() {
    return waypointId;
  }

  public final Iterable<Cell<?>> getCells() {
    return cells;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.TRY_ADVANCE_MUTATE_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TryAdvanceMutateMessage{");
    sb.append("streamId=").append(getStreamId());
    sb.append(", waypointId=").append(waypointId);
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    builder.getUnderlying()
        .struct("fetch", 10, TryAdvanceFetchMessage.struct(builder.newBuilder()));
    builder.int32("waypointId", 20);
    builder.cells("cells", 30);
    return builder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    TryAdvanceMutateMessage mutateMessage = (TryAdvanceMutateMessage)message;
    encoder.struct("fetch", mutateMessage, TryAdvanceFetchMessage::encode);
    encoder.int32("waypointId", mutateMessage.waypointId);
    if (mutateMessage.cells != null) {
      encoder.cells("cells", mutateMessage.cells);
    }
  }

  public static TryAdvanceMutateMessage decode(DatasetStructDecoder decoder) {
    TryAdvanceFetchMessage fetchApplyResponse = decoder.struct("fetch", TryAdvanceFetchMessage::decode);
    int waypointId = decoder.int32("waypointId");
    Iterable<Cell<?>> cells = decoder.cells("cells");
    return new TryAdvanceMutateMessage(fetchApplyResponse, waypointId, cells);
  }
}
