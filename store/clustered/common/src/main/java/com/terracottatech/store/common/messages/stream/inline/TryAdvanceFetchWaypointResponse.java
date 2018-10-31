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

import org.terracotta.runnel.Struct;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.RecordData;

import java.util.UUID;

/**
 * Message from server-to-client returning the next stream element available via a {@link WaypointMarker}
 * in a remote {@code Stream}.  This is used when a pipeline contains a non-portable mutative operation.
 * Since more server-side pipeline remains to be perform, this message is used in lieu of a
 * {@link TryAdvanceFetchApplyResponse} message.
 *
 * @param <K> the key type of the {@code Record} payload
 *
 * @see TryAdvanceFetchMessage
 * @see TryAdvanceFetchExhaustedResponse
 * @see TryAdvanceReleaseMessage
 */
public class TryAdvanceFetchWaypointResponse<K extends Comparable<K>> extends TryAdvanceFetchApplyResponse<K> {

  private final int waypointId;

  public TryAdvanceFetchWaypointResponse(UUID streamId, RecordData<K> recordData, int waypointId) {
    super(streamId, recordData);
    this.waypointId = waypointId;
  }

  private TryAdvanceFetchWaypointResponse(TryAdvanceFetchApplyResponse<K> fetchApplyResponse, int waypointId) {
    super(fetchApplyResponse);
    this.waypointId = waypointId;
  }

  public final int getWaypointId() {
    return waypointId;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.TRY_ADVANCE_FETCH_WAYPOINT_RESPONSE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TryAdvanceFetchWaypointResponse{");
    sb.append("streamId=").append(getStreamId());
    sb.append(", waypointId=").append(waypointId);
    sb.append(", elementType=").append(getElement().getType());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    builder.getUnderlying()
        .struct("fetchApply", 10, TryAdvanceFetchApplyResponse.struct(builder.newBuilder()));
    builder.int32("waypointId", 20);
    return builder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    TryAdvanceFetchWaypointResponse<?> fetchResponse = (TryAdvanceFetchWaypointResponse<?>)response;
    encoder.struct("fetchApply", fetchResponse, TryAdvanceFetchApplyResponse::encode);
    encoder.int32("waypointId", fetchResponse.waypointId);
  }

  public static TryAdvanceFetchWaypointResponse<?> decode(DatasetStructDecoder decoder) {
    TryAdvanceFetchApplyResponse<?> fetchApplyResponse =
        decoder.struct("fetchApply", TryAdvanceFetchApplyResponse::decode);
    int waypointId = decoder.int32("waypointId");
    return new TryAdvanceFetchWaypointResponse<>(fetchApplyResponse, waypointId);
  }

}
