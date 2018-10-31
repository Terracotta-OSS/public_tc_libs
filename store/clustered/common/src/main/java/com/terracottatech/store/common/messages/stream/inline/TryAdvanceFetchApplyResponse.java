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

import com.terracottatech.store.Record;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorResponse;
import com.terracottatech.store.common.messages.stream.StreamStructures;
import org.terracotta.runnel.Struct;

import java.util.UUID;

/**
 * Message from server-to-client returning the next stream element available from a remote {@code Stream}.
 * The element formed from this message should be supplied to the client-side portion of the stream
 * pipeline.
 *
 * @param <K> the key type of the {@code Record} payload, if any
 *
 * @see TryAdvanceFetchMessage
 * @see TryAdvanceFetchExhaustedResponse
 * @see TryAdvanceReleaseMessage
 */
public class TryAdvanceFetchApplyResponse<K extends Comparable<K>> extends PipelineProcessorResponse {

  private final Element element;

  public TryAdvanceFetchApplyResponse(UUID streamId, Element element) {
    super(streamId);
    this.element = element;
  }

  /**
   * Constructs a new {@code TryAdvanceFetchApplyResponse} holding {@link Record} data.
   * @param streamId the identifier assigned to the remote stream to which is sent
   * @param recordData the {@code RecordData} instance to include
   */
  public TryAdvanceFetchApplyResponse(UUID streamId, RecordData<K> recordData) {
    super(streamId);
    this.element = new Element(ElementType.RECORD, recordData);
  }

  /**
   * Copy constructor intended for subclass construction.
   * @param fetchApplyResponse a {@code TryAdvanceFetchApplyResponse} instance created from
   *        {@link #decode(DatasetStructDecoder)}
   */
  protected TryAdvanceFetchApplyResponse(TryAdvanceFetchApplyResponse<K> fetchApplyResponse) {
    this(fetchApplyResponse.getStreamId(), fetchApplyResponse.getElement());
  }

  public final Element getElement() {
    return element;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.TRY_ADVANCE_FETCH_APPLY_RESPONSE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TryAdvanceFetchApplyResponse{");
    sb.append("streamId=").append(getStreamId());
    sb.append(", elementType=").append(element.getType());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    builder.uuid("streamId", 10);
    return StreamStructures.elementStruct(builder);
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    TryAdvanceFetchApplyResponse<?> fetchResponse = (TryAdvanceFetchApplyResponse<?>) response;
    encoder.uuid("streamId", fetchResponse.getStreamId());
    Element element = fetchResponse.getElement();
    if (element != null) {
      StreamStructures.encodeElement(encoder, element);
    }
  }

  public static TryAdvanceFetchApplyResponse<?> decode(DatasetStructDecoder decoder) {
    UUID streamId = decoder.uuid("streamId");
    Element element = StreamStructures.decodeElement(decoder);
    return new TryAdvanceFetchApplyResponse<>(streamId, element);
  }

}
