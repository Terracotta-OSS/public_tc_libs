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

package com.terracottatech.store.common.messages.stream.batched;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.PipelineProcessorResponse;
import com.terracottatech.store.common.messages.stream.StreamStructures;
import org.terracotta.runnel.Struct;

import java.util.List;
import java.util.UUID;

public class BatchFetchResponse extends PipelineProcessorResponse {

  private final List<Element> elements;

  public BatchFetchResponse(UUID streamId, List<Element> elements) {
    super(streamId);
    this.elements = elements;
  }

  public final List<Element> getElements() {
    return elements;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.BATCH_FETCH_RESPONSE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("BatchFetchResponse{");
    sb.append("streamId=").append(getStreamId());
    sb.append(", size=").append(elements.size());
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.uuid("streamId", 10).getUnderlying()
            .structs("elements", 20, StreamStructures.elementStruct(builder.newBuilder())).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    BatchFetchResponse fetchResponse = (BatchFetchResponse) response;
    encoder.uuid("streamId", fetchResponse.getStreamId());
    List<Element> elements = fetchResponse.getElements();
    encoder.structs("elements", elements, StreamStructures::encodeElement);
  }

  public static BatchFetchResponse decode(DatasetStructDecoder decoder) {
    UUID streamId = decoder.uuid("streamId");
    List<Element> elements = decoder.structs("elements", StreamStructures::decodeElement);
    return new BatchFetchResponse(streamId, elements);
  }
}
