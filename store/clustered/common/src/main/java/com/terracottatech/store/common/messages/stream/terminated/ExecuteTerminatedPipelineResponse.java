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
package com.terracottatech.store.common.messages.stream.terminated;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.stream.ElementValue;
import com.terracottatech.store.common.messages.stream.PipelineProcessorResponse;
import com.terracottatech.store.common.messages.stream.StreamStructures;
import org.terracotta.runnel.Struct;

import java.util.Optional;
import java.util.UUID;

import static com.terracottatech.store.common.messages.stream.StreamStructures.ELEMENT_VALUE_STRUCT;
import static java.util.Optional.ofNullable;

public class ExecuteTerminatedPipelineResponse extends PipelineProcessorResponse {

  private final ElementValue elementValue;
  private final String explanation;

  public ExecuteTerminatedPipelineResponse(UUID streamId, ElementValue elementValue, String explanation) {
    super(streamId);
    this.elementValue = elementValue;
    this.explanation = explanation;
  }

  public ElementValue getElementValue() {
    return elementValue;
  }

  public Optional<String> getExplanation() {
    return ofNullable(explanation);
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.EXECUTE_TERMINATED_PIPELINE_RESPONSE;
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    return datasetStructBuilder
        .uuid("streamId", 10)
        .getUnderlying()
          .struct("elementValue", 20, ELEMENT_VALUE_STRUCT)
          .string("explanation", 30)
        .build();
  }

  public static void encode(DatasetStructEncoder datasetStructEncoder, DatasetEntityResponse datasetEntityResponse) {
    ExecuteTerminatedPipelineResponse entityResponse = (ExecuteTerminatedPipelineResponse)datasetEntityResponse;
    datasetStructEncoder
        .uuid("streamId", entityResponse.getStreamId())
        .getUnderlying()
          .struct("elementValue", entityResponse.getElementValue(), StreamStructures::encodeElementValue)
          .string("explanation", entityResponse.getExplanation().orElse(null));
  }

  public static DatasetEntityResponse decode(DatasetStructDecoder datasetStructDecoder) {
    UUID streamId = datasetStructDecoder.uuid("streamId");
    ElementValue elementValue =
        StreamStructures.decodeElementValue(datasetStructDecoder.getUnderlying().struct("elementValue"));
    String explanation = datasetStructDecoder.getUnderlying().string("explanation");
    return new ExecuteTerminatedPipelineResponse(streamId, elementValue, explanation);
  }
}
