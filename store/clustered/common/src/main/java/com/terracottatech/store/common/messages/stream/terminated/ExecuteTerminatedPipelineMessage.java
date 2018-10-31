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

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;

import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.StreamStructures;
import org.terracotta.runnel.Struct;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Message from client-to-server
 */
public class ExecuteTerminatedPipelineMessage extends PipelineProcessorMessage {

  private final List<PipelineOperation> intermediateOperations;
  private final PipelineOperation terminalOperation;
  private final boolean requiresExplanation;
  private final boolean isMutative;

  public ExecuteTerminatedPipelineMessage(UUID streamId, List<PipelineOperation> intermediateOperations,
                                          PipelineOperation terminalOperation, boolean requiresExplanation, boolean isMutative) {
    super(streamId);
    Objects.requireNonNull(intermediateOperations);
    Objects.requireNonNull(terminalOperation);
    this.intermediateOperations = intermediateOperations;
    this.terminalOperation = terminalOperation;
    this.requiresExplanation = requiresExplanation;
    this.isMutative = isMutative;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE;
  }

  public List<PipelineOperation> getIntermediateOperations() {
    return intermediateOperations;
  }

  public PipelineOperation getTerminalOperation() {
    return terminalOperation;
  }

  public boolean getRequiresExplanation() {
    return requiresExplanation;
  }

  public boolean isMutative() {
    return isMutative;
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    return datasetStructBuilder
        .uuid("streamId", 10)
        .getUnderlying()
          .structs("intermediateOperations", 20, StreamStructures.pipelineOperationStruct(datasetStructBuilder))
          .struct("terminalOperation", 30, StreamStructures.pipelineOperationStruct(datasetStructBuilder))
          .bool("requiresExplanation", 40)
          .bool("isMutative", 50)
        .build();
  }

  public static void encode(DatasetStructEncoder datasetStructEncoder, DatasetEntityMessage datasetEntityMessage) {
    ExecuteTerminatedPipelineMessage entityMessage = (ExecuteTerminatedPipelineMessage)datasetEntityMessage;
    datasetStructEncoder
        .uuid("streamId", entityMessage.getStreamId())
        .structs("intermediateOperations", entityMessage.getIntermediateOperations(), StreamStructures::encodePipelineOperation)
        .struct("terminalOperation", entityMessage.getTerminalOperation(), StreamStructures::encodePipelineOperation)
        .bool("requiresExplanation", entityMessage.getRequiresExplanation())
        .bool("isMutative", entityMessage.isMutative());
  }

  public static ExecuteTerminatedPipelineMessage decode(DatasetStructDecoder datasetStructDecoder) {
    UUID streamId = datasetStructDecoder.uuid("streamId");
    List<PipelineOperation> intermediateOperations = datasetStructDecoder.structs("intermediateOperations", StreamStructures::decodePipelineOperation);
    PipelineOperation terminalOperation = datasetStructDecoder.struct("terminalOperation", StreamStructures::decodePipelineOperation);
    boolean requiresExplanation = datasetStructDecoder.bool("requiresExplanation");
    boolean isMutative = datasetStructDecoder.bool("isMutative");
    return new ExecuteTerminatedPipelineMessage(streamId, intermediateOperations, terminalOperation, requiresExplanation, isMutative);
  }
}
