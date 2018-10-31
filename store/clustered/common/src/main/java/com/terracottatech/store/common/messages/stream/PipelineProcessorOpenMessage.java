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

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.Enm;

import java.util.List;
import java.util.UUID;

import static com.terracottatech.store.common.messages.stream.RemoteStreamType.STREAM_TYPE_ENUM_MAPPING;

/**
 * Message requesting the opening of a remote {@code Stream} instance based on the pipeline sequence
 * provided.  The identifier of the remote stream is returned in a {@link PipelineProcessorOpenResponse} message.
 * The remote stream must be closed using a {@link PipelineProcessorCloseMessage}.
 * <p>
 * The {@link PipelineOperation} instances provided <b>must</b> use only
 * {@link com.terracottatech.store.intrinsics.Intrinsic Intrinsic} arguments.
 *
 * @see PipelineProcessorOpenResponse
 * @see PipelineProcessorCloseMessage
 */
public class PipelineProcessorOpenMessage extends PipelineProcessorMessage {

  private final RemoteStreamType type;
  private final ElementType elementType;
  private final List<PipelineOperation> portableOperations;
  private final PipelineOperation terminalOperation;
  private final boolean requiresExplanation;


  /**
   * Create a new {@code PipelineProcessorOpenMessage}.
   *
   * @param streamId the identifier to assign to the newly-opened stream
   * @param elementType the expected element type from this stream
   * @param portableOperations the sequence of {@link PipelineOperation} instances, using only
   *        {@link com.terracottatech.store.intrinsics.Intrinsic Intrinsic} arguments, appended
   *        to the server-side stream created for the pipeline processor
   */
  public PipelineProcessorOpenMessage(UUID streamId, RemoteStreamType type, ElementType elementType, List<PipelineOperation> portableOperations, PipelineOperation terminalOperation, boolean requiresExplanation) {
    super(streamId);
    this.type = type;
    this.elementType = elementType;
    this.portableOperations = portableOperations;
    this.terminalOperation = terminalOperation;
    this.requiresExplanation = requiresExplanation;
  }

  public RemoteStreamType getStreamType() {
    return type;
  }

  public ElementType getElementType() {
    return elementType;
  }

  public List<PipelineOperation> getPortableOperations() {
    return portableOperations;
  }

  public PipelineOperation getTerminalOperation() {
    return terminalOperation;
  }

  public boolean getRequiresExplanation() {
    return requiresExplanation;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.PIPELINE_PROCESSOR_OPEN_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PipelineProcessorOpenMessage{");
    sb.append("streamId=").append(getStreamId());
    sb.append(", elementType=").append(elementType);
    sb.append(", portableOperations=").append(portableOperations);
    sb.append(", terminalOperation=").append(terminalOperation);
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
            .uuid("streamId", 10)
            .enm("streamType", 20, STREAM_TYPE_ENUM_MAPPING)
            .getUnderlying().structs("portableOperations", 30, StreamStructures.pipelineOperationStruct(builder))
            .struct("terminalOperation", 40, StreamStructures.pipelineOperationStruct(builder))
            .enm("elementType", 50, StreamStructures.ELEMENT_TYPE_ENUM_MAPPING)
            .bool("requiresExplanation", 60).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    PipelineProcessorOpenMessage openMessage = (PipelineProcessorOpenMessage) message;
    encoder = encoder.uuid("streamId", openMessage.getStreamId());
    RemoteStreamType streamType = openMessage.getStreamType();
    if (streamType != null) {
      encoder = encoder.enm("streamType", streamType);
    }
    encoder.structs("portableOperations", openMessage.getPortableOperations(), StreamStructures::encodePipelineOperation);

    PipelineOperation terminal  = openMessage.getTerminalOperation();
    if (terminal != null) {
      encoder = encoder.struct("terminalOperation", openMessage.getTerminalOperation(), StreamStructures::encodePipelineOperation);
    }
    encoder.enm("elementType", openMessage.getElementType())
            .bool("requiresExplanation", openMessage.getRequiresExplanation());
  }

  public static PipelineProcessorOpenMessage decode(DatasetStructDecoder decoder) {
    UUID streamId = decoder.uuid("streamId");
    Enm<RemoteStreamType> typeEnm = decoder.<RemoteStreamType>enm("streamType");
    RemoteStreamType type = typeEnm.isFound() ? typeEnm.get() : null;
    List<PipelineOperation> portableOperations = decoder.structs("portableOperations", StreamStructures::decodePipelineOperation);
    PipelineOperation terminalOperation = decoder.struct("terminalOperation", StreamStructures::decodePipelineOperation);
    ElementType elementType = decoder.<ElementType>enm("elementType").get();
    boolean requiresExplanation = decoder.bool("requiresExplanation");
    return new PipelineProcessorOpenMessage(streamId, type, elementType, portableOperations, terminalOperation, requiresExplanation);
  }

}
