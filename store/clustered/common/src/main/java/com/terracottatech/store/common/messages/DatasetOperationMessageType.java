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
package com.terracottatech.store.common.messages;

import com.terracottatech.store.common.messages.crud.AddRecordMessage;
import com.terracottatech.store.common.messages.crud.GetRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordMessage;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexDestroyMessage;
import com.terracottatech.store.common.messages.indexing.IndexListMessage;
import com.terracottatech.store.common.messages.indexing.IndexStatusMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorRequestResultMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceMutateMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseMessage;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import org.terracotta.runnel.Struct;

import java.util.function.BiConsumer;
import java.util.function.Function;

public enum DatasetOperationMessageType implements MessageComponent<DatasetOperationMessage> {
  IDENTIFY_CLIENT_MESSAGE(IdentifyClientMessage::struct, IdentifyClientMessage::encode, IdentifyClientMessage::decode),
  ADD_RECORD_MESSAGE(AddRecordMessage::struct, AddRecordMessage::encode, AddRecordMessage::decode),
  GET_RECORD_MESSAGE(GetRecordMessage::struct, GetRecordMessage::encode, GetRecordMessage::decode),
  PREDICATED_UPDATE_RECORD_MESSAGE(PredicatedUpdateRecordMessage::struct, PredicatedUpdateRecordMessage::encode, PredicatedUpdateRecordMessage::decode),
  PREDICATED_DELETE_RECORD_MESSAGE(PredicatedDeleteRecordMessage::struct, PredicatedDeleteRecordMessage::encode, PredicatedDeleteRecordMessage::decode),
  PIPELINE_PROCESSOR_OPEN_MESSAGE(PipelineProcessorOpenMessage::struct, PipelineProcessorOpenMessage::encode, PipelineProcessorOpenMessage::decode),
  PIPELINE_PROCESSOR_CLOSE_MESSAGE(PipelineProcessorCloseMessage::struct, PipelineProcessorCloseMessage::encode, PipelineProcessorCloseMessage::decode),
  TRY_ADVANCE_FETCH_MESSAGE(TryAdvanceFetchMessage::struct, TryAdvanceFetchMessage::encode, TryAdvanceFetchMessage::decode),
  PIPELINE_REQUEST_RESULT_MESSAGE(PipelineProcessorRequestResultMessage::struct, PipelineProcessorRequestResultMessage::encode, PipelineProcessorRequestResultMessage::decode),
  TRY_ADVANCE_MUTATE_MESSAGE(TryAdvanceMutateMessage::struct, TryAdvanceMutateMessage::encode, TryAdvanceMutateMessage::decode),
  TRY_ADVANCE_RELEASE_MESSAGE(TryAdvanceReleaseMessage::struct, TryAdvanceReleaseMessage::encode, TryAdvanceReleaseMessage::decode),
  BATCH_FETCH_MESSAGE(BatchFetchMessage::struct, BatchFetchMessage::encode, BatchFetchMessage::decode),
  SEND_CHANGE_EVENTS_MESSAGE(SendChangeEventsMessage::struct, SendChangeEventsMessage::encode, SendChangeEventsMessage::decode),
  EXECUTE_TERMINATED_PIPELINE_MESSAGE(ExecuteTerminatedPipelineMessage::struct, ExecuteTerminatedPipelineMessage::encode, ExecuteTerminatedPipelineMessage::decode),
  UNIVERSAL_NOP_MESSAGE(UniversalNopMessage::struct, UniversalNopMessage::encode, UniversalNopMessage::decode),
  INDEX_LIST_MESSAGE(IndexListMessage::struct, IndexListMessage::encode, IndexListMessage::decode),
  INDEX_CREATE_MESSAGE(IndexCreateMessage::struct, IndexCreateMessage::encode, IndexCreateMessage::decode),
  INDEX_CREATE_STATUS_MESSAGE(IndexCreateStatusMessage::struct, IndexCreateStatusMessage::encode, IndexCreateStatusMessage::decode),
  INDEX_DESTROY_MESSAGE(IndexDestroyMessage::struct, IndexDestroyMessage::encode, IndexDestroyMessage::decode),
  INDEX_STATUS_MESSAGE(IndexStatusMessage::struct, IndexStatusMessage::encode, IndexStatusMessage::decode),
  ;

  private final Function<DatasetStructBuilder, Struct> structGenerator;
  private final BiConsumer<DatasetStructEncoder, DatasetOperationMessage> encodingFunction;
  private final Function<DatasetStructDecoder, ? extends DatasetOperationMessage> decodingFunction;

  DatasetOperationMessageType(
          Function<DatasetStructBuilder, Struct> structGenerator,
          BiConsumer<DatasetStructEncoder, DatasetOperationMessage> encodingFunction,
          Function<DatasetStructDecoder, ? extends DatasetOperationMessage> decodingFunction) {
    this.structGenerator = structGenerator;
    this.encodingFunction = encodingFunction;
    this.decodingFunction = decodingFunction;
  }

  @Override
  public Struct struct(DatasetStructBuilder builder) {
    return structGenerator.apply(builder);
  }

  @Override
  public void encode(DatasetStructEncoder encoder, DatasetOperationMessage message) {
    encodingFunction.accept(encoder, message);
  }

  @Override
  public DatasetOperationMessage decode(DatasetStructDecoder decoder) {
    return decodingFunction.apply(decoder);
  }
}
