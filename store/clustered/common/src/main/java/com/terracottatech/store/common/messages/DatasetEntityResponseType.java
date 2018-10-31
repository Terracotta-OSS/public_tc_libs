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

import com.terracottatech.store.common.messages.crud.AddRecordFullResponse;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.GetRecordResponse;
import com.terracottatech.store.common.messages.crud.NullCRUDReplicationResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.event.ChangeEventResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateAcceptedResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusResponse;
import com.terracottatech.store.common.messages.indexing.IndexListResponse;
import com.terracottatech.store.common.messages.indexing.IndexStatusResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenResponse;
import com.terracottatech.store.common.messages.stream.PipelineRequestProcessingResponse;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchResponse;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchApplyResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchConsumedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchExhaustedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchWaypointResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseResponse;
import org.terracotta.runnel.Struct;

import java.util.function.BiConsumer;
import java.util.function.Function;

public enum DatasetEntityResponseType implements MessageComponent<DatasetEntityResponse> {
  ERROR_RESPONSE(ErrorResponse::struct, ErrorResponse::encode, ErrorResponse::decode),
  ADD_RECORD_FULL_RESPONSE(AddRecordFullResponse::struct, AddRecordFullResponse::encode, AddRecordFullResponse::decode),
  ADD_RECORD_SIMPLIFIED_RESPONSE(AddRecordSimplifiedResponse::struct, AddRecordSimplifiedResponse::encode, AddRecordSimplifiedResponse::decode),
  GET_RECORD_RESPONSE(GetRecordResponse::struct, GetRecordResponse::encode, GetRecordResponse::decode),
  CHANGE_EVENT_RESPONSE(ChangeEventResponse::struct, ChangeEventResponse::encode, ChangeEventResponse::decode),
  PREDICATED_UPDATE_RECORD_FULL_RESPONSE(PredicatedUpdateRecordFullResponse::struct, PredicatedUpdateRecordFullResponse::encode, PredicatedUpdateRecordFullResponse::decode),
  PREDICATED_UPDATE_RECORD_SIMPLIFIED_RESPONSE(PredicatedUpdateRecordSimplifiedResponse::struct, PredicatedUpdateRecordSimplifiedResponse::encode, PredicatedUpdateRecordSimplifiedResponse::decode),
  PREDICATED_DELETE_RECORD_FULL_RESPONSE(PredicatedDeleteRecordFullResponse::struct, PredicatedDeleteRecordFullResponse::encode, PredicatedDeleteRecordFullResponse::decode),
  PREDICATED_DELETE_RECORD_SIMPLIFIED_RESPONSE(PredicatedDeleteRecordSimplifiedResponse::struct, PredicatedDeleteRecordSimplifiedResponse::encode, PredicatedDeleteRecordSimplifiedResponse::decode),
  PIPELINE_PROCESSOR_OPEN_RESPONSE(PipelineProcessorOpenResponse::struct, PipelineProcessorOpenResponse::encode, PipelineProcessorOpenResponse::decode),
  PIPELINE_PROCESSOR_CLOSE_RESPONSE(PipelineProcessorCloseResponse::struct, PipelineProcessorCloseResponse::encode, PipelineProcessorCloseResponse::decode),
  TRY_ADVANCE_FETCH_APPLY_RESPONSE(TryAdvanceFetchApplyResponse::struct, TryAdvanceFetchApplyResponse::encode, TryAdvanceFetchApplyResponse::decode),
  TRY_ADVANCE_FETCH_CONSUMED_RESPONSE(TryAdvanceFetchConsumedResponse::struct, TryAdvanceFetchConsumedResponse::encode, TryAdvanceFetchConsumedResponse::decode),
  TRY_ADVANCE_FETCH_EXHAUSTED_RESPONSE(TryAdvanceFetchExhaustedResponse::struct, TryAdvanceFetchExhaustedResponse::encode, TryAdvanceFetchExhaustedResponse::decode),
  TRY_ADVANCE_FETCH_WAYPOINT_RESPONSE(TryAdvanceFetchWaypointResponse::struct, TryAdvanceFetchWaypointResponse::encode, TryAdvanceFetchWaypointResponse::decode),
  PIPELINE_REQUEST_PROCESSING_RESPONSE(PipelineRequestProcessingResponse::struct, PipelineRequestProcessingResponse::encode, PipelineRequestProcessingResponse::decode),
  TRY_ADVANCE_RELEASE_RESPONSE(TryAdvanceReleaseResponse::struct, TryAdvanceReleaseResponse::encode, TryAdvanceReleaseResponse::decode),
  BATCH_FETCH_RESPONSE(BatchFetchResponse::struct, BatchFetchResponse::encode, BatchFetchResponse::decode),
  SUCCESS_RESPONSE(SuccessResponse::struct, SuccessResponse::encode, SuccessResponse::decode),
  EXECUTE_TERMINATED_PIPELINE_RESPONSE(ExecuteTerminatedPipelineResponse::struct, ExecuteTerminatedPipelineResponse::encode, ExecuteTerminatedPipelineResponse::decode),
  INDEX_LIST_RESPONSE(IndexListResponse::struct, IndexListResponse::encode, IndexListResponse::decode),
  INDEX_CREATE_ACCEPT_RESPONSE(IndexCreateAcceptedResponse::struct, IndexCreateAcceptedResponse::encode, IndexCreateAcceptedResponse::decode),
  INDEX_CREATE_STATUS_RESPONSE(IndexCreateStatusResponse::struct, IndexCreateStatusResponse::encode, IndexCreateStatusResponse::decode),
  INDEX_STATUS_RESPONSE(IndexStatusResponse::struct, IndexStatusResponse::encode, IndexStatusResponse::decode),
  BUSY_RESPONSE(BusyResponse::struct, BusyResponse::encode, BusyResponse::decode),
  NULL_CRUD_REPLICATION_RESPONSE(NullCRUDReplicationResponse::struct, NullCRUDReplicationResponse::encode, NullCRUDReplicationResponse::decode),
  ;

  private final Function<DatasetStructBuilder, Struct> structGenerator;
  private final BiConsumer<DatasetStructEncoder, DatasetEntityResponse> encodingFunction;
  private final Function<DatasetStructDecoder, ? extends DatasetEntityResponse> decodingFunction;

  DatasetEntityResponseType(
          Function<DatasetStructBuilder, Struct> structGenerator,
          BiConsumer<DatasetStructEncoder,  DatasetEntityResponse> encodingFunction,
          Function<DatasetStructDecoder, ? extends DatasetEntityResponse> decodingFunction) {
    this.structGenerator = structGenerator;
    this.encodingFunction = encodingFunction;
    this.decodingFunction = decodingFunction;
  }

  @Override
  public Struct struct(DatasetStructBuilder builder) {
    return structGenerator.apply(builder);
  }

  @Override
  public void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    encodingFunction.accept(encoder, message);
  }

  @Override
  public DatasetEntityResponse decode(DatasetStructDecoder decoder) {
    return decodingFunction.apply(decoder);
  }
}
