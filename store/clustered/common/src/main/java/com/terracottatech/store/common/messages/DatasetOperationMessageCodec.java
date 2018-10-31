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

import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DatasetOperationMessageCodec extends AbstractMessageCodec {

  /*
   * The members of this map must be coordinated with DatasetServerMessageCodec.REPLICATION_MESSAGE_COMPONENTS.
   * The members of this map and the members of REPLICATION_MESSAGE_COMPONENTS are merged in
   * DatasetServerMessageCodec -- the assigned numbers are not allowed to collide.
   */
  private static final Map<Integer, DatasetOperationMessageType> MESSAGE_COMPONENTS = new HashMap<>();
  static {
    MESSAGE_COMPONENTS.put(0, DatasetOperationMessageType.ADD_RECORD_MESSAGE);
    MESSAGE_COMPONENTS.put(1, DatasetOperationMessageType.GET_RECORD_MESSAGE);
    MESSAGE_COMPONENTS.put(2, DatasetOperationMessageType.PREDICATED_UPDATE_RECORD_MESSAGE);
    MESSAGE_COMPONENTS.put(3, DatasetOperationMessageType.PREDICATED_DELETE_RECORD_MESSAGE);
    MESSAGE_COMPONENTS.put(4, DatasetOperationMessageType.PIPELINE_PROCESSOR_OPEN_MESSAGE);
    MESSAGE_COMPONENTS.put(5, DatasetOperationMessageType.PIPELINE_PROCESSOR_CLOSE_MESSAGE);
    MESSAGE_COMPONENTS.put(6, DatasetOperationMessageType.TRY_ADVANCE_FETCH_MESSAGE);
    MESSAGE_COMPONENTS.put(7, DatasetOperationMessageType.TRY_ADVANCE_RELEASE_MESSAGE);
    MESSAGE_COMPONENTS.put(9, DatasetOperationMessageType.SEND_CHANGE_EVENTS_MESSAGE);
    MESSAGE_COMPONENTS.put(12, DatasetOperationMessageType.INDEX_LIST_MESSAGE);
    MESSAGE_COMPONENTS.put(13, DatasetOperationMessageType.TRY_ADVANCE_MUTATE_MESSAGE);
    MESSAGE_COMPONENTS.put(14, DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE);
    MESSAGE_COMPONENTS.put(15, DatasetOperationMessageType.UNIVERSAL_NOP_MESSAGE);
    MESSAGE_COMPONENTS.put(16, DatasetOperationMessageType.INDEX_CREATE_MESSAGE);
    MESSAGE_COMPONENTS.put(17, DatasetOperationMessageType.INDEX_CREATE_STATUS_MESSAGE);
    MESSAGE_COMPONENTS.put(18, DatasetOperationMessageType.INDEX_DESTROY_MESSAGE);
    MESSAGE_COMPONENTS.put(19, DatasetOperationMessageType.INDEX_STATUS_MESSAGE);
    MESSAGE_COMPONENTS.put(22, DatasetOperationMessageType.PIPELINE_REQUEST_RESULT_MESSAGE);
    MESSAGE_COMPONENTS.put(24, DatasetOperationMessageType.BATCH_FETCH_MESSAGE);
    MESSAGE_COMPONENTS.put(25, DatasetOperationMessageType.IDENTIFY_CLIENT_MESSAGE);
  }

  private static final Map<Integer, DatasetEntityResponseType> RESPONSE_COMPONENTS = new HashMap<>();
  static {
    RESPONSE_COMPONENTS.put(0, DatasetEntityResponseType.ERROR_RESPONSE);
    RESPONSE_COMPONENTS.put(1, DatasetEntityResponseType.ADD_RECORD_FULL_RESPONSE);
    RESPONSE_COMPONENTS.put(2, DatasetEntityResponseType.ADD_RECORD_SIMPLIFIED_RESPONSE);
    RESPONSE_COMPONENTS.put(3, DatasetEntityResponseType.GET_RECORD_RESPONSE);
    RESPONSE_COMPONENTS.put(4, DatasetEntityResponseType.CHANGE_EVENT_RESPONSE);
    RESPONSE_COMPONENTS.put(5, DatasetEntityResponseType.PREDICATED_UPDATE_RECORD_FULL_RESPONSE);
    RESPONSE_COMPONENTS.put(6, DatasetEntityResponseType.PREDICATED_UPDATE_RECORD_SIMPLIFIED_RESPONSE);
    RESPONSE_COMPONENTS.put(7, DatasetEntityResponseType.PREDICATED_DELETE_RECORD_FULL_RESPONSE);
    RESPONSE_COMPONENTS.put(8, DatasetEntityResponseType.PREDICATED_DELETE_RECORD_SIMPLIFIED_RESPONSE);
    RESPONSE_COMPONENTS.put(9, DatasetEntityResponseType.PIPELINE_PROCESSOR_OPEN_RESPONSE);
    RESPONSE_COMPONENTS.put(10, DatasetEntityResponseType.PIPELINE_PROCESSOR_CLOSE_RESPONSE);
    RESPONSE_COMPONENTS.put(11, DatasetEntityResponseType.TRY_ADVANCE_FETCH_APPLY_RESPONSE);
    RESPONSE_COMPONENTS.put(12, DatasetEntityResponseType.TRY_ADVANCE_FETCH_EXHAUSTED_RESPONSE);
    RESPONSE_COMPONENTS.put(13, DatasetEntityResponseType.TRY_ADVANCE_RELEASE_RESPONSE);
    RESPONSE_COMPONENTS.put(15, DatasetEntityResponseType.SUCCESS_RESPONSE);
    RESPONSE_COMPONENTS.put(16, DatasetEntityResponseType.EXECUTE_TERMINATED_PIPELINE_RESPONSE);
    RESPONSE_COMPONENTS.put(17, DatasetEntityResponseType.INDEX_LIST_RESPONSE);
    RESPONSE_COMPONENTS.put(18, DatasetEntityResponseType.TRY_ADVANCE_FETCH_WAYPOINT_RESPONSE);
    RESPONSE_COMPONENTS.put(19, DatasetEntityResponseType.TRY_ADVANCE_FETCH_CONSUMED_RESPONSE);
    RESPONSE_COMPONENTS.put(20, DatasetEntityResponseType.INDEX_CREATE_ACCEPT_RESPONSE);
    RESPONSE_COMPONENTS.put(21, DatasetEntityResponseType.INDEX_CREATE_STATUS_RESPONSE);
    RESPONSE_COMPONENTS.put(22, DatasetEntityResponseType.INDEX_STATUS_RESPONSE);
    RESPONSE_COMPONENTS.put(23, DatasetEntityResponseType.PIPELINE_REQUEST_PROCESSING_RESPONSE);
    RESPONSE_COMPONENTS.put(24, DatasetEntityResponseType.BATCH_FETCH_RESPONSE);
    RESPONSE_COMPONENTS.put(25, DatasetEntityResponseType.BUSY_RESPONSE);
    RESPONSE_COMPONENTS.put(26, DatasetEntityResponseType.NULL_CRUD_REPLICATION_RESPONSE);
  }

  public DatasetOperationMessageCodec(IntrinsicCodec intrinsicCodec) {
    super(intrinsicCodec, DatasetOperationMessageType.class, MESSAGE_COMPONENTS, DatasetEntityResponseType.class, RESPONSE_COMPONENTS);
  }

  public static Map<Integer, DatasetOperationMessageType> getMessageComponents() {
    return Collections.unmodifiableMap(MESSAGE_COMPONENTS);
  }

  public static Map<Integer, DatasetEntityResponseType> getResponseComponents() {
    return Collections.unmodifiableMap(RESPONSE_COMPONENTS);
  }
}
