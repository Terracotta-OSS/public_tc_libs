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
package com.terracottatech.store.server;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;

import com.terracottatech.store.common.messages.DatasetOperationMessage;

import java.util.EnumSet;
import java.util.function.Predicate;

import static com.terracottatech.store.common.messages.DatasetOperationMessageType.ADD_RECORD_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.INDEX_CREATE_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.INDEX_DESTROY_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.PREDICATED_DELETE_RECORD_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.PREDICATED_UPDATE_RECORD_MESSAGE;
import static com.terracottatech.store.server.messages.ReplicationMessageType.CRUD_DATA_REPLICATION_MESSAGE;

public class DatasetTrackerPolicy implements Predicate<DatasetEntityMessage> {

  public static EnumSet<DatasetOperationMessageType> TRACKABLE_OP_MESSAGES = EnumSet.of(ADD_RECORD_MESSAGE,
      PREDICATED_UPDATE_RECORD_MESSAGE, PREDICATED_DELETE_RECORD_MESSAGE, INDEX_DESTROY_MESSAGE, INDEX_CREATE_MESSAGE);

  public static EnumSet<ReplicationMessageType> TRACKABLE_REPLICATION_MESSAGES = EnumSet.of(CRUD_DATA_REPLICATION_MESSAGE);

  @Override
  public boolean test(DatasetEntityMessage entityMessage) {
    if (entityMessage instanceof DatasetOperationMessage) {
      switch (((DatasetOperationMessage) entityMessage).getType()) {
        case ADD_RECORD_MESSAGE:
        case PREDICATED_UPDATE_RECORD_MESSAGE:
        case PREDICATED_DELETE_RECORD_MESSAGE:
        case INDEX_DESTROY_MESSAGE:
        case INDEX_CREATE_MESSAGE:
          return true;
        case EXECUTE_TERMINATED_PIPELINE_MESSAGE:
          return ((ExecuteTerminatedPipelineMessage) entityMessage).isMutative();
        default:
          return false;
      }
    } else if (entityMessage instanceof ReplicationMessage) {
      switch (((ReplicationMessage) entityMessage).getType()) {
        case CRUD_DATA_REPLICATION_MESSAGE:
          return true;
        default:
          return false;
      }
    } else {
      return false;
    }
  }
}
