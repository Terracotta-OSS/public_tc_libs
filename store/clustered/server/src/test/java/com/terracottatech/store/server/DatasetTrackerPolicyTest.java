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

import org.junit.Test;

import com.terracottatech.store.common.messages.DatasetOperationMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.ServerServerMessage;
import com.terracottatech.store.server.messages.ServerServerMessageType;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;

import java.util.Collections;
import java.util.Set;

import static com.terracottatech.store.common.messages.DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE;
import static com.terracottatech.store.server.DatasetTrackerPolicy.TRACKABLE_OP_MESSAGES;
import static com.terracottatech.store.server.messages.ReplicationMessageType.CRUD_DATA_REPLICATION_MESSAGE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatasetTrackerPolicyTest {

  @Test
  public void trackable_OpMessages() throws Exception {
    DatasetTrackerPolicy datasetTrackerPolicy = new DatasetTrackerPolicy();
    DatasetOperationMessage message = mock(DatasetOperationMessage.class);
    for (DatasetOperationMessageType type : DatasetOperationMessageType.values()) {
      if (type == EXECUTE_TERMINATED_PIPELINE_MESSAGE) {  //trackability tested by trackable_DatasetOperationMessages()
        continue;
      }
      when(message.getType()).thenReturn(type);
      boolean trackable = datasetTrackerPolicy.test(message);
      if (TRACKABLE_OP_MESSAGES.contains(type)) {
        assertThat(trackable, is(true));
      } else {
        assertThat(trackable, is(false));
      }
    }
  }

  @Test
  public void trackable_ExecuteTerminatedPipelineMessage() throws Exception {
    ExecuteTerminatedPipelineMessage message = mock(ExecuteTerminatedPipelineMessage.class);
    when(message.getType()).thenReturn(EXECUTE_TERMINATED_PIPELINE_MESSAGE);
    when(message.isMutative()).thenReturn(false);

    DatasetTrackerPolicy datasetTrackerPolicy = new DatasetTrackerPolicy();
    assertThat(datasetTrackerPolicy.test(message), is(false));

    when(message.isMutative()).thenReturn(true);
    assertThat(datasetTrackerPolicy.test(message), is(true));
  }

  @Test
  public void trackable_ReplicationMessages() throws Exception {
    Set<ReplicationMessageType> trackableTypes = Collections.singleton(CRUD_DATA_REPLICATION_MESSAGE);

    DatasetTrackerPolicy datasetTrackerPolicy = new DatasetTrackerPolicy();
    ReplicationMessage message = mock(ReplicationMessage.class);
    for (ReplicationMessageType type : ReplicationMessageType.values()) {
      when(message.getType()).thenReturn(type);
      boolean trackable = datasetTrackerPolicy.test(message);
      if (trackableTypes.contains(type)) {
        assertThat(trackable, is(true));
      } else {
        assertThat(trackable, is(false));
      }
    }
  }

  @Test
  public void trackable_SyncMessages() throws Exception {
    DatasetTrackerPolicy datasetTrackerPolicy = new DatasetTrackerPolicy();
    ServerServerMessage message = mock(ServerServerMessage.class);
    for (ServerServerMessageType type : ServerServerMessageType.values()) {
      when(message.getType()).thenReturn(type);
      assertThat(datasetTrackerPolicy.test(message), is(false));
    }
  }

}