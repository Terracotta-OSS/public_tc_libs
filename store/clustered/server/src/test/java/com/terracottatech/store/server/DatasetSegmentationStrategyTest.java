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
import com.terracottatech.store.common.messages.crud.KeyMessage;
import com.terracottatech.store.server.concurrency.ConcurrencyShardMapper;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.ServerServerMessage;
import com.terracottatech.store.server.messages.ServerServerMessageType;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.store.common.messages.DatasetOperationMessageType.ADD_RECORD_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.INDEX_CREATE_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.INDEX_DESTROY_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.PREDICATED_DELETE_RECORD_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.PREDICATED_UPDATE_RECORD_MESSAGE;
import static com.terracottatech.store.server.DatasetTrackerPolicy.TRACKABLE_OP_MESSAGES;
import static com.terracottatech.store.server.DatasetTrackerPolicy.TRACKABLE_REPLICATION_MESSAGES;
import static com.terracottatech.store.server.messages.ReplicationMessageType.CRUD_DATA_REPLICATION_MESSAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatasetSegmentationStrategyTest {

  @Test
  public void testSegmentMapping_NonCRUD() throws Exception {
    Set<DatasetOperationMessageType> trackableTypes = Stream.of(INDEX_DESTROY_MESSAGE, INDEX_CREATE_MESSAGE
        , EXECUTE_TERMINATED_PIPELINE_MESSAGE).collect(Collectors.toSet());

    ConcurrencyShardMapper concurrencyShardMapper = mock(ConcurrencyShardMapper.class);
    when(concurrencyShardMapper.getShardCount()).thenReturn(5);
    DatasetSegmentationStrategy segmentationStrategy = new DatasetSegmentationStrategy(concurrencyShardMapper);

    for (DatasetOperationMessageType trackableType : trackableTypes) {
      DatasetOperationMessage message = mock(DatasetOperationMessage.class);
      when(message.getType()).thenReturn(trackableType);
      assertThat(segmentationStrategy.applyAsInt(message), is(5));
    }
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testSegmentMapping_CRUD() throws Exception {
    Set<DatasetOperationMessageType> trackableTypes = Stream.of(ADD_RECORD_MESSAGE, PREDICATED_UPDATE_RECORD_MESSAGE,
        PREDICATED_DELETE_RECORD_MESSAGE).collect(Collectors.toSet());

    Comparable key = mock(Comparable.class);
    ConcurrencyShardMapper concurrencyShardMapper = mock(ConcurrencyShardMapper.class);
    when(concurrencyShardMapper.getShardIndex(key)).thenReturn(5);
    DatasetSegmentationStrategy segmentationStrategy = new DatasetSegmentationStrategy(concurrencyShardMapper);

    for (DatasetOperationMessageType trackableType : trackableTypes) {
      KeyMessage message = mock(KeyMessage.class);
      when(message.getType()).thenReturn(trackableType);
      when(message.getKey()).thenReturn(key);
      assertThat(segmentationStrategy.applyAsInt(message), is(5));
    }
  }

  @Test
  public void testSegmentMapping_UntrackableOpMessages() throws Exception {
    DatasetSegmentationStrategy segmentationStrategy = new DatasetSegmentationStrategy(mock(ConcurrencyShardMapper.class));
    EnumSet<DatasetOperationMessageType> untrackableTypes = EnumSet.complementOf(TRACKABLE_OP_MESSAGES);
    DatasetOperationMessage message = mock(DatasetOperationMessage.class);
    for (DatasetOperationMessageType untrackableType : untrackableTypes) {
      try {
        when(message.getType()).thenReturn(untrackableType);
        segmentationStrategy.applyAsInt(message);
      } catch (AssertionError e) {
        assertThat(e.getMessage(), containsString("is not trackable"));
      }
    }
  }

  @Test
  public void testSegmentMapping_CRUDDataReplicationMessage() throws Exception {
    long index = 123L;
    ConcurrencyShardMapper concurrencyShardMapper = mock(ConcurrencyShardMapper.class);
    when(concurrencyShardMapper.getShardIndex(index)).thenReturn(5);
    DatasetSegmentationStrategy segmentationStrategy = new DatasetSegmentationStrategy(concurrencyShardMapper);

    CRUDDataReplicationMessage message = mock(CRUDDataReplicationMessage.class);
    when(message.getType()).thenReturn(CRUD_DATA_REPLICATION_MESSAGE);
    when(message.getIndex()).thenReturn(index);
    assertThat(segmentationStrategy.applyAsInt(message), is(5));
  }

  @Test
  public void testSegmentMapping_NonCRUDDataReplicationMessages() throws Exception {
    DatasetSegmentationStrategy segmentationStrategy = new DatasetSegmentationStrategy(mock(ConcurrencyShardMapper.class));
    EnumSet<ReplicationMessageType> untrackableTypes = EnumSet.complementOf(TRACKABLE_REPLICATION_MESSAGES);
    ReplicationMessage message = mock(ReplicationMessage.class);
    for (ReplicationMessageType untrackableType : untrackableTypes) {
      try {
        when(message.getType()).thenReturn(untrackableType);
        segmentationStrategy.applyAsInt(message);
      } catch (AssertionError e) {
        assertThat(e.getMessage(), containsString("is not trackable"));
      }
    }
  }

  @Test
  public void testSegmentMapping_SyncMessages() throws Exception {
    DatasetSegmentationStrategy segmentationStrategy = new DatasetSegmentationStrategy(mock(ConcurrencyShardMapper.class));
    ServerServerMessage message = mock(ServerServerMessage.class);
    for (ServerServerMessageType untrackableType : ServerServerMessageType.values()) {
      try {
        when(message.getType()).thenReturn(untrackableType);
        segmentationStrategy.applyAsInt(message);
      } catch (AssertionError e) {
        assertThat(e.getMessage(), containsString("is not trackable"));
      }
    }
  }

}