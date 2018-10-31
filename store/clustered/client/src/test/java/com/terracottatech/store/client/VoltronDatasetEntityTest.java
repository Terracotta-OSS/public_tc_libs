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
package com.terracottatech.store.client;

import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.endpoint.SimpleResponder;
import com.terracottatech.store.client.endpoint.TestEndpoint;
import com.terracottatech.store.common.exceptions.ClientIdCollisionException;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.ErrorResponse;
import com.terracottatech.store.common.messages.IdentifyClientMessage;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.common.messages.SuccessResponse;
import com.terracottatech.store.common.messages.crud.AddRecordFullResponse;
import com.terracottatech.store.common.messages.crud.AddRecordMessage;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.GetRecordMessage;
import com.terracottatech.store.common.messages.crud.GetRecordResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.exception.EntityException;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.terracottatech.store.client.AssertionUtils.assertAllCellsEqual;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class VoltronDatasetEntityTest {
  @Mock
  private EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint;

  @Mock
  private InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> invocationBuilder;

  @Captor
  private ArgumentCaptor<DatasetEntityMessage> messageCaptor;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    when(invocationBuilder.message(messageCaptor.capture())).thenReturn(invocationBuilder);
    when(invocationBuilder.invoke()).thenAnswer(getIdentifyOk());
    when(endpoint.beginInvoke()).thenReturn(invocationBuilder);
  }

  private Answer<Object> getIdentifyOk() {
    return invocation -> {
        DatasetEntityMessage message = messageCaptor.getValue();
        if (message instanceof IdentifyClientMessage) {
          SuccessResponse response = new SuccessResponse();
          return new InvokeFuture<DatasetEntityResponse>() {
            @Override
            public boolean isDone() {
              return true;
            }

            @Override
            public DatasetEntityResponse get() throws InterruptedException, EntityException {
              return response;
            }

            @Override
            public DatasetEntityResponse getWithTimeout(long l, TimeUnit timeUnit)
                throws InterruptedException, EntityException, TimeoutException {
              return response;
            }

            @Override
            public void interrupt() {

            }
          };
        }
        return null;
      };
  }

  @Test
  public void testHandlesIdentifyCollision() throws Exception {
    resetInvocationBuilder();
    when(invocationBuilder.message(messageCaptor.capture())).thenReturn(invocationBuilder);
    when(endpoint.beginInvoke()).thenReturn(invocationBuilder);

    AtomicBoolean oopsCalled = new AtomicBoolean(false);
    Answer<Object> oopsAnswer = invocation -> {
      DatasetEntityMessage message = messageCaptor.getValue();
      if (message instanceof IdentifyClientMessage) {
        ErrorResponse response = new ErrorResponse(new ClientIdCollisionException("oops"));
        return new InvokeFuture<DatasetEntityResponse>() {
          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public DatasetEntityResponse get() throws InterruptedException, EntityException {
            oopsCalled.set(true);
            return response;
          }

          @Override
          public DatasetEntityResponse getWithTimeout(long l, TimeUnit timeUnit)
              throws InterruptedException, EntityException, TimeoutException {
            oopsCalled.set(true);
            return response;
          }

          @Override
          public void interrupt() {

          }
        };
      }
      return null;
    };
    when(invocationBuilder.invoke()).thenAnswer(oopsAnswer).thenAnswer(getIdentifyOk());

    createAndClose();
    assertTrue(oopsCalled.get());
  }

  @Test
  public void testHandlesIdentifyException() throws Exception {
    resetInvocationBuilder();
    when(invocationBuilder.message(messageCaptor.capture())).thenReturn(invocationBuilder);
    when(endpoint.beginInvoke()).thenReturn(invocationBuilder);

    Answer<Object> oopsAnswer = invocation -> {
      DatasetEntityMessage message = messageCaptor.getValue();
      if (message instanceof IdentifyClientMessage) {
        ErrorResponse response = new ErrorResponse(new IllegalStateException("oops"));
        return new InvokeFuture<DatasetEntityResponse>() {
          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public DatasetEntityResponse get() throws InterruptedException, EntityException {
            return response;
          }

          @Override
          public DatasetEntityResponse getWithTimeout(long l, TimeUnit timeUnit)
              throws InterruptedException, EntityException, TimeoutException {
            return response;
          }

          @Override
          public void interrupt() {

          }
        };
      }
      return null;
    };
    when(invocationBuilder.invoke()).thenAnswer(oopsAnswer);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("oops");
    VoltronDatasetEntity.newInstance(Type.STRING, endpoint, null);
  }

  @Test
  public void closesEndpoint() {
    createAndClose();
    verify(endpoint).close();
  }

  @SuppressWarnings({"try", "unused"})
  private void createAndClose() {
    try (VoltronDatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, endpoint, null)) {
    }
  }

  @Test
  public void entityOnlyClosedIfDatasetClosed() {
    VoltronDatasetEntity.newInstance(Type.STRING, endpoint, null);
    verify(endpoint).setDelegate(any(DatasetEndpointDelegate.class));
    verify(endpoint).beginInvoke();
    verifyNoMoreInteractions(endpoint);
  }

  @Test
  public void givesCorrectKeyTypeString() {
    VoltronDatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, endpoint, null);
    assertEquals(Type.STRING, entity.getKeyType());
  }

  @Test
  public void givesCorrectKeyTypeLong() {
    VoltronDatasetEntity<Long> entity = VoltronDatasetEntity.newInstance(Type.LONG, endpoint, null);
    assertEquals(Type.LONG, entity.getKeyType());
  }

  @Test
  public void addWithAddedResult() throws Exception {
    assertTrue(runAddTest(true, "bob", Collections.emptyList()));
  }

  @Test
  public void addWithCellsWithAddedResult() throws Exception {
    StringCellDefinition cell1 = defineString("cell1");
    assertTrue(runAddTest(true, "bob", Arrays.asList(cell1.newCell("abc"), cell1.newCell("def"))));
  }

  @Test
  public void addWithAlreadyExistsResult() throws Exception {
    assertFalse(runAddTest(false, "bob", Collections.emptyList()));
  }

  @Test
  public void addReturnRecordWithAddedResult() throws Exception {
    runAddReturnRecordTest(true, "bob", Collections.emptySet());
  }

  @Test
  public void addReturnRecordWithCellsWithAddedResult() throws Exception {
    StringCellDefinition cell1 = defineString("cell1");
    runAddReturnRecordTest(true, "bob", Arrays.asList(cell1.newCell("abc"), cell1.newCell("def")));
  }

  @Test
  public void addReturnRecordWithAlreadyExistsResult() throws Exception {
    runAddReturnRecordTest(false, "bob", Collections.emptySet());
  }

  @Test
  public void getWithRecordPresent() throws Exception {
    runGetTest(true, true);
  }

  @Test
  public void getWithRecordAbsent() throws Exception {
    runGetTest(false, false);
  }

  @Test
  public void updateWithRecordPresent() throws Exception {
    runUpdateTest(true);
  }

  @Test
  public void updateWithRecordAbsent() throws Exception {
    runUpdateTest(false);
  }

  @Test
  public void updateReturnTupleWithRecordPresent() throws Exception {
    runUpdateReturnTupleTest(true);
  }

  @Test
  public void updateReturnTupleWithRecordAbsent() throws Exception {
    runUpdateReturnTupleTest(false);
  }

  @Test
  public void deleteWithRecordPresent() throws Exception {
    runDeleteTest(true);
  }

  @Test
  public void deleteWithRecordAbsent() throws Exception {
    runDeleteTest(false);
  }

  @Test
  public void deleteReturnRecordWithRecordPresent() throws Exception {
    runDeleteReturnRecordTest(true);
  }

  @Test
  public void deleteReturnRecordWithRecordAbsent() throws Exception {
    runDeleteReturnRecordTest(false);
  }

  @Test
  public void registerChangeListener() {
    SuccessResponse response = mock(SuccessResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    VoltronDatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    entity.registerChangeListener(new NullChangeListener<>());

    SendChangeEventsMessage sendChangeEventsMessage = (SendChangeEventsMessage) responder.getMessage();
    assertTrue(sendChangeEventsMessage.sendChangeEvents());
  }

  @Test
  public void deregisterChangeListener() {
    SuccessResponse response = mock(SuccessResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    VoltronDatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    ChangeListener<String> listener = new NullChangeListener<>();
    entity.registerChangeListener(listener);
    entity.deregisterChangeListener(listener);

    SendChangeEventsMessage sendChangeEventsMessage = (SendChangeEventsMessage) responder.getMessage();
    assertFalse(sendChangeEventsMessage.sendChangeEvents());
  }

  private boolean runAddTest(boolean added, String key, Iterable<Cell<?>> cells) throws Exception {
    AddRecordSimplifiedResponse response = mock(AddRecordSimplifiedResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    DatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    when(response.isAdded()).thenReturn(added);

    boolean result = entity.add(key, cells);

    AddRecordMessage<String> addRecordMessage = (AddRecordMessage<String>) responder.getMessage();
    assertEquals(DatasetOperationMessageType.ADD_RECORD_MESSAGE, addRecordMessage.getType());
    assertEquals(key, addRecordMessage.getKey());
    assertAllCellsEqual(cells, addRecordMessage.getCells());

    return result;
  }

  private void runAddReturnRecordTest(boolean added, String key, Iterable<Cell<?>> cells) {
    AddRecordFullResponse<String> response = mock(AddRecordFullResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    DatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    if (added) {
      when(response.getExisting()).thenReturn(null);
    } else {
      when(response.getExisting()).thenReturn(new RecordData<>(10L, key, Collections.emptyList()));
    }

    Record<String> record = entity.addReturnRecord(key, cells);

    AddRecordMessage<String> addRecordMessage = (AddRecordMessage<String>) responder.getMessage();
    assertEquals(DatasetOperationMessageType.ADD_RECORD_MESSAGE, addRecordMessage.getType());
    assertEquals(key, addRecordMessage.getKey());
    assertAllCellsEqual(cells, addRecordMessage.getCells());

    if (added) {
      assertNull(record);
    } else {
      assertEquals(key, record.getKey());
    }
  }

  private void runGetTest(boolean responseResult, boolean getResult) throws Exception {
    GetRecordResponse<String> response = mock(GetRecordResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    VoltronDatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    if (responseResult) {
      when(response.getData()).thenReturn(new RecordData<>(1L, "bob", Collections.emptySet()));
    } else {
      when(response.getData()).thenReturn(null);
    }

    Record<String> record = entity.get("bob");
    assertEquals(getResult, record != null);
    if (record != null) {
      assertEquals("bob", record.getKey());
    }

    GetRecordMessage<String> getRecordMessage = (GetRecordMessage<String>) responder.getMessage();
    assertEquals(DatasetOperationMessageType.GET_RECORD_MESSAGE, getRecordMessage.getType());
    assertEquals("bob", getRecordMessage.getKey());
  }

  private void runDeleteTest(boolean delete) {
    PredicatedDeleteRecordSimplifiedResponse response = mock(PredicatedDeleteRecordSimplifiedResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    DatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    when(response.isDeleted()).thenReturn(delete);

    boolean deleted;
    if (delete) {
      deleted = entity.delete("bob", AlwaysTrue.alwaysTrue());
    } else {
      deleted = entity.delete("bob", AlwaysTrue.alwaysTrue().negate());
    }
    assertThat(deleted, is(delete));

    PredicatedDeleteRecordMessage<String> message = (PredicatedDeleteRecordMessage<String>) responder.getMessage();
    assertFalse(message.isRespondInFull());
  }

  private void runDeleteReturnRecordTest(boolean delete) {
    PredicatedDeleteRecordFullResponse<String> response = mock(PredicatedDeleteRecordFullResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    DatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    if (delete) {
      when(response.getDeleted()).thenReturn(new RecordData<>(1L, "bob", Collections.emptyList()));
    } else {
      when(response.getDeleted()).thenReturn(null);
    }

    if (delete) {
      Record<String> deleted = entity.deleteReturnRecord("bob", AlwaysTrue.alwaysTrue());
      assertEquals("bob", deleted.getKey());
    } else {
      Record<String> deleted = entity.deleteReturnRecord("bob", AlwaysTrue.alwaysTrue().negate());
      assertNull(deleted);
    }

    PredicatedDeleteRecordMessage<String> message = (PredicatedDeleteRecordMessage<String>) responder.getMessage();
    assertTrue(message.isRespondInFull());
  }

  private void runUpdateTest(boolean update) {
    PredicatedUpdateRecordSimplifiedResponse response = mock(PredicatedUpdateRecordSimplifiedResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    DatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    when(response.isUpdated()).thenReturn(update);

    boolean updated = entity.update("bob",
            AlwaysTrue.alwaysTrue(),
            new InstallOperation<>(Collections.emptyList()));
    assertEquals(updated, update);

    PredicatedUpdateRecordMessage<String> updateRecordMessage = (PredicatedUpdateRecordMessage<String>) responder.getMessage();
    assertEquals(DatasetOperationMessageType.PREDICATED_UPDATE_RECORD_MESSAGE, updateRecordMessage.getType());
    assertEquals("bob", updateRecordMessage.getKey());
    assertFalse(updateRecordMessage.isRespondInFull());
  }

  private void runUpdateReturnTupleTest(boolean update) {
    PredicatedUpdateRecordFullResponse<String> response = mock(PredicatedUpdateRecordFullResponse.class);
    SimpleResponder responder = new SimpleResponder(response);
    TestEndpoint testEndpoint = new TestEndpoint(responder);
    DatasetEntity<String> entity = VoltronDatasetEntity.newInstance(Type.STRING, testEndpoint, null);

    if (update) {
      when(response.getBefore()).thenReturn(new RecordData<>(0L, "bob", Collections.emptySet()));
      when(response.getAfter()).thenReturn(new RecordData<>(1L, "bob", Collections.singleton(Cell.cell("name", "bob"))));
    } else {
      when(response.getBefore()).thenReturn(null);
      when(response.getAfter()).thenReturn(null);
    }

    Tuple<Record<String>, Record<String>> tuple = entity.updateReturnTuple("bob",
            AlwaysTrue.alwaysTrue(),
            new InstallOperation<>(Collections.emptyList()));
    if (update) {
      assertNotEquals(tuple.getFirst(), tuple.getSecond());
    } else {
      assertNull(tuple);
    }

    PredicatedUpdateRecordMessage<String> updateRecordMessage = (PredicatedUpdateRecordMessage<String>) responder.getMessage();
    assertEquals(DatasetOperationMessageType.PREDICATED_UPDATE_RECORD_MESSAGE, updateRecordMessage.getType());
    assertEquals("bob", updateRecordMessage.getKey());
    assertTrue(updateRecordMessage.isRespondInFull());
  }

  @Test
  public void testStreamIdGenerator() throws Exception {
    VoltronDatasetEntity.StreamIDGenerator streamIDGenerator = new VoltronDatasetEntity.StreamIDGenerator();
    UUID baseStreamId = streamIDGenerator.next();
    for (int i = 0; i < 100; i++) {
      UUID nextStreamId = streamIDGenerator.next();
      assertThat(nextStreamId.getLeastSignificantBits(), is(equalTo(baseStreamId.getLeastSignificantBits())));
      assertThat(nextStreamId.getMostSignificantBits(), is(not(equalTo(baseStreamId.getMostSignificantBits()))));
    }

    VoltronDatasetEntity.StreamIDGenerator otherStreamIdGenerator = new VoltronDatasetEntity.StreamIDGenerator();
    UUID otherBaseStreamId = otherStreamIdGenerator.next();
    assertThat(otherBaseStreamId.getLeastSignificantBits(), is(not(equalTo(baseStreamId.getLeastSignificantBits()))));
  }

  @SuppressWarnings("unchecked")
  private void resetInvocationBuilder() {
    reset(invocationBuilder);
  }

  private static class NullChangeListener<K extends Comparable<K>> implements ChangeListener<K> {
    @Override
    public void onChange(K key, ChangeType changeType) {
    }

    @Override
    public void missedEvents() {
    }
  }
}
