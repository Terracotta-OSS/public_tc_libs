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
import com.terracottatech.store.common.messages.event.ChangeEventResponse;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.common.messages.indexing.IndexListResponse;

import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import com.terracottatech.store.ChangeType;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.common.messages.intrinsics.TestIntrinsicDescriptors;

import java.util.Arrays;
import java.util.UUID;

import static com.terracottatech.store.common.messages.BaseMessageTest.createCells;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Ludovic Orban
 */
public class DatasetOperationMessageCodecTest {

  private final UUID stableClientId = UUID.randomUUID();

  private final DatasetOperationMessageCodec codec =
      new DatasetOperationMessageCodec(new IntrinsicCodec(TestIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS));

  @Test
  public void messageAddRecordString() throws Exception {
    byte[] encoded = codec.encodeMessage(new AddRecordMessage<>(stableClientId, "myKey", createCells(), false));

    DatasetOperationMessage datasetOperationMessage = (DatasetOperationMessage) codec.decodeMessage(encoded);
    assertThat(datasetOperationMessage.getType(), is(DatasetOperationMessageType.ADD_RECORD_MESSAGE));
    AddRecordMessage<?> addRecordMessage = (AddRecordMessage) datasetOperationMessage;
    assertThat(addRecordMessage.getKey(), is("myKey"));
    assertThat(addRecordMessage.isRespondInFull(), is(false));
    assertThat(addRecordMessage.getCells(), containsInAnyOrder(createCells().toArray()));
    assertThat(addRecordMessage.getStableClientId(), is(stableClientId));
  }

  @Test
  public void messageAddRecordChar() throws Exception {
    byte[] encoded = codec.encodeMessage(new AddRecordMessage<>(stableClientId, 'K', createCells(), true));

    DatasetOperationMessage datasetOperationMessage = (DatasetOperationMessage) codec.decodeMessage(encoded);
    assertThat(datasetOperationMessage.getType(), is(DatasetOperationMessageType.ADD_RECORD_MESSAGE));
    AddRecordMessage<?> addRecordMessage = (AddRecordMessage) datasetOperationMessage;
    assertThat(addRecordMessage.getKey(), is('K'));
    assertThat(addRecordMessage.isRespondInFull(), is(true));
    assertThat(addRecordMessage.getCells(), containsInAnyOrder(createCells().toArray()));
    assertThat(addRecordMessage.getStableClientId(), is(stableClientId));
  }

  @Test
  public void messageGetRecord() throws Exception {
    byte[] encoded = codec.encodeMessage(new GetRecordMessage<>(Long.MAX_VALUE, AlwaysTrue.alwaysTrue()));

    DatasetOperationMessage datasetOperationMessage = (DatasetOperationMessage) codec.decodeMessage(encoded);
    assertThat(datasetOperationMessage.getType(), is(DatasetOperationMessageType.GET_RECORD_MESSAGE));
    GetRecordMessage<?> getRecordMessage = (GetRecordMessage) datasetOperationMessage;
    assertThat(getRecordMessage.getKey(), is(Long.MAX_VALUE));
    assertThat(getRecordMessage.getPredicate().test(null), is(true));
  }

  @Test
  public void messagePredicatedUpdateRecord() throws Exception {
    @SuppressWarnings("unchecked")
    byte[] encoded = codec.encodeMessage(new PredicatedUpdateRecordMessage<>(stableClientId, "bob", AlwaysTrue.alwaysTrue(),
            (IntrinsicUpdateOperation<String>) Functions.installUpdateOperation(createCells()),
            true));

    DatasetOperationMessage datasetOperationMessage = (DatasetOperationMessage) codec.decodeMessage(encoded);
    assertThat(datasetOperationMessage.getType(), is(DatasetOperationMessageType.PREDICATED_UPDATE_RECORD_MESSAGE));
    PredicatedUpdateRecordMessage<?> predicatedUpdateRecordMessage = (PredicatedUpdateRecordMessage) datasetOperationMessage;
    assertThat(predicatedUpdateRecordMessage.isRespondInFull(), is(true));
    assertThat(predicatedUpdateRecordMessage.getKey(), is("bob"));
    assertThat(predicatedUpdateRecordMessage.getPredicate().test(null), is(true));
    assertThat(predicatedUpdateRecordMessage.getUpdateOperation().apply(null), containsInAnyOrder(createCells().toArray()));
    assertThat(predicatedUpdateRecordMessage.getStableClientId(), is(stableClientId));
  }

  @Test
  public void messagePredicatedDeleteRecord() throws Exception {
    byte[] encoded = codec.encodeMessage(new PredicatedDeleteRecordMessage<>(stableClientId, "bob", AlwaysTrue.alwaysTrue(),
        true));

    DatasetOperationMessage datasetOperationMessage = (DatasetOperationMessage) codec.decodeMessage(encoded);
    assertThat(datasetOperationMessage.getType(), is(DatasetOperationMessageType.PREDICATED_DELETE_RECORD_MESSAGE));
    PredicatedDeleteRecordMessage<?> predicatedDeleteRecordMessage = (PredicatedDeleteRecordMessage) datasetOperationMessage;
    assertThat(predicatedDeleteRecordMessage.isRespondInFull(), is(true));
    assertThat(predicatedDeleteRecordMessage.getKey(), is("bob"));
    assertThat(predicatedDeleteRecordMessage.getPredicate().test(null), is(true));
    assertThat(predicatedDeleteRecordMessage.getStableClientId(), is(stableClientId));
  }

  @Test
  public void messageSendChangeEvents() throws Exception {
    byte[] encoded = codec.encodeMessage(new SendChangeEventsMessage(true));

    SendChangeEventsMessage decodedMessage = (SendChangeEventsMessage) codec.decodeMessage(encoded);
    assertEquals(DatasetOperationMessageType.SEND_CHANGE_EVENTS_MESSAGE, decodedMessage.getType());
    assertTrue(decodedMessage.sendChangeEvents());
  }

  @Test
  public void responseGetRecordExists() throws Exception {
    byte[] bytes = codec.encodeResponse(new GetRecordResponse<>("foo", 0L, createCells()));

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.GET_RECORD_RESPONSE));
    GetRecordResponse<?> getRecordResponse = (GetRecordResponse) datasetEntityResponse;
    assertThat(getRecordResponse.getData(), notNullValue());
    assertThat(getRecordResponse.getData().getCells(), containsInAnyOrder(createCells().toArray()));
  }

  @Test
  public void responseGetRecordDoesNotExist() throws Exception {
    byte[] bytes = codec.encodeResponse(new GetRecordResponse<>());

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.GET_RECORD_RESPONSE));
    GetRecordResponse<?> getRecordResponse = (GetRecordResponse) datasetEntityResponse;
    assertThat(getRecordResponse.getData(), nullValue());
  }

  @Test
  public void responseAddRecordSimplified() throws Exception {
    byte[] bytes = codec.encodeResponse(new AddRecordSimplifiedResponse(false));

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.ADD_RECORD_SIMPLIFIED_RESPONSE));
    AddRecordSimplifiedResponse addRecordSimplifiedResponse = (AddRecordSimplifiedResponse) datasetEntityResponse;
    assertThat(addRecordSimplifiedResponse.isAdded(), is(false));
  }

  @Test
  public void responseAddRecordFull() throws Exception {
    byte[] bytes = codec.encodeResponse(new AddRecordFullResponse<>("foo", 123L, createCells()));
    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.ADD_RECORD_FULL_RESPONSE));
    AddRecordFullResponse<?> addRecordFullResponse = (AddRecordFullResponse) datasetEntityResponse;
    assertThat(addRecordFullResponse.getExisting().getMsn(), is(123L));
    assertThat(addRecordFullResponse.getExisting().getCells(), containsInAnyOrder(createCells().toArray()));
  }

  @Test
  public void responsePredicatedUpdateSimplified() throws Exception {
    byte[] bytes = codec.encodeResponse(new PredicatedUpdateRecordSimplifiedResponse(true));

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.PREDICATED_UPDATE_RECORD_SIMPLIFIED_RESPONSE));
    PredicatedUpdateRecordSimplifiedResponse predicatedUpdateRecordSimplifiedResponse = (PredicatedUpdateRecordSimplifiedResponse) datasetEntityResponse;
    assertThat(predicatedUpdateRecordSimplifiedResponse.isUpdated(), is(true));
  }

  @Test
  public void responsePredicatedUpdateFull() throws Exception {
    byte[] bytes = codec.encodeResponse(new PredicatedUpdateRecordFullResponse<>("foo", Long.MIN_VALUE, createCells(), Long.MAX_VALUE, createCells()));

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.PREDICATED_UPDATE_RECORD_FULL_RESPONSE));
    PredicatedUpdateRecordFullResponse<?> predicatedUpdateRecordFullResponse = (PredicatedUpdateRecordFullResponse) datasetEntityResponse;
    assertThat(predicatedUpdateRecordFullResponse.getBefore().getMsn(), is(Long.MIN_VALUE));
    assertThat(predicatedUpdateRecordFullResponse.getBefore().getCells(), containsInAnyOrder(createCells().toArray()));
    assertThat(predicatedUpdateRecordFullResponse.getAfter().getMsn(), is(Long.MAX_VALUE));
    assertThat(predicatedUpdateRecordFullResponse.getAfter().getCells(), containsInAnyOrder(createCells().toArray()));
  }

  @Test
  public void responsePredicatedDeleteSimplified() throws Exception {
    byte[] bytes = codec.encodeResponse(new PredicatedDeleteRecordSimplifiedResponse(true));

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.PREDICATED_DELETE_RECORD_SIMPLIFIED_RESPONSE));
    PredicatedDeleteRecordSimplifiedResponse predicatedDeleteRecordSimplifiedResponse = (PredicatedDeleteRecordSimplifiedResponse) datasetEntityResponse;
    assertThat(predicatedDeleteRecordSimplifiedResponse.isDeleted(), is(true));
  }

  @Test
  public void responsePredicatedDeleteFull() throws Exception {
    byte[] bytes = codec.encodeResponse(new PredicatedDeleteRecordFullResponse<>("foo", 100L, createCells()));

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.PREDICATED_DELETE_RECORD_FULL_RESPONSE));
    PredicatedDeleteRecordFullResponse<?> predicatedDeleteRecordFullResponse = (PredicatedDeleteRecordFullResponse<?>) datasetEntityResponse;
    assertThat(predicatedDeleteRecordFullResponse.getDeleted().getMsn(), is(100L));
    assertThat(predicatedDeleteRecordFullResponse.getDeleted().getCells(), containsInAnyOrder(createCells().toArray()));
  }

  @Test
  public void responseError() throws Exception {
    byte[] bytes = codec.encodeResponse(new ErrorResponse(new Throwable("errorCause")));

    DatasetEntityResponse datasetEntityResponse = codec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(DatasetEntityResponseType.ERROR_RESPONSE));
    assertThat(datasetEntityResponse, instanceOf(ErrorResponse.class));
    assertThat(((ErrorResponse)datasetEntityResponse).getCause().getMessage(), is("errorCause"));
  }

  @Test
  public void responseChangeEvent() throws Exception {
    byte[] encoded = codec.encodeResponse(new ChangeEventResponse<>("key", ChangeType.MUTATION));

    ChangeEventResponse<?> decodedResponse = (ChangeEventResponse) codec.decodeResponse(encoded);
    assertEquals(DatasetEntityResponseType.CHANGE_EVENT_RESPONSE, decodedResponse.getType());
    assertEquals("key", decodedResponse.getKey());
    assertEquals(ChangeType.MUTATION, decodedResponse.getChangeType());
  }

  @Test
  public void testSuccessResponse() throws Exception {
    byte[] encoded = codec.encodeResponse(new SuccessResponse());
    SuccessResponse decodedResponse = (SuccessResponse) codec.decodeResponse(encoded);
    assertEquals(DatasetEntityResponseType.SUCCESS_RESPONSE, decodedResponse.getType());
  }

  @Test
  public void testIndexListResponse() throws Exception {
    ImmutableIndex<String> indexA = new ImmutableIndex<>(defineString("name"), IndexSettings.btree(), Index.Status.LIVE);
    ImmutableIndex<Integer> indexB = new ImmutableIndex<>(defineInt("age"), IndexSettings.btree(), Index.Status.INITALIZING);
    IndexListResponse originalResponse = new IndexListResponse(Arrays.asList(indexA, indexB));

    IndexListResponse decodedReponse = (IndexListResponse) codec.decodeResponse(codec.encodeResponse(originalResponse));

    @SuppressWarnings("unchecked")
    Matcher<Iterable<Index<?>>> matcher = IsCollectionContaining.<Index<?>>hasItems(
            samePropertyValuesAs(indexA),
            samePropertyValuesAs(indexB)
    );
    assertThat(decodedReponse.getIndexes(), matcher);
  }
}
