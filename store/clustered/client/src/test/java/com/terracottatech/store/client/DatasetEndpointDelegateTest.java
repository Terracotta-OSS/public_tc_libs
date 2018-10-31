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

import com.terracottatech.store.ChangeType;
import com.terracottatech.store.client.stream.RootRemoteRecordStream;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.event.ChangeEventResponse;
import com.terracottatech.store.common.reconnect.ReconnectCodec;
import com.terracottatech.store.common.reconnect.ReconnectState;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatasetEndpointDelegateTest {
  @Mock
  private ChangeEventManager<String> changeEventManager;

  @Mock
  private Supplier<ReconnectState> reconnectStateSupplier;

  @Mock
  private Set<RootRemoteRecordStream<String>> openStreams;

  @Mock
  private Runnable disconnectHook;

  private DatasetEndpointDelegate<String> delegate;

  @Before
  public void before() {
    delegate = new DatasetEndpointDelegate<>(changeEventManager, reconnectStateSupplier, openStreams, disconnectHook);
  }

  @Test
  public void delegateGivesEventToListener() {
    ChangeEventResponse<String> message = new ChangeEventResponse<>("key", ChangeType.ADDITION);
    delegate.handleMessage(message);
    verify(changeEventManager).changeEvent(message);
  }

  @Test
  public void copesWithADatasetEntityResponseOfTheWrongType() {
    delegate.handleMessage(new AddRecordSimplifiedResponse(true));
    verifyNoMoreInteractions(changeEventManager);
  }

  @Test
  public void copesWithUnexpectedKeyType() {
    ChangeEventResponse<Long> message = new ChangeEventResponse<>(1L, ChangeType.ADDITION);
    delegate.handleMessage(message);
    verify(changeEventManager).changeEvent(message);
    verifyNoMoreInteractions(changeEventManager);
  }

  @Test
  public void disconnectCausesMissedEvent() {
    delegate.didDisconnectUnexpectedly();
    verify(changeEventManager).missedEvents();
  }

  @Test
  public void disconnectFiresHook() {
    delegate.didDisconnectUnexpectedly();
    verify(disconnectHook).run();
  }

  @Test
  public void reconnectEncodesReconnectionState() {
    UUID indexReqId = UUID.randomUUID();
    when(reconnectStateSupplier.get()).thenReturn(new ReconnectState(false, indexReqId));

    byte[] reconnectData = delegate.createExtendedReconnectData();
    ReconnectState decodedReconnectState = new ReconnectCodec().decode(reconnectData);

    assertFalse(decodedReconnectState.sendChangeEvents());
    assertThat(decodedReconnectState.getStableClientId(), Matchers.is(indexReqId));
  }
}
