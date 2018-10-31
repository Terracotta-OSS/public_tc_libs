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
package com.terracottatech.connection.disconnect;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityResponse;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DisconnectDetectingDelegateTest {
  @Mock
  private DisconnectListener listener;

  @Mock
  private EndpointDelegate<EntityResponse> underlying;

  @Mock
  private EntityResponse entityResponse;

  private DisconnectDetectingDelegate<EntityResponse> delegate;

  @Before
  public void before() {
    delegate = new DisconnectDetectingDelegate<>(listener);
  }

  @Test
  public void nothingHappensWithoutPrompting() {
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void disconnectIsNotifiedToListener() {
    delegate.didDisconnectUnexpectedly();
    verify(listener).disconnected();
  }

  @Test
  public void noErrorWithHandleMessageAndNoUnderlying() {
    delegate.handleMessage(entityResponse);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void emptyReconnectDataWithNoUnderlying() {
    assertArrayEquals(new byte[0], delegate.createExtendedReconnectData());
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void disconnectForwardedToUnderlyingAndListener() {
    delegate.setUnderlying(underlying);
    delegate.didDisconnectUnexpectedly();

    verify(underlying).didDisconnectUnexpectedly();
    verify(listener).disconnected();
  }

  @Test
  public void handleMessageForwardedToUnderlying() {
    delegate.setUnderlying(underlying);
    delegate.handleMessage(entityResponse);

    verify(underlying).handleMessage(entityResponse);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void reconnectDataForwardedToUnderlying() {
    byte[] bytes = new byte[] { 1 };
    when(underlying.createExtendedReconnectData()).thenReturn(bytes);

    delegate.setUnderlying(underlying);
    assertArrayEquals(bytes, delegate.createExtendedReconnectData());

    verify(underlying).createExtendedReconnectData();
    verifyNoMoreInteractions(listener);
  }

  @Test(expected = IllegalStateException.class)
  public void closePreventsSettingUnderlying() {
    delegate.close();
    delegate.setUnderlying(underlying);
  }
}
