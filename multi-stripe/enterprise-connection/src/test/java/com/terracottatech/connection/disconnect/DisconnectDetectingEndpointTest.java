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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvocationBuilder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DisconnectDetectingEndpointTest {
  @Mock
  private DisconnectListener listener;

  @Mock
  private EndpointDelegate<EntityResponse> delegate;

  @Mock
  private InvocationBuilder<EntityMessage, EntityResponse> builder;

  private EntityClientEndpoint<EntityMessage, EntityResponse> underlying;
  private DisconnectDetectingEndpoint<EntityMessage, EntityResponse> endpoint;
  private DisconnectDetectingDelegate<?> disconnectDelegate;

  @SuppressWarnings("unchecked")
  @Before
  public void before() {
    underlying = mock(EntityClientEndpoint.class);
    ArgumentCaptor<EndpointDelegate<EntityResponse>> delegateCaptor = ArgumentCaptor
            .forClass(EndpointDelegate.class);
    doNothing().when(underlying).setDelegate(delegateCaptor.capture());
    endpoint = new DisconnectDetectingEndpoint<>(underlying, listener);
    verify(underlying).setDelegate(any(DisconnectDetectingDelegate.class));
    disconnectDelegate = (DisconnectDetectingDelegate) delegateCaptor.getValue();
    assertNotNull(disconnectDelegate);
  }

  @Test
  public void nothingHappensWithoutPrompting() {
    // Nothing in the body of the test, but the before and after will run
  }

  @Test
  public void disconnectFiresEventToListener() {
    disconnectDelegate.didDisconnectUnexpectedly();
    verify(listener).disconnected();
  }

  @Test
  public void forwardsBeginInvoke() {
    when(underlying.beginInvoke()).thenReturn(builder);
    assertEquals(builder, endpoint.beginInvoke());
    verify(underlying).beginInvoke();
  }

  @Test
  public void forwardsGetEntityConfiguration() {
    byte[] bytes = new byte[] { 1 };
    when(underlying.getEntityConfiguration()).thenReturn(bytes);
    assertArrayEquals(bytes, endpoint.getEntityConfiguration());
    verify(underlying).getEntityConfiguration();
  }

  @Test
  public void forwardsClose() {
    endpoint.close();
    verify(underlying).close();
  }

  @Test(expected = IllegalStateException.class)
  public void closeBlocksSetDelegate() {
    endpoint.close();
    verify(underlying).close();
    endpoint.setDelegate(delegate);
  }

  @Test
  public void afterSettingADelegateListenerStillGetsDisconnectedEvents() {
    endpoint.setDelegate(delegate);
    disconnectDelegate.didDisconnectUnexpectedly();
    verify(delegate).didDisconnectUnexpectedly();
    verify(listener).disconnected();
  }

  @After
  public void after() {
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(underlying);
    verifyNoMoreInteractions(delegate);
  }
}
