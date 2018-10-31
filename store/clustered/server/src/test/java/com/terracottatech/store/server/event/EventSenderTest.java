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
package com.terracottatech.store.server.event;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.MessageCodecException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class EventSenderTest {
  @Mock
  private ClientCommunicator clientCommunicator;

  @Mock
  private ClientDescriptor client1;

  @Mock
  private ClientDescriptor client2;

  @Mock
  private DatasetEntityResponse event1;

  @Test
  public void noClients() throws MessageCodecException {
    EventSender eventSender = new EventSender(clientCommunicator);
    eventSender.sendToAll(event1);

    verify(clientCommunicator, never()).sendNoResponse(any(), any());
  }

  @Test
  public void oneChangeClient() throws MessageCodecException {
    EventSender eventSender = new EventSender(clientCommunicator);
    eventSender.sendChangeEvents(client1, true);
    eventSender.sendToAll(event1);

    verify(clientCommunicator).sendNoResponse(client1, event1);
  }

  @Test
  public void twoClientsOneDisconnected() throws MessageCodecException {
    EventSender eventSender = new EventSender(clientCommunicator);
    eventSender.sendChangeEvents(client1, true);
    eventSender.disconnected(client2);
    eventSender.sendToAll(event1);

    verify(clientCommunicator).sendNoResponse(client1, event1);
  }

  @Test
  public void subscribeThenUnsubscribeFromChange() throws Exception {
    EventSender eventSender = new EventSender(clientCommunicator);
    eventSender.sendChangeEvents(client1, true);
    eventSender.sendChangeEvents(client1, false);
    eventSender.sendToAll(event1);

    verify(clientCommunicator, never()).sendNoResponse(client1, event1);
  }
}
