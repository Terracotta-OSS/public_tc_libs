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

import com.terracottatech.store.ChangeType;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.event.ChangeEventResponse;
import com.terracottatech.store.server.NamedClientDescriptor;
import com.terracottatech.store.server.RecordingClientCommunicator;
import org.junit.Test;
import org.terracotta.entity.ClientDescriptor;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClientEventSenderTest {
  private ClientDescriptor client = new NamedClientDescriptor("client1", 1);
  private RecordingClientCommunicator clientCommunicator = new RecordingClientCommunicator();

  @Test
  public void noChangeEventSender() {
    assertNull(ClientEventSender.forChangeEvents(clientCommunicator, client, false));
  }

  @Test
  public void subscribeForChangeEvents() {
    ClientEventSender eventSender = ClientEventSender.forChangeEvents(clientCommunicator, client, true);
    runTest(eventSender);
  }

  @Test
  public void switchToNoChangeEvents() {
    ClientEventSender eventSender = ClientEventSender.forChangeEvents(clientCommunicator, client, true);
    assertNull(eventSender.withChangeEvents(false));
  }

  @Test
  public void switchToSameChangeEvents() {
    ClientEventSender eventSender = ClientEventSender.forChangeEvents(clientCommunicator, client, true);
    eventSender = eventSender.withChangeEvents(true);
    runTest(eventSender);
  }

  private void runTest(ClientEventSender eventSender) {
    DatasetEntityResponse event = new ChangeEventResponse<>("key", ChangeType.ADDITION);
    eventSender.sendEvent(event);

    String response = getResponse(clientCommunicator);
    assertEquals("client1:key:ADDITION:", response);
  }

  private String getResponse(RecordingClientCommunicator clientCommunicator) {
    List<String> responses = clientCommunicator.getResponses();
    assertEquals(1, responses.size());
    return responses.get(0);
  }
}
