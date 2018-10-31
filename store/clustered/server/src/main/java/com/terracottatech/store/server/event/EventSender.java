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
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;

import java.util.concurrent.ConcurrentHashMap;

public class EventSender {
  private final ClientCommunicator clientCommunicator;
  private final ConcurrentHashMap<ClientDescriptor, ClientEventSender> clientEventSenders = new ConcurrentHashMap<>();

  public EventSender(ClientCommunicator clientCommunicator) {
    this.clientCommunicator = clientCommunicator;
  }

  public void disconnected(ClientDescriptor clientDescriptor) {
    clientEventSenders.remove(clientDescriptor);
  }

  public void sendToAll(DatasetEntityResponse event) {
    clientEventSenders.values().forEach(clientEventSender -> clientEventSender.sendEvent(event));
  }

  public void sendChangeEvents(ClientDescriptor clientDescriptor, boolean sendChangeEvents) {
    clientEventSenders.compute(clientDescriptor, (client, sender) -> {
      if (sender == null) {
        return ClientEventSender.forChangeEvents(clientCommunicator, clientDescriptor, sendChangeEvents);
      } else {
        return sender.withChangeEvents(sendChangeEvents);
      }
    });
  }
}
