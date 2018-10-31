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
import org.terracotta.entity.MessageCodecException;

public class ClientEventSender {
  private final ClientCommunicator clientCommunicator;
  private final ClientDescriptor clientDescriptor;

  private ClientEventSender(ClientCommunicator clientCommunicator, ClientDescriptor clientDescriptor) {
    this.clientCommunicator = clientCommunicator;
    this.clientDescriptor = clientDescriptor;
  }

  public void sendEvent(DatasetEntityResponse event) {
    try {
      clientCommunicator.sendNoResponse(clientDescriptor, event);
    } catch (MessageCodecException e) {
      throw new RuntimeException(e);
    }
  }

  public static ClientEventSender forChangeEvents(ClientCommunicator clientCommunicator, ClientDescriptor clientDescriptor, boolean sendChangeEvents) {
    if (sendChangeEvents) {
      return new ClientEventSender(clientCommunicator, clientDescriptor);
    } else {
      return null;
    }
  }

  public ClientEventSender withChangeEvents(boolean sendChangeEvents) {
    if (sendChangeEvents) {
      return new ClientEventSender(clientCommunicator, clientDescriptor);
    } else {
      return null;
    }
  }
}
