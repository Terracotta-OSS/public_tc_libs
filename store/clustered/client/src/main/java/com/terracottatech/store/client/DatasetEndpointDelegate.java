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

import com.terracottatech.store.client.stream.RootRemoteRecordStream;
import com.terracottatech.store.common.messages.event.ChangeEventResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.reconnect.ReconnectCodec;
import com.terracottatech.store.common.reconnect.ReconnectState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EndpointDelegate;

import java.util.Set;
import java.util.function.Supplier;

public class DatasetEndpointDelegate<K extends Comparable<K>> implements EndpointDelegate<DatasetEntityResponse> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetEndpointDelegate.class);

  private final ChangeEventManager<K> changeEventManager;
  private final Supplier<ReconnectState> reconnectStateSupplier;
  private final Set<RootRemoteRecordStream<K>> openStreams;
  private final Runnable disconnected;

  public DatasetEndpointDelegate(ChangeEventManager<K> changeEventManager,
                                 Supplier<ReconnectState> reconnectStateSupplier,
                                 Set<RootRemoteRecordStream<K>> openStreams, Runnable disconnected) {
    this.changeEventManager = changeEventManager;
    this.reconnectStateSupplier = reconnectStateSupplier;
    this.openStreams = openStreams;
    this.disconnected = disconnected;
  }

  @Override
  public void handleMessage(DatasetEntityResponse datasetEntityResponse) {
    DatasetEntityResponseType responseType = datasetEntityResponse.getType();

    switch (responseType) {
      case CHANGE_EVENT_RESPONSE:
        changeEventManager.changeEvent((ChangeEventResponse<?>) datasetEntityResponse);
        break;
      default:
        LOGGER.error("Received unexpected message type: " + responseType);
        break;
    }
  }

  @Override
  public byte[] createExtendedReconnectData() {
    ReconnectState reconnectState = reconnectStateSupplier.get();
    ReconnectCodec codec = new ReconnectCodec();
    this.openStreams.stream().forEach(RootRemoteRecordStream::failoverOccurred);
    return codec.encode(reconnectState);
  }

  @Override
  public void didDisconnectUnexpectedly() {
    try {
      changeEventManager.missedEvents();
    } finally {
      disconnected.run();
    }
  }
}
