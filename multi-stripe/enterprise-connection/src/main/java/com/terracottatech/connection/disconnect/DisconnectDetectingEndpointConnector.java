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

import com.terracotta.connection.EndpointConnector;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;

public class DisconnectDetectingEndpointConnector implements EndpointConnector {
  private final DisconnectListener disconnectListener;

  public DisconnectDetectingEndpointConnector(DisconnectListener disconnectListener) {
    this.disconnectListener = disconnectListener;
  }

  @Override
  public <T extends Entity, C, M extends EntityMessage, R extends EntityResponse, U> T connect(EntityClientEndpoint<M, R> endpoint, EntityClientService<T, C, M, R, U> entityClientService, U userData) {
    EntityClientEndpoint<M, R> disconnectDetectingEndpoint = new DisconnectDetectingEndpoint<M, R>(endpoint, disconnectListener);
    return entityClientService.create(disconnectDetectingEndpoint, userData);
  }
}
