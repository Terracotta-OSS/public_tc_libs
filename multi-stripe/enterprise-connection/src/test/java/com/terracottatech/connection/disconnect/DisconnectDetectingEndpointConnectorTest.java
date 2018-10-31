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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DisconnectDetectingEndpointConnectorTest {
  @Mock
  private DisconnectListener listener;

  @Mock
  private EntityClientEndpoint<EntityMessage, EntityResponse> endpoint;

  @Mock
  private EntityClientService<Entity, Object, EntityMessage, EntityResponse, Object> entityCientService;

  @Mock
  private Object userData;

  @Mock
  private Entity entity;

  @Test
  public void wrapsEndpointInDisconnectDetectingEndpoint() {
    @SuppressWarnings("unchecked")
    ArgumentCaptor<EntityClientEndpoint<EntityMessage, EntityResponse>> endpointCaptor = (ArgumentCaptor) ArgumentCaptor.forClass(EntityClientEndpoint.class);
    ArgumentCaptor<Object> userDataCaptor = ArgumentCaptor.forClass(Object.class);
    when(entityCientService.create(endpointCaptor.capture(), userDataCaptor.capture())).thenReturn(entity);

    DisconnectDetectingEndpointConnector connector = new DisconnectDetectingEndpointConnector(listener);
    assertEquals(entity, connector.connect(endpoint, entityCientService, userData));

    assertEquals(userData, userDataCaptor.getValue());
    assertTrue(endpointCaptor.getValue() instanceof DisconnectDetectingEndpoint);
  }
}