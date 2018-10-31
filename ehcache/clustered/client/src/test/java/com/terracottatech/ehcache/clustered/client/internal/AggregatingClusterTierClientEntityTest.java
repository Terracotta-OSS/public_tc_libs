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
package com.terracottatech.ehcache.clustered.client.internal;

import org.ehcache.clustered.client.internal.store.InternalClusterTierClientEntity;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.KeyBasedServerStoreOpMessage;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.terracottatech.entity.AggregateEndpoint;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AggregatingClusterTierClientEntityTest
 */
public class AggregatingClusterTierClientEntityTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  AggregateEndpoint<InternalClusterTierClientEntity> endpoint;

  @Mock
  InternalClusterTierClientEntity clientEntity1;

  @Mock
  InternalClusterTierClientEntity clientEntity2;

  @Mock
  KeyBasedServerStoreOpMessage message = mock(KeyBasedServerStoreOpMessage.class);

  @Test
  public void testRoutesInvokeProperly() throws Exception {
    when(endpoint.getEntities()).thenReturn(asList(clientEntity1, clientEntity2));

    AggregatingClusterTierClientEntity entity = new AggregatingClusterTierClientEntity(endpoint);

    when(message.getKey()).thenReturn(2L, -9223372036854775808L);
    entity.invokeAndWaitForComplete(message, false);

    verify(clientEntity1).invokeAndWaitForComplete(eq(message), eq(false));
    reset(clientEntity1);

    entity.invokeAndWaitForComplete(message, true);
    verify(clientEntity2).invokeAndWaitForComplete(eq(message), eq(true));
  }

}
