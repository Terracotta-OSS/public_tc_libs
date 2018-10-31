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

import com.terracottatech.entity.AggregateEndpoint;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AggregatingDatasetEntityTest
 */
@SuppressWarnings("unchecked")
public class AggregatingDatasetEntityTest {

  @Test
  public void testRouteInvokesProperly() {
    DatasetEntity<Long> entity1 = mock(DatasetEntity.class);
    DatasetEntity<Long> entity2 = mock(DatasetEntity.class);

    AggregateEndpoint<DatasetEntity<Long>> endpoint = mock(AggregateEndpoint.class);
    when(endpoint.getEntities()).thenReturn(asList(entity1, entity2));

    AggregatingDatasetEntity<Long> aggregateEntity = new AggregatingDatasetEntity<>(endpoint);
    aggregateEntity.get(2L);
    verify(entity2).get(2L);

    aggregateEntity.get(4L);
    verify(entity1).get(4L);
  }

  @Test
  public void testClose() throws Exception {
    DatasetEntity<Long> entity1 = mock(DatasetEntity.class);
    DatasetEntity<Long> entity2 = mock(DatasetEntity.class);

    AggregateEndpoint<DatasetEntity<Long>> endpoint = mock(AggregateEndpoint.class);
    when(endpoint.getEntities()).thenReturn(asList(entity1, entity2));

    AggregatingDatasetEntity<Long> aggregateEntity = new AggregatingDatasetEntity<>(endpoint);

    aggregateEntity.close();
    verify(entity1).close();
    verify(entity2).close();
  }

  @Test
  public void testCloseThrowsDoesCloseAllUnderlying() throws Exception {
    DatasetEntity<Long> entity1 = mock(DatasetEntity.class);
    DatasetEntity<Long> entity2 = mock(DatasetEntity.class);
    doThrow(new IllegalArgumentException("boom 1!")).when(entity1).close();
    doThrow(new IllegalArgumentException("boom 2!")).when(entity2).close();

    AggregateEndpoint<DatasetEntity<Long>> endpoint = mock(AggregateEndpoint.class);
    when(endpoint.getEntities()).thenReturn(asList(entity1, entity2));

    AggregatingDatasetEntity<Long> aggregateEntity = new AggregatingDatasetEntity<>(endpoint);

    try {
      aggregateEntity.close();
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException re) {
      // expected
    }
    verify(entity1).close();
    verify(entity2).close();
  }
}
