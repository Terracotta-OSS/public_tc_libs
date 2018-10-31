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
package com.terracottatech.store.client.reconnectable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;

import com.terracottatech.store.Type;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.client.reconnectable.ReconnectController.ThrowingSupplier;
import com.terracottatech.store.common.DatasetEntityConfiguration;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests basic functionality of {@link ReconnectableDatasetEntityRef}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReconnectableDatasetEntityRefTest {

  @Mock
  private ReconnectController reconnectController;

  @Mock
  private EntityRef<DatasetEntity<String>, DatasetEntityConfiguration<String>, DatasetEntity.Parameters> mockEntityRef;

  @Captor
  private ArgumentCaptor<DatasetEntity.Parameters> parametersCaptor;

  @Captor
  private ArgumentCaptor<DatasetEntityConfiguration<String>> configCaptor;

  @Before
  public void setUp() throws Exception {
    when(reconnectController.withConnectionClosedHandlingExceptionally(any()))
        .thenAnswer(invocation -> {
          ThrowingSupplier<Object> supplier = invocation.getArgument(0);
          return supplier.get();
        });
  }

  @Test
  public void testConstruction() {
    Object refreshData = mock(Object.class);
    ReconnectableDatasetEntityRef<String> reconnectableEntityRef =
        new ReconnectableDatasetEntityRef<>(mockEntityRef, reconnectController, refreshData);
    verifyZeroInteractions(mockEntityRef);
    verifyZeroInteractions(reconnectController);

    assertThat(reconnectableEntityRef.getRefreshData(), is(sameInstance(refreshData)));
  }

  @Test
  public void testGetName() {
    Object refreshData = mock(Object.class);
    ReconnectableDatasetEntityRef<String> reconnectableEntityRef =
        new ReconnectableDatasetEntityRef<>(mockEntityRef, reconnectController, refreshData);
    reconnectableEntityRef.getName();
    verify(mockEntityRef).getName();
    verifyZeroInteractions(reconnectController);
  }

  @Test
  public void testFetchEntity() throws Exception {
    TestDatasetEntity<String> realEntity = new TestDatasetEntity<>(Type.STRING);
    when(mockEntityRef.fetchEntity(any())).thenReturn(realEntity);

    Object refreshData = mock(Object.class);
    ReconnectableDatasetEntityRef<String> reconnectableEntityRef =
        new ReconnectableDatasetEntityRef<>(mockEntityRef, reconnectController, refreshData);

    Entity entity = reconnectableEntityRef.fetchEntity(null);
    assertNotNull(entity);

    assertThat(entity, is(instanceOf(ReconnectableDatasetEntity.class)));

    verify(mockEntityRef).fetchEntity(parametersCaptor.capture());
    assertThat(parametersCaptor.getValue(), is(instanceOf(ReconnectableDatasetEntity.Parameters.class)));
    assertThat(((ReconnectableDatasetEntity.Parameters)parametersCaptor.getValue()).reconnectController(), is(reconnectController));
    assertThat(((ReconnectableDatasetEntity.Parameters)parametersCaptor.getValue()).unwrap(), is(nullValue()));
  }

  @Test
  public void testCreate() throws Exception {
    Object refreshData = mock(Object.class);
    ReconnectableDatasetEntityRef<String> reconnectableEntityRef =
        new ReconnectableDatasetEntityRef<>(mockEntityRef, reconnectController, refreshData);

    @SuppressWarnings("unchecked") DatasetEntityConfiguration<String> config = mock(DatasetEntityConfiguration.class);
    reconnectableEntityRef.create(config);

    verify(mockEntityRef).create(configCaptor.capture());
    assertThat(configCaptor.getValue(), is(config));
  }

  @Test
  public void testReconfigure() throws Exception {
    Object refreshData = mock(Object.class);
    ReconnectableDatasetEntityRef<String> reconnectableEntityRef =
        new ReconnectableDatasetEntityRef<>(mockEntityRef, reconnectController, refreshData);

    @SuppressWarnings("unchecked") DatasetEntityConfiguration<String> config = mock(DatasetEntityConfiguration.class);
    reconnectableEntityRef.reconfigure(config);

    verify(mockEntityRef).reconfigure(configCaptor.capture());
    assertThat(configCaptor.getValue(), is(config));
  }

  @Test
  public void testDestroy() throws Exception {
    Object refreshData = mock(Object.class);
    ReconnectableDatasetEntityRef<String> reconnectableEntityRef =
        new ReconnectableDatasetEntityRef<>(mockEntityRef, reconnectController, refreshData);

    reconnectableEntityRef.destroy();

    verify(mockEntityRef).destroy();
  }

  @Test
  public void testSwap() throws Exception {
    TestDatasetEntity<String> firstRealEntity = new TestDatasetEntity<>(Type.STRING);
    when(mockEntityRef.fetchEntity(any())).thenReturn(firstRealEntity);

    Object refreshData = mock(Object.class);
    ReconnectableDatasetEntityRef<String> reconnectableEntityRef =
        new ReconnectableDatasetEntityRef<>(mockEntityRef, reconnectController, refreshData);

    Entity entity = reconnectableEntityRef.fetchEntity(null);
    assertNotNull(entity);

    /*
     * Invoke swap ...
     */
    @SuppressWarnings("unchecked") EntityRef<DatasetEntity<String>, DatasetEntityConfiguration<String>, DatasetEntity.Parameters> secondEntityRef = mock(EntityRef.class);
    TestDatasetEntity<String> secondRealEntity = new TestDatasetEntity<>(Type.STRING);
    when(secondEntityRef.fetchEntity(any())).thenReturn(secondRealEntity);

    reconnectableEntityRef.swap(secondEntityRef);

    verify(mockEntityRef).fetchEntity(any());
    verify(secondEntityRef).fetchEntity(any());

    reconnectableEntityRef.getName();
    verify(mockEntityRef, never()).getName();
    verify(secondEntityRef).getName();
  }
}