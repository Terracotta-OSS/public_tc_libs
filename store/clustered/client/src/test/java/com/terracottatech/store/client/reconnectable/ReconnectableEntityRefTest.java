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

import com.terracottatech.store.client.reconnectable.ReconnectController.ThrowingSupplier;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests basic functionality of {@link ReconnectableEntityRef}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReconnectableEntityRefTest {

  @Mock
  private ReconnectController reconnectController;

  @Mock
  private EntityRef<TestEntity, Object, Object> mockEntityRef;

  @Captor
  private ArgumentCaptor<Object> argumentCaptor;

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
    ReconnectableEntityRef<TestEntity, Object, Object> reconnectableEntityRef =
        new ReconnectableEntityRef<>(mockEntityRef, reconnectController, refreshData);
    verifyZeroInteractions(mockEntityRef);
    verifyZeroInteractions(reconnectController);

    assertThat(reconnectableEntityRef.getRefreshData(), is(sameInstance(refreshData)));
  }

  @Test
  public void testGetName() {
    Object refreshData = mock(Object.class);
    ReconnectableEntityRef<TestEntity, Object, Object> reconnectableEntityRef =
        new ReconnectableEntityRef<>(mockEntityRef, reconnectController, refreshData);
    reconnectableEntityRef.getName();
    verify(mockEntityRef).getName();
    verifyZeroInteractions(reconnectController);
  }

  @Test
  public void testFetchEntity() throws Exception {
    TestEntity realEntity = new TestEntity();
    when(mockEntityRef.fetchEntity(any())).thenReturn(realEntity);

    Object refreshData = mock(Object.class);
    ReconnectableEntityRef<TestEntity, Object, Object> reconnectableEntityRef =
        new ReconnectableEntityRef<>(mockEntityRef, reconnectController, refreshData);

    Object userData = mock(Object.class);
    Entity entity = reconnectableEntityRef.fetchEntity(userData);
    assertNotNull(entity);

    assertThat(entity, is(instanceOf(Proxy.class)));
    assertThat(Proxy.getInvocationHandler(entity), is(instanceOf(ReconnectableProxyEntity.class)));

    verify(mockEntityRef).fetchEntity(argumentCaptor.capture());
    assertThat(argumentCaptor.getValue(), is(sameInstance(userData)));
  }

  @Test
  public void testCreate() throws Exception {
    Object refreshData = mock(Object.class);
    ReconnectableEntityRef<TestEntity, Object, Object> reconnectableEntityRef =
        new ReconnectableEntityRef<>(mockEntityRef, reconnectController, refreshData);

    Object config = mock(Object.class);
    reconnectableEntityRef.create(config);

    verify(mockEntityRef).create(argumentCaptor.capture());
    assertThat(argumentCaptor.getValue(), is(config));
  }

  @Test
  public void testReconfigure() throws Exception {
    Object refreshData = mock(Object.class);
    ReconnectableEntityRef<TestEntity, Object, Object> reconnectableEntityRef =
        new ReconnectableEntityRef<>(mockEntityRef, reconnectController, refreshData);

    Object config = mock(Object.class);
    reconnectableEntityRef.reconfigure(config);

    verify(mockEntityRef).reconfigure(argumentCaptor.capture());
    assertThat(argumentCaptor.getValue(), is(config));
  }

  @Test
  public void testDestroy() throws Exception {
    Object refreshData = mock(Object.class);
    ReconnectableEntityRef<TestEntity, Object, Object> reconnectableEntityRef =
        new ReconnectableEntityRef<>(mockEntityRef, reconnectController, refreshData);

    reconnectableEntityRef.destroy();

    verify(mockEntityRef).destroy();
  }

  @Test
  public void testSwap() throws Exception {
    TestEntity firstRealEntity = new TestEntity();
    when(mockEntityRef.fetchEntity(any())).thenReturn(firstRealEntity);

    Object refreshData = mock(Object.class);
    ReconnectableEntityRef<TestEntity, Object, Object> reconnectableEntityRef =
        new ReconnectableEntityRef<>(mockEntityRef, reconnectController, refreshData);

    Object userData = mock(Object.class);
    Entity entity = reconnectableEntityRef.fetchEntity(userData);
    assertNotNull(entity);

    /*
     * Invoke swap ...
     */
    @SuppressWarnings("unchecked") EntityRef<TestEntity, Object, Object> secondEntityRef = mock(EntityRef.class);
    TestEntity secondRealEntity = new TestEntity();
    when(secondEntityRef.fetchEntity(any())).thenReturn(secondRealEntity);

    reconnectableEntityRef.swap(secondEntityRef);

    verify(mockEntityRef).fetchEntity(any());
    verify(secondEntityRef).fetchEntity(any());

    reconnectableEntityRef.getName();
    verify(mockEntityRef, never()).getName();
    verify(secondEntityRef).getName();
  }

  private static class TestEntity implements Entity {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @Override
    public void close() {
      closed.set(true);
    }
  }
}