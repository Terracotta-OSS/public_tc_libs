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
package com.terracottatech.connection;

import com.tc.util.Assert;
import com.terracottatech.entity.AltEntity;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.lease.connection.LeasedConnection;

/**
 *
 */
public class BasicMultiConnectionTest {

  Connection connection;
  LeasedConnection stripe1;
  LeasedConnection stripe2;
  EntityRef<SystemCatalog, Object, Object> systemCatalogRef;
  SystemCatalog catalog;

  Entity stripe1Entity;
  Entity stripe2Entity;


  public BasicMultiConnectionTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    List<Connection> connections = new ArrayList<>();
    stripe1 = Mockito.mock(LeasedConnection.class);
    stripe2 = Mockito.mock(LeasedConnection.class);
    connections.add(stripe1);
    connections.add(stripe2);
    systemCatalogRef = mock(EntityRef.class);
    catalog = mock(SystemCatalog.class);
    when(catalog.tryLock(ArgumentMatchers.any(Class.class), ArgumentMatchers.anyString())).thenReturn(Boolean.TRUE);
    when(catalog.unlock(ArgumentMatchers.any(Class.class), ArgumentMatchers.anyString())).thenReturn(Boolean.TRUE);
    when(catalog.getConfiguration(ArgumentMatchers.any(Class.class), ArgumentMatchers.anyString())).thenReturn(new byte[]{});
    when(systemCatalogRef.fetchEntity(null)).thenReturn(catalog);
    when(stripe1.getEntityRef(ArgumentMatchers.eq(SystemCatalog.class), ArgumentMatchers.eq(SystemCatalog.VERSION), ArgumentMatchers.eq(SystemCatalog.ENTITY_NAME))).thenReturn(systemCatalogRef);
    EntityRef<Entity, ?, ?> er1 = mock(EntityRef.class);
    EntityRef<Entity, ?, ?> er2 = mock(EntityRef.class);
    stripe1Entity = mock(Entity.class);
    stripe2Entity = mock(Entity.class);
    when(er1.fetchEntity(null)).thenReturn(stripe1Entity);
    when(er2.fetchEntity(null)).thenReturn(stripe2Entity);
    when(stripe1.getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test"))).thenReturn(er1);
    when(stripe2.getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test"))).thenReturn(er2);
    connection = new BasicMultiConnection(connections);
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of getEntityRef method, of class BasicMultiConnection.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testGetEntityRef() throws Exception {
    System.out.println("getEntityRef");
    EntityRef<Entity, Object, Void> result = connection.getEntityRef(Entity.class,1L,"test");
    verify(stripe1, never()).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test"));
    verify(stripe2, never()).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test"));
    Entity item = result.fetchEntity(null);
    verify(stripe1).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(SystemCatalog.VERSION), ArgumentMatchers.eq(SystemCatalog.ENTITY_NAME));
    verify(stripe2, Mockito.never()).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(SystemCatalog.VERSION), ArgumentMatchers.eq(SystemCatalog.ENTITY_NAME));
    verify(stripe1).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test"));
    verify(stripe2).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test"));
    verify(catalog).getConfiguration(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq("test"));
  }

  /**
   * Test of close method, of class BasicMultiConnection.
   */
  @Test
  public void testClose() throws Exception {
    EntityRef<Entity, Object, Void> result = connection.getEntityRef(Entity.class,1L,"test");
    Entity item = result.fetchEntity(null);
    item.close();
    verify(stripe1Entity).close();
    verify(stripe2Entity).close();
  }
  /**
   * Test of close method, of class BasicMultiConnection.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testSelectiveStripes() throws Exception {
    EntityRef<AltEntity, ?, ?> ref1 = mock(EntityRef.class);
    EntityRef<AltEntity, ?, ?> ref2 = mock(EntityRef.class);
    when(ref1.fetchEntity(null)).thenReturn(mock(AltEntity.class));
    when(ref2.fetchEntity(null)).thenReturn(mock(AltEntity.class));

    when(stripe1.getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test2"))).thenReturn(ref1);
    when(stripe2.getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test2"))).thenReturn(ref2);
    EntityRef<AltEntity, Object, Void> result = connection.getEntityRef(AltEntity.class,1L,"test2");
    AltEntity item = result.fetchEntity(null);
    verify(stripe1).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test2"));
    verify(stripe2, never()).getEntityRef(ArgumentMatchers.any(Class.class), ArgumentMatchers.eq(1L), ArgumentMatchers.eq("test2"));
    Assert.assertEquals(1, item.getStripeCount());
  }
}
