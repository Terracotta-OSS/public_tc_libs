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

import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.ClientSideConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;

import java.io.IOException;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusteredDatasetManagerTest {
  @Mock
  Connection connection;

  @SuppressWarnings("rawtypes")
  @Mock
  EntityRef<DatasetEntity, Object, Object> entityRef;

  @Mock
  DatasetEntity<String> entity;

  @Mock
  SystemCatalog systemCatalog;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings({"try", "unused"})
  @Test
  public void closesConnection() throws Exception {
    try (ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class))) {
    }

    verify(connection).close();
  }

  @Test
  public void entityOnlyClosedIfDatasetClosed() throws Exception {
    new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    verify(connection, never()).close();
  }

  @Test
  public void connectionCloseExceptionsPropagate() throws Exception {
    doThrow(IOException.class).when(connection).close();

    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    try {
      datasetManager.close();
      fail("No exception from close() when connection.close() threw an exception");
    } catch (StoreRuntimeException e) {
      // Expected exception
    }

    verify(connection).close();
  }

  @Test
  public void createsDataset() throws Exception {
    InOrder inOrder = inOrder(connection, entityRef, entity);

    String simpleName = "name";
    String datasetName = "name";
    when(connection.getEntityRef(DatasetEntity.class, 1, datasetName)).thenReturn(this.entityRef);
    when(entityRef.fetchEntity(null)).thenReturn(entity);
    when(entity.getKeyType()).thenReturn(Type.STRING);

    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    assertThat(datasetManager.newDataset(simpleName, Type.STRING, datasetManager.datasetConfiguration().offheap("offheapResource").build()), is(true));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<DatasetEntityConfiguration<?>> argumentCaptor = ArgumentCaptor.forClass(DatasetEntityConfiguration.class);
    inOrder.verify(connection).getEntityRef(DatasetEntity.class, 1, datasetName);
    inOrder.verify(entityRef).create(argumentCaptor.capture());
    inOrder.verifyNoMoreInteractions();

    DatasetEntityConfiguration<?> configuration = argumentCaptor.getValue();
    assertEquals("name", configuration.getDatasetName());
    assertEquals(Type.STRING, configuration.getKeyType());
    assertEquals("offheapResource", configuration.getDatasetConfiguration().getOffheapResource());
    assertFalse(configuration.getDatasetConfiguration().getDiskResource().isPresent());
  }

  @Test
  public void getsDataset() throws Exception {
    InOrder inOrder = inOrder(connection, entity);

    when(connection.getEntityRef(DatasetEntity.class, 1, "name")).thenReturn(entityRef);
    when(entityRef.fetchEntity(null)).thenReturn(entity);
    when(entity.getKeyType()).thenReturn(Type.STRING);

    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    datasetManager.getDataset("name", Type.STRING);

    inOrder.verify(connection).getEntityRef(DatasetEntity.class, 1, "name");
    inOrder.verify(entity).getKeyType();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void getDatasetThatDoesNotExistsFails() throws Exception {
    when(connection.getEntityRef(DatasetEntity.class, 1, "name")).thenReturn(entityRef);
    when(entityRef.fetchEntity(null)).thenThrow(new EntityNotFoundException("DatasetEntity", "name"));
    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    try {
      datasetManager.getDataset("name", Type.STRING);
      fail("Exception expected");
    } catch (DatasetMissingException e) {
      // Test success
    }
  }

  @Test
  public void getDatasetWithWrongTypeFailsAndClosesEntity() throws Exception {
    when(connection.getEntityRef(DatasetEntity.class, 1, "name")).thenReturn(entityRef);
    when(entityRef.fetchEntity(null)).thenReturn(entity);
    when(entity.getKeyType()).thenReturn(Type.STRING);

    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    try {
      datasetManager.getDataset("name", Type.INT);
      fail("Expected DatasetKeyTypeMismatchException");
    } catch (DatasetKeyTypeMismatchException e) {
      //expected
    }

    verify(entity).close();
  }

  @Test
  public void destroysDataset() throws Exception {
    InOrder inOrder = inOrder(connection, entityRef, entity);

    when(connection.getEntityRef(DatasetEntity.class, 1, "name")).thenReturn(entityRef);
    when(entityRef.destroy()).thenReturn(true);

    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    datasetManager.destroyDataset("name");

    inOrder.verify(connection).getEntityRef(DatasetEntity.class, 1, "name");
    inOrder.verify(entityRef).destroy();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void memoryConfiguration() {
    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    ClusteredDatasetConfiguration datasetConfiguration = (ClusteredDatasetConfiguration) datasetManager.datasetConfiguration().offheap("resource1").build();
    assertEquals("resource1", datasetConfiguration.getOffheapResource());
    assertFalse(datasetConfiguration.getDiskResource().isPresent());
  }

  @Test
  public void diskConfiguration() {
    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    ClusteredDatasetConfiguration datasetConfiguration = (ClusteredDatasetConfiguration) datasetManager.datasetConfiguration().offheap("resource1").disk("resource2").build();
    assertEquals("resource1", datasetConfiguration.getOffheapResource());
    assertEquals("resource2", datasetConfiguration.getDiskResource().get());
  }

  @Test(expected = IllegalStateException.class)
  public void noOffheapConfiguration() {
    ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class));
    datasetManager.datasetConfiguration().disk("resource2").build();
  }

  @Test
  public void listDatasetsClosesSystemCatalog()  throws Exception {
    try (ClusteredDatasetManager datasetManager = new ClusteredDatasetManager(connection, mock(ClientSideConfiguration.class))) {
      @SuppressWarnings("unchecked")
      EntityRef<SystemCatalog, Object, Object> catalogRef = mock(EntityRef.class);
      when(connection.getEntityRef(SystemCatalog.class, SystemCatalog.VERSION, SystemCatalog.ENTITY_NAME)).thenReturn(catalogRef);
      when(catalogRef.fetchEntity(null)).thenReturn(systemCatalog);
      datasetManager.listDatasets();
    }

    verify(systemCatalog).close();
  }
}
