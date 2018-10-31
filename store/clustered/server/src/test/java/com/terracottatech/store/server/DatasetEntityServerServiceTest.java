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
package com.terracottatech.store.server;

import com.terracottatech.licensing.services.LicenseService;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.messages.ConfigurationEncoder;
import com.terracottatech.store.server.execution.ExecutionService;
import com.terracottatech.store.server.state.DatasetEntityStateService;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.platform.ServerInfo;

import java.util.concurrent.ExecutorService;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatasetEntityServerServiceTest {
  @Mock
  private ServiceRegistry serviceRegistry;

  @Mock
  private StorageFactory storageFactory;

  @Mock
  private ClientCommunicator clientCommunicator;

  @Mock
  private ExecutionService executionService;

  @Mock
  private LicenseService licenseService;

  @Mock
  private DatasetEntityStateService<?> datasetEntityStateService;

  @Before
  public void before() throws Exception {
    when(serviceRegistry.getService(any())).then(invocation -> {
      ServiceConfiguration<?> configuration = invocation.getArgument(0);
      Class<?> serviceType = configuration.getServiceType();
      if (serviceType.equals(StorageFactory.class)) {
        return storageFactory;
      }

      if (serviceType.equals(ClientCommunicator.class)) {
        return clientCommunicator;
      }

      if (serviceType.equals(ExecutionService.class)) {
        return executionService;
      }

      if (serviceType.equals(LicenseService.class)) {
        return licenseService;
      }

      if (serviceType.equals(DatasetEntityStateService.class)) {
        return datasetEntityStateService;
      }

      if (serviceType.equals(ServerInfo.class)) {
        return new ServerInfo("serverName");
      }

      return null;
    });
  }

  @Test
  public void handlesDatasetEntity() {
    DatasetEntityServerService service = new DatasetEntityServerService();
    assertTrue(service.handlesEntityType("com.terracottatech.store.client.DatasetEntity"));
    assertFalse(service.handlesEntityType("org.terracotta.connection.entity.Entity"));
  }

  @Test
  public void activeEntityCreated() throws Exception {
    DatasetEntityServerService service = new DatasetEntityServerService();
    when(executionService.getPipelineProcessorExecutor(any())).thenReturn(mock(ExecutorService.class));
    DatasetEntityConfiguration<?> configuration =
        new DatasetEntityConfiguration<>(Type.BOOL, "name", new ClusteredDatasetConfiguration("A", null, emptyMap()));
    DatasetActiveEntity<?> activeEntity = service.createActiveEntity(serviceRegistry, ConfigurationEncoder.encode(configuration));
    assertNotNull(activeEntity);
  }
}
