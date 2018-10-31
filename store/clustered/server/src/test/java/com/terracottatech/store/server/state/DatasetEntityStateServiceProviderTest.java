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
package com.terracottatech.store.server.state;

import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.server.execution.ExecutionService;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import org.junit.Test;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;

import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatasetEntityStateServiceProviderTest {

  @SuppressWarnings("unchecked")
  @Test
  public <T extends Comparable<T>> void testGetService() throws ServiceException {
    DatasetEntityStateServiceProvider serviceProvider = new DatasetEntityStateServiceProvider();

    ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
    ExecutionService executionService = mock(ExecutionService.class);
    StorageFactory storageFactory = mock(StorageFactory.class);

    DatasetEntityConfiguration<T> datasetEntityConfiguration = mock(DatasetEntityConfiguration.class);
    when(datasetEntityConfiguration.getDatasetName()).thenReturn("test");
    when(datasetEntityConfiguration.getDatasetConfiguration()).thenReturn(mock(ClusteredDatasetConfiguration.class));

    when(executionService.getUnorderedExecutor(anyString())).thenReturn(Executors.newSingleThreadExecutor());
    when(serviceRegistry.getService(any(ServiceConfiguration.class))).thenAnswer(args -> {
      ServiceConfiguration<?> serviceConfiguration = args.getArgument(0);
      if (serviceConfiguration.getServiceType().equals(ExecutionService.class)) {
        return executionService;
      } else if (serviceConfiguration.getServiceType().equals(StorageFactory.class)) {
        return storageFactory;
      }
      return null;
    });

    DatasetEntityStateServiceConfig<T> entityStateServiceConfig = new DatasetEntityStateServiceConfig<>(datasetEntityConfiguration, serviceRegistry);
    DatasetEntityStateService<T> datasetEntityStateService = serviceProvider.getService(1L, entityStateServiceConfig);

    assertThat(datasetEntityStateService, notNullValue());

    DatasetEntityStateService<T> anotherLookup = serviceProvider.getService(1L, entityStateServiceConfig);

    assertThat(datasetEntityStateService, sameInstance(anotherLookup));

    when(datasetEntityConfiguration.getDatasetName()).thenReturn("testDifferent");
    DatasetEntityStateService<T> differentLookup = serviceProvider.getService(2L, entityStateServiceConfig);

    assertNotSame(datasetEntityStateService, differentLookup);
  }


}