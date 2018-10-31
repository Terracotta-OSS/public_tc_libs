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
package com.terracottatech.ehcache.disk.internal.frsbacked.store;

import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.core.spi.store.Store;
import org.junit.Test;

import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerService;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * RestartableOffHeapStoreProviderTest
 */
public class RestartableOffHeapStoreProviderTest {

  @Test
  public void testProviderLoadedWhenLocalPersistenceServiceAvailable() throws Exception {
    ServiceLocator serviceLocator = ServiceLocator.dependencySet()
        .with(mock(LocalPersistenceService.class))
        .with(Store.Provider.class)
        .build();

    assertThat(serviceLocator.getService(RestartableOffHeapStore.Provider.class), notNullValue());
  }

  @Test
  public void testProviderLoadsFastRestartStoreContainerService() throws Exception {
    ServiceLocator serviceLocator = ServiceLocator.dependencySet()
        .with(mock(LocalPersistenceService.class))
        .with(Store.Provider.class)
        .build();

    assertThat(serviceLocator.getService(FastRestartStoreContainerService.class), notNullValue());
  }

  @Test
  public void testProviderNotLoadedWhenLocalPersistenceServiceNotAvailable() throws Exception {
    ServiceLocator serviceLocator = ServiceLocator.dependencySet()
        .with(Store.Provider.class)
        .build();

    assertThat(serviceLocator.getService(RestartableOffHeapStore.Provider.class), nullValue());
  }

}