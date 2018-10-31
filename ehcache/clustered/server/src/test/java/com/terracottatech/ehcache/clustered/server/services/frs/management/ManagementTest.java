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
package com.terracottatech.ehcache.clustered.server.services.frs.management;

import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.frs.RestartStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.terracotta.management.service.monitoring.EntityManagementRegistry;
import org.terracotta.management.service.monitoring.EntityMonitoringService;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

/**
 * Created by adah on 2017-06-28.
 */
public class ManagementTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  @Mock
  private EntityManagementRegistry registry;
  private Management management = new Management();;

  @Mock
  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore;

  @Mock
  private FrsDataLogIdentifier frsDataLogIdentifier;

  @Mock
  private EntityMonitoringService monitoringService;

  @Before
  public void setUp() throws Exception {
    when(monitoringService.getConsumerId()).thenReturn(42L);
    when(registry.getMonitoringService()).thenReturn(monitoringService);
    when(registry.registerAndRefresh(any())).thenReturn(CompletableFuture.completedFuture(null));
    management.onManagementRegistryCreated(registry);
  }

  @After
  public void tearDown() throws Exception {
    management.onManagementRegistryClose(registry);
  }

  @Test
  public void registerRestartStore_pushed_created_event() throws Exception {
    management.registerRestartStore(frsDataLogIdentifier, folder.getRoot(), restartStore);
    verify(registry).pushServerEntityNotification(any(), contains("EHCACHE_RESTART_STORE_CREATED"));
  }

  @Test
  public void deRegisterRestartStore_pushed_destroyed_event() throws Exception {
    management.registerRestartStore(frsDataLogIdentifier, folder.getRoot(), restartStore);

    // and now de register
    management.deRegisterRestartStore(frsDataLogIdentifier);
    verify(registry).pushServerEntityNotification(any(), contains("EHCACHE_RESTART_STORE_DESTROYED"));
  }

}
