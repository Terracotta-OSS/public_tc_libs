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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.spi.persistence.PersistableResourceService.PersistenceSpaceIdentifier;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.ehcache.disk.config.EnterpriseDiskResourceType;
import com.terracottatech.ehcache.disk.internal.config.FastRestartStoreResourcePoolImpl;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerManager;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerService.FastRestartStoreSpaceIdentifier;

import static org.ehcache.config.ResourceType.Core.DISK;
import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * FRS service unit test.
 *
 * @author RKAV
 */
public class DefaultRestartableStoreContainerServiceTest {
  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private LocalPersistenceService persistenceService;
  private DefaultRestartableStoreContainerService serviceUnderTest;

  private ServiceProvider<Service> mockProvider;
  private CacheConfiguration<?, ?> mockConfiguration = mock(CacheConfiguration.class);

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    persistenceService = new DefaultLocalPersistenceService(new CacheManagerPersistenceConfiguration(
        tmpFolder.newFolder()));
    persistenceService.start(null);
    serviceUnderTest = new DefaultRestartableStoreContainerService();
    mockProvider = mock(ServiceProvider.class);
    when(mockProvider.getService(eq(LocalPersistenceService.class))).thenReturn(persistenceService);
    ResourcePools mockPools = mock(ResourcePools.class);
    when(mockPools.getPoolForResource(EnterpriseDiskResourceType.Types.FRS)).
        thenReturn(new FastRestartStoreResourcePoolImpl(100, MemoryUnit.MB));
    when(mockConfiguration.getResourcePools()).thenReturn(mockPools);
  }

  @After
  public void tearDown() {
    persistenceService.stop();
    persistenceService = null;
    serviceUnderTest = null;
  }

  @Test
  public void testStartWithoutLocalPersistence() throws Exception {
    try {
      serviceUnderTest.start(null);
      fail("FRS service succeeded despite no local persistence");
    } catch (RuntimeException e) {
      assertThat(e, is(instanceOf(NullPointerException.class)));
    }
    // stop should still work..
    serviceUnderTest.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStartWithLocalPersistence() throws Exception {
    serviceUnderTest.start(mockProvider);

    PersistenceSpaceIdentifier<?> id1 = serviceUnderTest.getPersistenceSpaceIdentifier("cache1", mockConfiguration);
    PersistenceSpaceIdentifier<?> id2 = serviceUnderTest.getPersistenceSpaceIdentifier("cache2", mockConfiguration);
    assertThat(id1, is(instanceOf(FastRestartStoreSpaceIdentifier.class)));
    assertThat(id2, is(instanceOf(FastRestartStoreSpaceIdentifier.class)));
    FastRestartStoreSpaceIdentifier fid1 = (FastRestartStoreSpaceIdentifier)id1;
    FastRestartStoreSpaceIdentifier fid2 = (FastRestartStoreSpaceIdentifier)id2;
    assertThat(serviceUnderTest.getRestartableStoreManager(fid1), is(instanceOf(FastRestartStoreContainerManager.class)));
    assertThat(serviceUnderTest.getRestartableStoreManager(fid2), is(instanceOf(FastRestartStoreContainerManager.class)));

    serviceUnderTest.stop();
    assertNull(serviceUnderTest.getPersistenceSpaceIdentifier("cache1", null));
    assertNull(serviceUnderTest.getPersistenceSpaceIdentifier("cache2", null));
  }


  @Test
  public void testMaintenanceModeWithoutLocalPersistence() throws Exception {
    try {
      serviceUnderTest.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
      fail("FRS service succeeded despite no local persistence");
    } catch (RuntimeException e) {
      assertThat(e, is(instanceOf(NullPointerException.class)));
    }
    // stop should still work..
    serviceUnderTest.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMaintenanceModeWithLocalPersistence() throws Exception {
    serviceUnderTest.startForMaintenance(mockProvider, MaintainableService.MaintenanceScope.CACHE_MANAGER);
    assertNull(serviceUnderTest.getPersistenceSpaceIdentifier("cache1", null));
    serviceUnderTest.stop();
    assertNull(serviceUnderTest.getPersistenceSpaceIdentifier("cache1", null));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDestroyInNormalMode() throws Exception {
    serviceUnderTest.start(mockProvider);
    PersistenceSpaceIdentifier<?> id1 = serviceUnderTest.getPersistenceSpaceIdentifier("cache1", mockConfiguration);
    assertThat(id1, is(instanceOf(FastRestartStoreSpaceIdentifier.class)));
    assertThat(serviceUnderTest.getRestartableStoreManager((FastRestartStoreSpaceIdentifier)id1),
        is(instanceOf(FastRestartStoreContainerManager.class)));
    try {
      serviceUnderTest.destroyAll();
      fail("Not expected to allow destroy operation when service started in normal mode");
    } catch (Exception e) {
      assertThat(e, is(instanceOf(IllegalStateException.class)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDestroyAll() throws Exception {
    serviceUnderTest.startForMaintenance(mockProvider, MaintainableService.MaintenanceScope.CACHE_MANAGER);
    assertNull(serviceUnderTest.getPersistenceSpaceIdentifier("cache1", null));
    serviceUnderTest.destroyAll();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHandlesResourceType() throws Exception {
    assertThat(serviceUnderTest.handlesResourceType(DISK), is(false));
    assertThat(serviceUnderTest.handlesResourceType(HEAP), is(false));
    assertThat(serviceUnderTest.handlesResourceType(OFFHEAP), is(false));
    assertThat(serviceUnderTest.handlesResourceType(EnterpriseDiskResourceType.Types.FRS), is(true));
    serviceUnderTest.start(mockProvider);
    assertThat(serviceUnderTest.handlesResourceType(EnterpriseDiskResourceType.Types.FRS), is(true));
  }
}
