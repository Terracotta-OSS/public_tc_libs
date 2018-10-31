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
package com.terracottatech.store.server.storage;

import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.br.ssi.BackupSnapshot;
import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.config.data_roots.DataDirectoriesConfig;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.impl.persistence.hybrid.SovereignHybridStorage;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.server.storage.configuration.PersistentStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.MemoryStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import com.terracottatech.store.server.storage.factory.StorageFactoryException;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.hamcrest.HamcrestArgumentMatcher;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResources;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.terracotta.offheapresource.OffHeapResourceIdentifier.identifier;

public class StorageFactoryProviderTest {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  private PlatformConfiguration platformConfiguration;

  @Before
  public void setup() {
    platformConfiguration = mock(PlatformConfiguration.class);
  }

  @Test
  public void providesCorrectServiceTypes() {
    setupOffheapResource();

    StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
    Collection<Class<?>> serviceTypes = storageFactoryProvider.getProvidedServiceTypes();
    assertEquals(3, serviceTypes.size());
    assertEquals(StorageFactory.class, serviceTypes.iterator().next());
    assertThat(serviceTypes.stream().filter((c) -> c.isAssignableFrom(BackupCapable.class)).count(), is(1L));
  }

  @Test
  public void getServiceForSuperType() throws Exception {
    setupOffheapResource();
    StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
    assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
    BackupCapable backupCapable = storageFactoryProvider.getService(1L, () -> BackupCapable.class);
    assertNull(backupCapable);
  }

  @Test
  public void getMemoryStorage() throws Exception {
    setupOffheapResource();

    StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
    assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
    StorageConfiguration storageConfiguration = new MemoryStorageConfiguration("offheap");
    StorageFactory storageFactory = storageFactoryProvider.getService(1L, () -> StorageFactory.class);
    SovereignStorage<?, ?> storage = storageFactory.getStorage(storageConfiguration);
    assertTrue(storage instanceof StorageTransient);
  }

  @Test
  public void getMemoryStorageWithInvalidResource() throws Exception {
    setupOffheapResource();

    StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
    assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
    StorageConfiguration storageConfiguration = new MemoryStorageConfiguration("unknown");
    StorageFactory storageFactory = storageFactoryProvider.getService(1L, () -> StorageFactory.class);

    exception.expect(StorageFactoryException.class);
    storageFactory.getStorage(storageConfiguration);
  }

  @Test
  public void getFRSStorage() throws Exception {
    setupOffheapResource();

    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = Files.createTempDirectory("data-root");
    try {
      setupDiskResource(tempDirectory);
      StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
      assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
      StorageConfiguration storageConfiguration = new PersistentStorageConfiguration("disk", "offheap");
      StorageFactory storageFactory = storageFactoryProvider.getService(1L, () -> StorageFactory.class);
      storage = storageFactory.getStorage(storageConfiguration);
      assertTrue(storage instanceof SovereignFRSStorage);
    } finally {
      FRSCleanup.clean(storage, tempDirectory);
    }
  }

  @Test
  public void getFRSStorageIsBackupCapable() throws Exception {
    setupOffheapResource();
    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = Files.createTempDirectory("data-root");
    try {
      setupDiskResource(tempDirectory);
      StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
      assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
      StorageConfiguration storageConfiguration = new PersistentStorageConfiguration("disk", "offheap");
      StorageFactory storageFactory = storageFactoryProvider.getService(1L, () -> StorageFactory.class);
      storage = storageFactory.getStorage(storageConfiguration);

      BackupCapable backupStorage = storageFactoryProvider.getService(1L, () -> BackupCapable.class);
      backupStorage.prepare();
      Collection<BackupSnapshot> snapshot = backupStorage.snapshot();
      assertThat(snapshot.size(), Matchers.greaterThan(0));
      snapshot.forEach((s) -> assertThat(s.getRelativeFromRoot().toString(), containsString("disk")));
      assertTrue(storage instanceof SovereignFRSStorage);
    } finally {
      FRSCleanup.clean(storage, tempDirectory);
    }
  }

  @Test
  public void getHybridStorageIsBackupCapable() throws Exception {
    setupOffheapResource();
    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = Files.createTempDirectory("data-root");
    try {
      setupDiskResource(tempDirectory);
      StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
      assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
      StorageConfiguration storageConfiguration = new PersistentStorageConfiguration("disk", "offheap",
          PersistentStorageEngine.HYBRID);
      StorageFactory storageFactory = storageFactoryProvider.getService(1L, () -> StorageFactory.class);
      storage = storageFactory.getStorage(storageConfiguration);

      BackupCapable backupStorage = storageFactoryProvider.getService(1L, () -> BackupCapable.class);
      backupStorage.prepare();
      Collection<BackupSnapshot> snapshot = backupStorage.snapshot();
      assertThat(snapshot.size(), Matchers.greaterThan(0));
      snapshot.forEach((s) -> assertThat(s.getRelativeFromRoot().toString(), containsString("disk")));
      assertTrue(storage instanceof SovereignHybridStorage);
    } finally {
      FRSCleanup.clean(storage, tempDirectory);
    }
  }

  @Test
  public void getFRSStorageWithInvalidResource() throws Exception {
    setupOffheapResource();

    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = Files.createTempDirectory("data-root");
    try {
      setupDiskResource(tempDirectory);
      StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
      assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
      StorageConfiguration storageConfiguration = new PersistentStorageConfiguration("disk", "unknown");
      StorageFactory storageFactory = storageFactoryProvider.getService(1L, () -> StorageFactory.class);

      exception.expect(StorageFactoryException.class);
      storage = storageFactory.getStorage(storageConfiguration);
    } finally {
      FRSCleanup.clean(storage, tempDirectory);
    }
  }

  @Test
  public void multipleOffheapResourcesAreNotAllowed() throws Exception {
    setupMultipleOffheapResources();

    StorageFactoryProvider storageFactoryProvider = new StorageFactoryProvider();
    assertTrue(storageFactoryProvider.initialize(null, platformConfiguration));
    StorageFactory storageFactory = storageFactoryProvider.getService(1L, () -> StorageFactory.class);

    exception.expect(StorageFactoryException.class);
    storageFactory.getStorage(new MemoryStorageConfiguration("offheap"));
  }

  private void setupOffheapResource() {
    OffHeapResources offheapResources = mock(OffHeapResources.class);
    OffHeapResource offheapResource = mock(OffHeapResource.class);
    when(platformConfiguration.getExtendedConfiguration(OffHeapResources.class)).thenReturn(Collections.singletonList(offheapResources));
    when(offheapResources.getAllIdentifiers()).thenReturn(Collections.singleton(identifier("offheap")));
    when(offheapResources.getOffHeapResource(argThat(new HamcrestArgumentMatcher<>(new OffHeapResourceIdentifierMatcher("offheap"))))).thenReturn(offheapResource);
    when(offheapResources.getOffHeapResource(argThat(new HamcrestArgumentMatcher<>(new OffHeapResourceIdentifierMatcher("unknown"))))).thenReturn(null);
  }

  void setupMultipleOffheapResources() {
    OffHeapResources offheapResources1 = mock(OffHeapResources.class);
    OffHeapResources offheapResources2 = mock(OffHeapResources.class);
    OffHeapResource offheapResource = mock(OffHeapResource.class);
    when(platformConfiguration.getExtendedConfiguration(OffHeapResources.class)).thenReturn(Arrays.asList(offheapResources1, offheapResources2));
    when(offheapResources1.getAllIdentifiers()).thenReturn(Collections.singleton(identifier("offheap")));
    when(offheapResources1.getOffHeapResource(argThat(new HamcrestArgumentMatcher<>(new OffHeapResourceIdentifierMatcher("offheap"))))).thenReturn(offheapResource);
    when(offheapResources1.getOffHeapResource(argThat(new HamcrestArgumentMatcher<>(new OffHeapResourceIdentifierMatcher("unknown"))))).thenReturn(null);
  }

  private void setupDiskResource(Path dataRoot) {
    DataDirectoriesConfig dataDirectoriesConfig = mock(DataDirectoriesConfig.class);
    when(platformConfiguration.getExtendedConfiguration(DataDirectoriesConfig.class)).thenReturn(Collections.singletonList(dataDirectoriesConfig));
    DataDirectories dataDirectories = mock(DataDirectories.class);
    when(dataDirectories.getDataDirectoryNames()).thenReturn(Collections.singleton("disk"));
    when(dataDirectories.getDataDirectory("disk")).thenReturn(dataRoot);
    when(dataDirectoriesConfig.getDataDirectoriesForServer(any(PlatformConfiguration.class))).thenReturn(dataDirectories);
  }
}
