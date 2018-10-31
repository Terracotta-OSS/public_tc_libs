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
package com.terracottatech.store.server.storage.factory;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.server.storage.FRSCleanup;
import com.terracottatech.store.server.storage.configuration.PersistentStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.management.DatasetStorageManagement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PersistentStorageFactoryTest extends BufferResourceTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void givesFRSStorage() throws Exception {
    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test1").toPath();
    try {
      DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
      StorageFactory storageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories, new DatasetStorageManagement(),
          new RestartabilityBackupCoordinatorFactory());

      PersistentStorageConfiguration configuration = new PersistentStorageConfiguration("disk", "offheap");
      storage = storageFactory.getStorage(configuration);
      assertTrue(storage instanceof SovereignFRSStorage);
    } finally {
      cleanFRS(storage, tempDirectory);
    }
  }

  @Test
  public void givesNewFRSStorageIfExistingStoreIsShutdown() throws Exception {
    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test2").toPath();
    try {
      DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
      StorageFactory storageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());

      PersistentStorageConfiguration configuration = new PersistentStorageConfiguration("disk", "offheap");
      SovereignStorage<?, ?> anotherStorage = storageFactory.getStorage(configuration);

      storageFactory.shutdownStorage(configuration, anotherStorage);

      storage = storageFactory.getStorage(configuration);
      assertNotSame(storage, anotherStorage);
    } finally {
      cleanFRS(storage, tempDirectory);
    }
  }

  @Test
  public void givesNewFRSStorageWithSameDiskButDifferentOffheapIfExistingStoreIsShutdown() throws Exception {
    SovereignFRSStorage storage;
    SovereignFRSStorage anotherStorage = null;
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test").toPath();
    try {
      DataDirectories dataRoots = getDataRootConfig(tempDirectory);
      PersistentStorageFactory storageFactory = new FRSStorageFactory(bufferResourceFactory, dataRoots,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());

      PersistentStorageConfiguration configuration = new PersistentStorageConfiguration("disk", "offheap",
          PersistentStorageEngine.FRS);
      AbstractPersistentStorage persistentStorage = storageFactory.getStorage(configuration);
      assertThat(persistentStorage, instanceOf(SovereignFRSStorage.class));
      storage = (SovereignFRSStorage) persistentStorage;

      storageFactory.shutdownStorage(configuration, storage);

      configuration = new PersistentStorageConfiguration("disk", "another-offheap");
      persistentStorage = storageFactory.getStorage(configuration);
      assertThat(persistentStorage, instanceOf(SovereignFRSStorage.class));

      anotherStorage = (SovereignFRSStorage) persistentStorage;
      assertNotSame(storage, anotherStorage);
    } finally {
      FRSCleanup.clean(anotherStorage, tempDirectory);
    }
  }

  @Test
  public void throwsIfNotADirectory() throws Exception {
    SovereignStorage<?, ?> storage = null;
    Path tempFile = temporaryFolder.newFile("file-not-directory").toPath();
    try {
      DataDirectories dataDirectories = getDataRootConfig(tempFile);
      StorageFactory storageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());

      PersistentStorageConfiguration configuration = new PersistentStorageConfiguration("disk", "offheap");
      exception.expect(StorageFactoryException.class);
      storage = storageFactory.getStorage(configuration);
    } finally {
      cleanFRS(storage, tempFile);
    }
  }

  @Test
  public void throwsIfUnknownOffheapResource() throws Exception {
    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test1").toPath();
    try {
      DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
      StorageFactory storageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());

      PersistentStorageConfiguration configuration = new PersistentStorageConfiguration("disk", "unknown");
      exception.expect(StorageFactoryException.class);
      storage = storageFactory.getStorage(configuration);
    } finally {
      cleanFRS(storage, tempDirectory);
    }
  }

  @Test
  public void throwsIfUnknownDiskResource() throws Exception {
    SovereignStorage<?, ?> storage = null;
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test1").toPath();
    try {
      DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
      StorageFactory storageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());

      PersistentStorageConfiguration configuration = new PersistentStorageConfiguration("unknown", "offheap");
      exception.expect(StorageFactoryException.class);
      storage = storageFactory.getStorage(configuration);
    } finally {
      cleanFRS(storage, tempDirectory);
    }
  }

  @Test
  public void throwsOnGetStorageForIdenticalConfigs() throws Exception {
    List<SovereignStorage<?, ?>> storages = new ArrayList<>();
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test1").toPath();

    try {
      DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
      InternalStorageFactory frsStorageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());
      StorageFactory storageFactory = new DiskResourceStorageMappingFactory(frsStorageFactory);

      StorageConfiguration configuration1 = new PersistentStorageConfiguration("disk", "offheap");
      storages.add(storageFactory.getStorage(configuration1));

      StorageConfiguration configuration2 = new PersistentStorageConfiguration("disk", "offheap");
      exception.expect(StorageFactoryException.class);
      exception.expectMessage(containsString("Multiple storage instances for the same configuration can not be created"));
      storages.add(storageFactory.getStorage(configuration2));
    } finally {
      shutdownStorages(storages);
      cleanFRS(null, tempDirectory);
    }
  }

  @Test
  public void throwsOnGetStorageStorageForDifferentConfigsWithSameDisk() throws Exception {
    List<SovereignStorage<?, ?>> storages = new ArrayList<>();
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test1").toPath();
    try {
      DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
      InternalStorageFactory frsStorageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());
      StorageFactory storageFactory = new DiskResourceStorageMappingFactory(frsStorageFactory);

      StorageConfiguration configuration1 = new PersistentStorageConfiguration("disk", "offheap");
      storages.add(storageFactory.getStorage(configuration1));

      StorageConfiguration configuration2 = new PersistentStorageConfiguration("disk", "another-offheap");
      exception.expect(StorageFactoryException.class);
      exception.expectMessage(containsString("Disk resource can not be shared between multiple offheap resources"));
      storages.add(storageFactory.getStorage(configuration2));
    } finally {
      shutdownStorages(storages);
      cleanFRS(null, tempDirectory);
    }
  }

  @Test
  public void throwsOnGetStorageForSameDiskDifferentStorageTypes() throws Exception {
    List<SovereignStorage<?, ?>> storages = new ArrayList<>();
    Path tempDirectory = temporaryFolder.newFolder("frs-storage-factory-test1").toPath();
    try {
      DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
      InternalStorageFactory hybridStorageFactory = new HybridStorageFactory(bufferResourceFactory, dataDirectories,
          new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());
      StorageFactory storageFactory = new DiskResourceStorageMappingFactory(hybridStorageFactory);

      StorageConfiguration configuration1 = new PersistentStorageConfiguration("disk", "offheap",
          PersistentStorageEngine.HYBRID);
      storages.add(storageFactory.getStorage(configuration1));

      StorageConfiguration configuration2 = new PersistentStorageConfiguration("disk", "offheap",
          PersistentStorageEngine.FRS);
      exception.expect(StorageFactoryException.class);
      exception.expectMessage(containsString("Incompatible Storage Types."));
      storages.add(storageFactory.getStorage(configuration2));
    } finally {
      shutdownStorages(storages);
      cleanFRS(null, tempDirectory);
    }
  }

  @Test
  public void prepareForSynchronization() throws Exception {
    Path tempDirectory = temporaryFolder.newFolder("prepareForSynchronization-test").toPath();
    DataDirectories dataDirectories = getDataRootConfig(tempDirectory);
    PersistentStorageFactory persistentStorageFactory = new FRSStorageFactory(bufferResourceFactory, dataDirectories,
        new DatasetStorageManagement(), new RestartabilityBackupCoordinatorFactory());

    PersistentStorageConfiguration configuration = new PersistentStorageConfiguration("disk", "offheap");
    SovereignStorage<?, ?> storage = persistentStorageFactory.getStorage(configuration);
    Path storagePath = tempDirectory.resolve("store");
    assertThat(Files.exists(storagePath), Matchers.is(true));

    storage.shutdown();
    System.gc();

    persistentStorageFactory.prepareForSynchronization();
    assertThat(Files.exists(storagePath), Matchers.is(false));
  }

  private DataDirectories getDataRootConfig(Path dataRoot) {
    DataDirectories dataRootConfig = mock(DataDirectories.class);
    when(dataRootConfig.getDataDirectoryNames()).thenReturn(Collections.singleton("disk"));
    when(dataRootConfig.getDataDirectory("disk")).thenReturn(dataRoot);
    return dataRootConfig;
  }

  private void shutdownStorages(List<SovereignStorage<?, ?>> storages) {
    storages.forEach(s -> {
      try {
        s.shutdown();
      } catch (IOException e) {
        System.err.println("Failed to shutdown SovereignStorage '" + s + "'");
        e.printStackTrace();
      }
    });
  }

  private void cleanFRS(SovereignStorage<?, ?> storage, Path tempDirectory) {
    try {
      FRSCleanup.clean(storage, tempDirectory);
    } catch (IOException e) {
      System.err.println("Failed to clean up '" + tempDirectory + "'");
      e.printStackTrace();
    }
  }
}