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

import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.store.server.storage.configuration.PersistentStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.offheap.BufferResourceFactory;
import com.terracottatech.store.server.storage.offheap.UnknownResourceException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class PersistentStorageFactory implements InternalStorageFactory {
  public static final String STORE_DIRECTORY = "store";
  private static final String BACKUP_DIR_PREFIX = "terracotta.backup.";

  private final BufferResourceFactory bufferResourceFactory;
  private final BackupCoordinatorFactory backupCoordinatorFactory;
  private final DataDirectories dataRootConfig;

  public PersistentStorageFactory(BufferResourceFactory bufferResourceFactory,
                                  DataDirectories dataRootConfig,
                                  BackupCoordinatorFactory backupCoordinatorFactory) {
    this.bufferResourceFactory = bufferResourceFactory;
    this.dataRootConfig = dataRootConfig;
    this.backupCoordinatorFactory = backupCoordinatorFactory;
  }

  @Override
  public AbstractPersistentStorage getStorage(StorageConfiguration storageConfiguration) throws StorageFactoryException {
    PersistentStorageConfiguration persistentStorageConfiguration = (PersistentStorageConfiguration) storageConfiguration;
    SovereignBufferResource bufferResource = getSovereignBufferResource(persistentStorageConfiguration);

    String diskResource = persistentStorageConfiguration.getDiskResource();
    Set<String> dataRootIdentifiers = dataRootConfig.getDataDirectoryNames();
    if (!dataRootIdentifiers.contains(diskResource)) {
      throw new StorageFactoryException("Unknown disk resource: " + diskResource);
    }

    Path dataRoot = dataRootConfig.getDataDirectory(diskResource);

    Path storeRoot = dataRoot.resolve(STORE_DIRECTORY);
    if (!Files.exists(storeRoot)) {
      try {
        Files.createDirectories(storeRoot);
      } catch (IOException e) {
        throw new StorageFactoryException("Could not create store root inside data directory: " + dataRoot, e);
      }
    }
    try {
      File storeRootAsFile = storeRoot.toFile();
      PersistenceRoot persistenceRoot = new PersistenceRoot(storeRootAsFile, PersistenceRoot.Mode.DONTCARE);
      AbstractPersistentStorage storage = createStorage(persistenceRoot, bufferResource);

      Future<Void> startupMetadata = storage.startupMetadata();
      startupMetadata.get();

      Future<Void> startupData = storage.startupData();
      startupData.get();

      setupStorageManagement(storage, diskResource, storeRoot);
      backupCoordinatorFactory.setupBackupManagementFor(storage, diskResource, dataRoot, persistenceRoot);
      return storage;
    } catch (InterruptedException | ExecutionException | IOException e) {
      throw new StorageFactoryException("An error occurred creating FRS storage", e);
    }
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
    storage.shutdown();
  }

  @Override
  public void prepareForSynchronization() {
    String backupDirectory = BACKUP_DIR_PREFIX + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd.HHmmss"));

    dataRootConfig.getDataDirectoryNames().forEach(rootId -> {
      Path dataRoot = dataRootConfig.getDataDirectory(rootId);
      Path storeStorageSpace = getDataPath(dataRoot);
      if(Files.exists(storeStorageSpace)) {
        Path backupSpace = dataRoot.resolve(backupDirectory);
        Path backupStorageSpace = backupSpace.resolve(STORE_DIRECTORY);
        try {
          Files.createDirectories(backupSpace);
          Files.move(storeStorageSpace, backupStorageSpace);
        } catch (IOException e) {
          throw new RuntimeException("Failed to clean files ", e);
        }
      }
    });
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    return backupCoordinatorFactory.create();
  }

  @Override
  public void close() {
    deRegisterStorageManagement();
  }

  protected abstract void deRegisterStorageManagement();
  protected abstract void setupStorageManagement(AbstractPersistentStorage storage, String diskResource, Path storeRoot);
  protected abstract AbstractPersistentStorage createStorage(PersistenceRoot persistenceRoot,
                                                             SovereignBufferResource bufferResource);


  private SovereignBufferResource getSovereignBufferResource(PersistentStorageConfiguration persistentStorageConfiguration)
      throws StorageFactoryException {
    String offheapResourceName = persistentStorageConfiguration.getOffheapResource();
    try {
      return bufferResourceFactory.get(offheapResourceName);
    } catch (UnknownResourceException e) {
      throw new StorageFactoryException("Unknown offheap resource: " + offheapResourceName, e);
    }
  }

  private Path getDataPath(Path dataRoot) {
    return dataRoot.resolve(STORE_DIRECTORY);
  }
}
