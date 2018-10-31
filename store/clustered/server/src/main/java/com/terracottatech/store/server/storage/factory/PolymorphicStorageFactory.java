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
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class PolymorphicStorageFactory implements InternalStorageFactory {
  private final Map<StorageType, InternalStorageFactory> storageFactoryMap;

  public PolymorphicStorageFactory(Map<StorageType, InternalStorageFactory> storageFactories) {
    this.storageFactoryMap = Collections.unmodifiableMap(storageFactories);
  }

  @Override
  public SovereignStorage<?,?> getStorage(StorageConfiguration storageConfiguration) throws StorageFactoryException {
    return getFactory(storageConfiguration).getStorage(storageConfiguration);
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
    getFactory(storageConfiguration).shutdownStorage(storageConfiguration, storage);
  }

  @Override
  public void close() {
  }

  @Override
  public void prepareForSynchronization() {
    storageFactoryMap.values().forEach(InternalStorageFactory::prepareForSynchronization);
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    BackupCapable backupCoordinator = null;
    for (InternalStorageFactory factory : storageFactoryMap.values()) {
      BackupCapable backupCapable = factory.getBackupCoordinator();
      if (backupCapable != null) {
        // As of now any one backup coordinator will do for now as we have only restartability based backups now
        // TODO: create a composite backup capable object when new non FRS based storage technologies are plugged in
        if (backupCoordinator != null && !backupCapable.getName().equals(backupCoordinator.getName())) {
          throw new AssertionError("Multiple Backup Coordinators are not supported");
        }
        backupCoordinator = backupCapable;
      }
    }
    return backupCoordinator;
  }

  private InternalStorageFactory getFactory(StorageConfiguration storageConfiguration) {
    StorageType storageType = storageConfiguration.getStorageType();
    InternalStorageFactory storageFactory =  storageFactoryMap.get(storageType);
    if (storageFactory == null) {
      throw new AssertionError("Unknown StorageType: " + storageType);
    }
    return storageFactory;
  }
}