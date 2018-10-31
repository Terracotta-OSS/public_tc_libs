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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InterningStorageFactory implements InternalStorageFactory, AutoCloseable {
  private final InternalStorageFactory storageFactory;
  private final Map<StorageConfiguration, SovereignStorage<?,?>> storages = new HashMap<>();
  private boolean closed = false;

  public InterningStorageFactory(InternalStorageFactory storageFactory) {
    this.storageFactory = storageFactory;
  }

  @Override
  public synchronized SovereignStorage<?,?> getStorage(StorageConfiguration storageConfiguration) throws StorageFactoryException{
    if (closed) {
      throw new StorageFactoryException("Attempt to get storage from a StorageFactory that is already closed");
    }

    SovereignStorage<?,?> internedStorage = storages.get(storageConfiguration);
    if (internedStorage != null) {
      return internedStorage;
    }

    SovereignStorage<?,?> storage = storageFactory.getStorage(storageConfiguration);
    storages.put(storageConfiguration, storage);
    return storage;
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
    SovereignStorage<?, ?> removed = storages.remove(storageConfiguration);
    if (removed == storage) {
      storageFactory.shutdownStorage(storageConfiguration, storage);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;

    for (SovereignStorage<?,?> storage : storages.values()) {
      storage.shutdown();
    }
  }

  @Override
  public void prepareForSynchronization() {
    storages.clear();
    storageFactory.prepareForSynchronization();
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    return storageFactory.getBackupCoordinator();
  }
}