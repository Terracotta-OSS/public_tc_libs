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
import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.store.server.storage.configuration.MemoryStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.offheap.BufferResourceFactory;
import com.terracottatech.store.server.storage.offheap.UnknownResourceException;

import java.io.IOException;

public class MemoryStorageFactory implements InternalStorageFactory {
  private final BufferResourceFactory bufferResourceFactory;

  public MemoryStorageFactory(BufferResourceFactory bufferResourceFactory) {
    this.bufferResourceFactory = bufferResourceFactory;
  }

  @Override
  public SovereignStorage<?, ?> getStorage(StorageConfiguration storageConfiguration) throws StorageFactoryException {
    MemoryStorageConfiguration memoryStorageConfiguration = (MemoryStorageConfiguration) storageConfiguration;

    String offheapResourceName = memoryStorageConfiguration.getOffheapResource();
    try {
      SovereignBufferResource bufferResource = bufferResourceFactory.get(offheapResourceName);
      return new StorageTransient(bufferResource);
    } catch (UnknownResourceException e) {
      throw new StorageFactoryException("Unknown offheap resource: " + offheapResourceName, e);
    }
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
    storage.shutdown();
  }

  @Override
  public void prepareForSynchronization() {
    //no op
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    return null;
  }

  @Override
  public void close() {
  }
}
