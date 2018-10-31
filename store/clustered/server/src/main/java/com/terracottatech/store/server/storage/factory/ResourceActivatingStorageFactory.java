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
import com.terracottatech.sovereign.resource.NamedBufferResources;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;

import java.io.IOException;

@SuppressWarnings("try")
public class ResourceActivatingStorageFactory implements InternalStorageFactory {
  private final InternalStorageFactory underlying;

  public ResourceActivatingStorageFactory(InternalStorageFactory underlying) {
    this.underlying = underlying;
  }

  @Override
  public SovereignStorage<?, ?> getStorage(StorageConfiguration storageConfiguration) throws StorageFactoryException {
    SovereignStorage<?, ?> storage = underlying.getStorage(storageConfiguration);

    NamedBufferResources bufferResource = (NamedBufferResources) storage.getBufferResource();
    String offheapResourceName = storageConfiguration.getOffheapResource();
    try {
      bufferResource.activate(offheapResourceName);
    } catch (StoreRuntimeException sre) {
      throw new StorageFactoryException(sre);
    }

    return storage;
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
    underlying.shutdownStorage(storageConfiguration, storage);
  }

  @Override
  public void close() throws Exception {
    underlying.close();
  }

  @Override
  public void prepareForSynchronization() {
    underlying.prepareForSynchronization();
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    return underlying.getBackupCoordinator();
  }
}
