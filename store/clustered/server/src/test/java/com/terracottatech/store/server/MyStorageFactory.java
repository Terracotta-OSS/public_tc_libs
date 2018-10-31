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
package com.terracottatech.store.server;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import com.terracottatech.store.server.storage.factory.StorageFactoryException;

import java.io.IOException;

@SuppressWarnings("try")
class MyStorageFactory implements StorageFactory {
  private SovereignStorage<? extends SovereignDataset<?>, ? extends SovereignDatasetDescription> storage
      = new StorageTransient(SovereignBufferResource.unlimited());
  private StorageConfiguration lastConfiguration;

  @Override
  public SovereignStorage<?, ?> getStorage(StorageConfiguration configuration) throws StorageFactoryException {
    lastConfiguration = configuration;
    return storage;
  }

  public SovereignStorage<? extends SovereignDataset<?>, ? extends SovereignDatasetDescription> getStorage() {
    return storage;
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
    storage.shutdown();
  }

  public StorageConfiguration getLastConfiguration() {
    return lastConfiguration;
  }

  @Override
  public void close() throws Exception {
    storage.shutdown();
  }
}
