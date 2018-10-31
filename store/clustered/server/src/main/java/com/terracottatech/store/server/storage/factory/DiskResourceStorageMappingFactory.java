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
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.server.storage.configuration.PersistentStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("try")
public class DiskResourceStorageMappingFactory implements InternalStorageFactory {
  private final Map<String, Tuple<String, AbstractPersistentStorage>> storages = new HashMap<>();
  private final InternalStorageFactory underlying;

  public DiskResourceStorageMappingFactory(InternalStorageFactory anyStorageFactory) {
    this.underlying = anyStorageFactory;
  }

  @Override
  public SovereignStorage<?, ?> getStorage(StorageConfiguration storageConfiguration) throws StorageFactoryException {
    if (storageConfiguration instanceof PersistentStorageConfiguration) {
      PersistentStorageConfiguration persistentStorageConfiguration = (PersistentStorageConfiguration) storageConfiguration;
      String offheapResource = storageConfiguration.getOffheapResource();
      String diskResource = persistentStorageConfiguration.getDiskResource();

      Tuple<String, AbstractPersistentStorage> tuple = storages.get(diskResource);
      if (tuple != null) {
        checkMatchingStorageTypes(tuple.getSecond(), persistentStorageConfiguration);
        if (tuple.getFirst().equals(offheapResource)) {
          throw new StorageFactoryException("Multiple storage instances for the same configuration can not be created. "
                                            + diskResource + " and " + offheapResource + " are currently tied to another storage instance");
        } else {
          throw new StorageFactoryException("Disk resource can not be shared between multiple offheap resources. "
                                            + diskResource + " is currently tied to " + tuple.getFirst());
        }
      }
      AbstractPersistentStorage storage = (AbstractPersistentStorage) underlying.getStorage(storageConfiguration);
      storages.put(diskResource, Tuple.of(offheapResource, storage));
      return storage;
    } else {
      return underlying.getStorage(storageConfiguration);
    }
  }

  private void checkMatchingStorageTypes(AbstractPersistentStorage storage,
                                         PersistentStorageConfiguration persistentStorageConfiguration) throws StorageFactoryException {
    if (!storage.isCompatible(persistentStorageConfiguration.getPersistentStorageType())) {
      throw new StorageFactoryException("Incompatible Storage Types. Disk Resource "
                                        + persistentStorageConfiguration.getDiskResource()
                                        + " already configured for "
                                        + storage.getPersistentStorageType().getShortName()
                                        + " Cannot use this disk resource again for "
                                        + persistentStorageConfiguration.getPersistentStorageType().getShortName());
    }
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
    if (storageConfiguration instanceof PersistentStorageConfiguration) {
      PersistentStorageConfiguration persistentStorageConfiguration = (PersistentStorageConfiguration) storageConfiguration;
      String offheapResource = storageConfiguration.getOffheapResource();
      String diskResource = persistentStorageConfiguration.getDiskResource();

      Tuple<String, AbstractPersistentStorage> tuple = storages.get(diskResource);
      if (tuple != null && tuple.getFirst().equals(offheapResource) && tuple.getSecond() == storage) {
        storages.remove(diskResource);
      }
    }
    underlying.shutdownStorage(storageConfiguration, storage);
  }

  @Override
  public void prepareForSynchronization() {
    underlying.prepareForSynchronization();
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    return underlying.getBackupCoordinator();
  }

  @Override
  public void close() throws Exception {
    underlying.close();
  }
}