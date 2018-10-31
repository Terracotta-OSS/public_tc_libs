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
import com.terracottatech.br.ssi.frs.FRSBackupCoordinator;
import com.terracottatech.br.ssi.frs.RestartStoreLocator;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.base.AbstractRestartabilityBasedStorage;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concrete factory that churns out backup coordinators for restartability (FRS or HYBRID) based storage  systems.
 */
public class RestartabilityBackupCoordinatorFactory implements BackupCoordinatorFactory {
  private static final String STORE_OWNER_NAME = "Tcstore";

  private final Set<RestartStoreLocator<ByteBuffer, ByteBuffer, ByteBuffer>> storeLocators;

  public RestartabilityBackupCoordinatorFactory() {
    this.storeLocators = Collections.newSetFromMap(new ConcurrentHashMap<>());
  }

  public BackupCapable create() {
    return new FRSBackupCoordinator(Collections.unmodifiableCollection(storeLocators), STORE_OWNER_NAME);
  }

  public void setupBackupManagementFor(AbstractPersistentStorage storage, String diskResource, Path dataRoot,
                                        PersistenceRoot persistenceRoot) {
    AbstractRestartabilityBasedStorage restartabilityBasedStorage = (AbstractRestartabilityBasedStorage) storage;
    storeLocators.add(new FRSLocator(diskResource, dataRoot, persistenceRoot.getMetaRoot()
        .toPath(), restartabilityBasedStorage.getMetaStore()));
    storeLocators.add(new FRSLocator(diskResource, dataRoot, persistenceRoot.getDataRoot()
        .toPath(), restartabilityBasedStorage.getDatasetStore()));
  }

  private static final class FRSLocator implements RestartStoreLocator<ByteBuffer, ByteBuffer, ByteBuffer> {
    private final Path relativePath;
    private final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> store;

    private FRSLocator(String rootID, Path rootPath, Path storePath, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> store) {
      this.store = store;
      this.relativePath = Paths.get(rootID).resolve(rootPath.relativize(storePath));
    }

    @Override
    public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getStore() {
      return store;
    }

    @Override
    public Path getRelativeToRoot() {
      return relativePath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FRSLocator that = (FRSLocator)o;

      return relativePath.equals(that.relativePath);
    }

    @Override
    public int hashCode() {
      return relativePath.hashCode();
    }
  }
}
