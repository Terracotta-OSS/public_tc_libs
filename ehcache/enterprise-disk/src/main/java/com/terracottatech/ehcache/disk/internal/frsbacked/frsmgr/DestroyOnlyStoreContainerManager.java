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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;
import com.terracottatech.ehcache.common.frs.DeleteOnlyRestartableObject;
import com.terracottatech.ehcache.common.frs.RestartLogLifecycle;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * FRS store container to hold stores when the cachemanager is opened only to destroy caches.
 *
 * @author RKAV .
 */
final class DestroyOnlyStoreContainerManager implements CloseFrsContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DestroyOnlyStoreContainerManager.class);

  private final RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> cacheObjectManager;
  private final FileBasedPersistenceContext frsContext;
  private final LocalMetadataProvider metadataProvider;
  private final Map<String, DeleteOnlyRestartableObject> destroyOnlyObjects;
  private final String frsId;

  private ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> cacheDataStoreArea;
  private RestartLogLifecycle destroyOnlyLifecycle;

  DestroyOnlyStoreContainerManager(FileBasedPersistenceContext fileContext,
                                   LocalMetadataProvider metadataProvider,
                                   String frsId) {
    this.metadataProvider = metadataProvider;
    this.frsContext = fileContext;
    this.cacheObjectManager = new RegisterableObjectManager<>();
    this.destroyOnlyObjects = new HashMap<>();
    this.frsId = frsId;
  }

  /**
   * Initialize the cache data area where all cache data are stored.
   */
  void init() {
    initCacheStore();
    destroyOnlyLifecycle = new RestartLogLifecycle(this.frsContext.getDirectory(), cacheDataStoreArea);
    bootstrapDestroyOnlyObject();
    destroyOnlyLifecycle.startStore();
  }

  /**
   * Close and shutdown the cache data area of FRS.
   */
  @Override
  public void closeContainer() {
    final ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> localRef = cacheDataStoreArea;
    LOGGER.info("Shutting down cache store");
    if (localRef != null) {
      destroyOnlyLifecycle.shutStore();
      cacheDataStoreArea = null;
      destroyOnlyLifecycle = null;
    }
  }

  /**
   * Destroy the store and all its dependent restartable objects.
   *
   * @param storeId Id of the store to destroy
   */
  void destroyStore(String storeId) throws CachePersistenceException {
    // first destroy state repositories
    Map<String, SavedStateRepositoryConfig<?, ?>> stateRepositoryMap =
        metadataProvider.getRestartableStateRepositoriesForStore(storeId);
    try {
      for (String composedId : stateRepositoryMap.keySet()) {
        DeleteOnlyRestartableObject restartableObject = destroyOnlyObjects.remove(composedId);
        restartableObject.destroy();
        cacheObjectManager.unregisterStripe(metadataProvider.toStateRepositoryIdentifier(composedId));
        metadataProvider.removeStateRepository(composedId);
      }
      DeleteOnlyRestartableObject restartableObject = destroyOnlyObjects.remove(storeId);
      restartableObject.destroy();
      cacheObjectManager.unregisterStripe(metadataProvider.toStoreIdentifier(storeId));

      metadataProvider.removeStore(storeId);
    } catch (RuntimeException te) {
      throw new CachePersistenceException("Unable to destroy all objects for store " + storeId + " within " + frsId
                                          + " container");
    }
  }

  /**
   * Initializes the FRS container of cache stores.
   * <p>
   *   This method assumes that the file/directory already exists and does not create it if it does not exist.
   */
  private void initCacheStore() {
    File logDirectory = new File(frsContext.getDirectory(), frsId);
    Properties properties = new Properties();
    try {
      cacheDataStoreArea = new ControlledTransactionRestartStore<>(
          RestartStoreFactory.createStore(cacheObjectManager, logDirectory, properties));
    } catch (IOException | RestartStoreException e) {
      throw new RuntimeException("Cannot initialize FRS store area for cache data", e);
    }
  }

  /**
   * Bootstrap dummy objects as this FRS container is only opened to destroy stores.
   */
  private void bootstrapDestroyOnlyObject() {
    for (Map.Entry<String, SavedStoreConfig> store : metadataProvider.getExistingRestartableStores(frsId).entrySet()) {
      final DeleteOnlyRestartableObject restartableObject = new DeleteOnlyRestartableObject(
          metadataProvider.toStoreIdentifier(store.getKey()),
          cacheDataStoreArea);
      destroyOnlyObjects.put(store.getKey(), restartableObject);
      cacheObjectManager.registerObject(restartableObject);
    }
    for (Map.Entry<String, SavedStateRepositoryConfig<?, ?>> repo :
        metadataProvider.getExistingRestartableStateRepositories(frsId).entrySet()) {
      final ByteBuffer stateRepoId = metadataProvider.toStateRepositoryIdentifier(repo.getKey());
      final DeleteOnlyRestartableObject restartableObject = new DeleteOnlyRestartableObject(stateRepoId, cacheDataStoreArea);
      destroyOnlyObjects.put(repo.getKey(), restartableObject);
      cacheObjectManager.registerObject(new DeleteOnlyRestartableObject(stateRepoId, cacheDataStoreArea));
    }
  }
}
