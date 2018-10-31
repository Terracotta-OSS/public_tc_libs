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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.core.spi.service.LocalPersistenceService.SafeSpaceIdentifier;
import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import com.terracottatech.ehcache.disk.config.EnterpriseDiskResourceType;
import com.terracottatech.ehcache.disk.config.FastRestartStoreResourcePool;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerManager;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerService;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr.FrsLocalConstants.*;

/**
 * A cache manager level service that provides support for frs backed offHeap stores to access the
 * backing FRS stores.
 * <p>
 * As of now, by default, multiple stores tied to different caches of the same cache manager access the
 * same underlying FRS store, unless the FRS pool is configured with different FRS containers.
 * <p>
 * Note: This service is available for ehcache customer(s) having enterprise license only.
 *
 * @author RKAV
 */
@ServiceDependencies(LocalPersistenceService.class)
public class DefaultRestartableStoreContainerService implements FastRestartStoreContainerService {
  private LocalMetadataManager metadataManager;
  private volatile LocalPersistenceService persistenceService;
  private Map<String, DefaultRestartableStoreContainerManager> restartStoreContainerMap;
  private Map<String, DestroyOnlyStoreContainerManager> destroyOnlyContainerMap;
  private volatile boolean inMaintenance = false;

  @Override
  public synchronized void start(final ServiceProvider<Service> serviceProvider) {
    internalStart(serviceProvider);
    inMaintenance = false;
  }

  @Override
  public synchronized void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    if (restartStoreContainerMap != null) {
      throw new IllegalStateException("Cannot start this service in maintenance mode as one or more FRS stores are open");
    }
    internalStart(serviceProvider);
    inMaintenance = true;
  }

  private void internalStart(ServiceProvider<? super MaintainableService> serviceProvider) {
    persistenceService = serviceProvider.getService(LocalPersistenceService.class);
    restartStoreContainerMap = new HashMap<>();
    destroyOnlyContainerMap = new HashMap<>();
    metadataManager = createMetaDataStore();
  }

  @Override
  public synchronized void stop() {
    internalStop();
    persistenceService = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized FastRestartStoreContainerManager getRestartableStoreManager(final FastRestartStoreSpaceIdentifier id) {
    if (restartStoreContainerMap == null || inMaintenance) {
      throw new IllegalStateException(
          "Fast Restart Store Service not started or in Maintenance. Cannot access FRS store manager(s)");
    }
    DefaultFastRestartStoreSpaceIdentifier identifier = (DefaultFastRestartStoreSpaceIdentifier)id;
    return restartStoreContainerMap.get(identifier.frsId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return EnterpriseDiskResourceType.Types.FRS.equals(resourceType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized PersistenceSpaceIdentifier<?> getPersistenceSpaceIdentifier(String name, CacheConfiguration<?, ?> config)
      throws CachePersistenceException {
    if (!isStarted() || inMaintenance) {
      return null;
    }
    final FastRestartStoreResourcePool frsPool = config.getResourcePools().getPoolForResource(
        EnterpriseDiskResourceType.Types.FRS);
    if (frsPool == null) {
      throw new IllegalArgumentException("No Fast restartable resource found for cache " + name);
    }
    DefaultRestartableStoreContainerManager managerForContainer;
    final String frsLoc = DEFAULT_FRS_DATA_LOG;
    removeContainerFromDestroyOnlyMap(frsLoc);
    managerForContainer = restartStoreContainerMap.get(frsLoc);
    if (managerForContainer == null) {
      managerForContainer = createNewFrsStore(frsLoc);
      restartStoreContainerMap.put(frsLoc, managerForContainer);
    }
    return new DefaultFastRestartStoreSpaceIdentifier(name, frsLoc);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier)
      throws CachePersistenceException {
    // nothing to do as we cannot release the underlying FRS store, due to recovery reasons
    // we can only attach/detach the stores at max, which is done when the actual store is released
    if (!(identifier instanceof DefaultFastRestartStoreSpaceIdentifier)) {
      throw new AssertionError("Unexpected identifier object ");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, final String name)
      throws CachePersistenceException {
    if (restartStoreContainerMap == null || inMaintenance) {
      throw new IllegalStateException(
          "Fast Restart Store Service not started or in Maintenance. Cannot access FRS store manager(s)");
    }
    if (identifier instanceof DefaultFastRestartStoreSpaceIdentifier) {
      final DefaultFastRestartStoreSpaceIdentifier frsIdentifier = (DefaultFastRestartStoreSpaceIdentifier)identifier;
      final String storeId = frsIdentifier.getId();
      final DefaultRestartableStoreContainerManager managerForContainer =
          restartStoreContainerMap.get(frsIdentifier.frsId);
      if (managerForContainer == null) {
        throw new IllegalStateException("Store with alias " + storeId + " already closed. Cannot use state repository");
      }
      return frsIdentifier.getOrCreateStateRepository(name, () -> new DefaultStateRepository(storeId, name, managerForContainer));
    } else {
      // unexpected internal error as wrong identifier type used by caller
      throw new AssertionError("Unexpected identifier object type " + identifier);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void destroy(final String storeId) throws CachePersistenceException {
    if (metadataManager == null) {
      throw new IllegalStateException(
          "Fast Restart Store Service not started with persistence." +
          "Cannot access FRS store manager(s) to destroy individual caches");
    }
    final SavedStoreConfig storeConfig = metadataManager.getRestartableStoreConfig(storeId);
    if (storeConfig == null) {
      // Nothing to do - unknown store
      return;
    }
    final DefaultRestartableStoreContainerManager storeManager = restartStoreContainerMap.get(storeConfig.getFrsId());
    if (storeManager == null) {
      // FRS is not opened yet
      DestroyOnlyStoreContainerManager destroyOnlyStoreContainerManager = destroyOnlyContainerMap.get(storeConfig.getFrsId());
      if (destroyOnlyStoreContainerManager == null) {
        destroyOnlyStoreContainerManager = createNewDestroyOnlyStore(storeConfig.getFrsId());
        destroyOnlyContainerMap.put(storeConfig.getFrsId(), destroyOnlyStoreContainerManager);
      }
      destroyOnlyStoreContainerManager.destroyStore(storeId);
    } else {
      storeManager.destroyStore(storeId);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void destroyAll() throws CachePersistenceException {
    final LocalPersistenceService localRef = persistenceService;
    if (localRef != null && inMaintenance) {
      // this must be done to ensure that the metadata file is closed as it is opened
      // even in maintenance mode.
      internalStop();
      localRef.destroyAll(FRS_OWNER);
    } else {
      throw new IllegalStateException("Cannot destroy FRS stores in normal mode");
    }
  }

  private void internalStop() {
    // close all cache stores
    final Map<String, DefaultRestartableStoreContainerManager> localMap = restartStoreContainerMap;
    restartStoreContainerMap = null;
    closeAllFrsStoreContainers(localMap);
    final Map<String, DestroyOnlyStoreContainerManager> localDestroyMap = destroyOnlyContainerMap;
    destroyOnlyContainerMap = null;
    closeAllFrsStoreContainers(localDestroyMap);

    // close meta data store
    final LocalMetadataManager localMgr = metadataManager;
    if (localMgr != null) {
      metadataManager = null;
      localMgr.close();
    }
  }

  private void removeContainerFromDestroyOnlyMap(String frsId) {
    final DestroyOnlyStoreContainerManager storeContainer = destroyOnlyContainerMap.remove(frsId);
    if (storeContainer != null) {
      storeContainer.closeContainer();
    }
  }

  /**
   * Create or open an existing metadata store
   *
   * @return meta data store manager
   */
  private LocalMetadataManager createMetaDataStore() {
    SafeSpaceIdentifier safeSpaceId = persistenceService.createSafeSpaceIdentifier(FRS_OWNER, DEFAULT_CONTAINER);
    try {
      persistenceService.createSafeSpace(safeSpaceId);
    } catch (CachePersistenceException e) {
      throw new RuntimeException(e);
    }
    LocalMetadataManager metadataManager = new LocalMetadataManager(
        new DefaultFileBasedPersistenceContext(safeSpaceId.getRoot()));
    metadataManager.init();
    return metadataManager;
  }

  /**
   * Create a new FRS store under the given storage space (i.e filesystem).
   *
   * @param space name of the space
   * @return An FRS manager that manages the created FRS container
   * @throws CachePersistenceException in case FRS files cannot be created/read.
   */
  private DefaultRestartableStoreContainerManager createNewFrsStore(String space) throws CachePersistenceException {
    SafeSpaceIdentifier safeSpaceId = persistenceService.createSafeSpaceIdentifier(FRS_OWNER, DEFAULT_CONTAINER);
    persistenceService.createSafeSpace(safeSpaceId);
    DefaultRestartableStoreContainerManager mgr = new DefaultRestartableStoreContainerManager(metadataManager,
        new DefaultFileBasedPersistenceContext(safeSpaceId.getRoot()), space);
    mgr.init();
    return mgr;
  }

  private DestroyOnlyStoreContainerManager createNewDestroyOnlyStore(String space) throws CachePersistenceException {
    SafeSpaceIdentifier safeSpaceId = persistenceService.createSafeSpaceIdentifier(FRS_OWNER, DEFAULT_CONTAINER);
    if (!safeSpaceId.getRoot().exists()) {
      throw new CachePersistenceException("No FRS container space found for " + space);
    }
    final DestroyOnlyStoreContainerManager mgr = new DestroyOnlyStoreContainerManager(
        new DefaultFileBasedPersistenceContext(safeSpaceId.getRoot()), metadataManager, space);
    mgr.init();
    return mgr;
  }

  /**
   * Close all store container managers.
   *
   * @param storeContainerMap Map of store containers.
   */
  private <K, V extends CloseFrsContainer> void closeAllFrsStoreContainers(final Map<K, V> storeContainerMap) {
    if (storeContainerMap != null) {
      for (V container : storeContainerMap.values()) {
        container.closeContainer();
      }
      storeContainerMap.clear();
    }
  }

  private boolean isStarted() {
    return persistenceService != null;
  }

  /**
   * Inner class representing a state repository.
   * <p>
   *   Each instance represents a unique state repository for a given {@code storeId} and {@code name} combination.
   */
  private static final class DefaultStateRepository implements StateRepository {
    private static final String DIVIDER = "-";
    private final String storeId;
    private final String composedId;
    private final DefaultRestartableStoreContainerManager managerForRepository;

    private DefaultStateRepository(final String storeId, final String name, final DefaultRestartableStoreContainerManager managerForRepository) {
      this.storeId = storeId;
      this.composedId = storeId + DIVIDER + name;
      this.managerForRepository = managerForRepository;
    }

    @Override
    public <K extends Serializable, V extends Serializable> StateHolder<K, V> getPersistentStateHolder(
        String name, Class<K> keyClass, Class<V> valueClass, Predicate<Class<?>> isClassPermitted, ClassLoader classLoader) {
      String uniqueId = composedId + DIVIDER + name;
      return managerForRepository.getLocalStateRepository().
          getOrCreateRestartableMap(storeId, uniqueId, keyClass, valueClass);
    }
  }

  /**
   * FRS per store (per Cache) identifier.
   */
  private static final class DefaultFastRestartStoreSpaceIdentifier implements FastRestartStoreSpaceIdentifier {
    private final String storeId;
    private final String frsId;
    private final Map<String, DefaultStateRepository> repositoryMap;

    private DefaultFastRestartStoreSpaceIdentifier(String id, String frsId) {
      this.storeId = id;
      this.frsId = frsId;
      this.repositoryMap = new HashMap<>();
    }

    @Override
    public String toString() {
      return storeId;
    }

    @Override
    public Class<FastRestartStoreContainerService> getServiceType() {
      return FastRestartStoreContainerService.class;
    }

    @Override
    public String getId() {
      return storeId;
    }

    StateRepository getOrCreateStateRepository(String name, Supplier<DefaultStateRepository> supplier) {
      DefaultStateRepository sr = repositoryMap.computeIfAbsent(name, k -> supplier.get());
      return sr;
    }
  }

  /**
   * File context necessary for each FRS container.
   */
  private static class DefaultFileBasedPersistenceContext implements FileBasedPersistenceContext {
    private final File directory;

    private DefaultFileBasedPersistenceContext(File directory) {
      this.directory = directory;
    }

    @Override
    public File getDirectory() {
      return directory;
    }
  }
}
