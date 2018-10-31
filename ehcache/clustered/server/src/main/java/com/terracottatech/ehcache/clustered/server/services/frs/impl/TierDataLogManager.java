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
package com.terracottatech.ehcache.clustered.server.services.frs.impl;

import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.offheap.LongPortability;
import org.ehcache.clustered.server.offheap.OffHeapChainMap;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.util.Factory;

import com.tc.classloader.CommonComponent;
import com.terracottatech.br.ssi.frs.RestartStoreLocator;
import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.server.ResourcePoolManager;
import com.terracottatech.ehcache.clustered.server.ResourcePoolManager.ResourcePoolMode;
import com.terracottatech.ehcache.clustered.server.offheap.frs.KeyToSegment;
import com.terracottatech.ehcache.clustered.server.offheap.frs.OffHeapChainMapStripe;
import com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableChainStorageEngine;
import com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableHybridChainStorageEngine;
import com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableOffHeapChainStorageEngine;
import com.terracottatech.ehcache.clustered.server.services.frs.IdentifierComposer;
import com.terracottatech.ehcache.clustered.server.services.frs.PerTierDataObjects;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerSideConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerStoreConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableStateRepositoryConfig;
import com.terracottatech.ehcache.clustered.server.services.frs.management.Management;
import com.terracottatech.ehcache.clustered.server.services.pool.ResourcePoolManagerConfiguration;
import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;
import com.terracottatech.ehcache.common.frs.RestartLogLifecycle;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import com.terracottatech.ehcache.common.frs.RootPathProvider;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProvider;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.RegisterableObjectManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.terracottatech.ehcache.common.frs.FrsCodecFactory.toByteBuffer;
import static com.terracottatech.frs.config.FrsProperty.IO_NIO_ACCESS_METHOD;
import static com.terracottatech.frs.config.FrsProperty.IO_NIO_POOL_MEMORY_SIZE;
import static com.terracottatech.frs.config.FrsProperty.IO_NIO_RANDOM_ACCESS_MEMORY_SIZE;
import static com.terracottatech.frs.config.FrsProperty.IO_NIO_RECOVERY_MEMORY_SIZE;
import static com.terracottatech.frs.config.FrsProperty.IO_RANDOM_ACCESS;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;

/**
 * Creates and manages the cache data fast restart log.
 */
@CommonComponent
public final class TierDataLogManager implements RestartStoreLocator<ByteBuffer, ByteBuffer, ByteBuffer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TierDataLogManager.class);

  private static final String TIER_PREFIX = "__tier__";
  private static final String STATE_REPOSITORY_PREFIX = "__sr__";
  private static final String INVALIDATION_TRACKER_PREFIX = "__invalidation_tracker__";
  private static final String DIVIDER = ":";

  private final RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> cacheObjectManager;
  private final MetadataProvider<String, RestartableServerSideConfiguration, RestartableServerStoreConfiguration> metadataProvider;
  private final FrsDataLogIdentifier dataLogIdentifier;
  private final RootPathProvider pathProvider;
  private final ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> cacheDataStoreArea;
  private final RestartLogLifecycle dataStoreLifecycle;
  private final ServiceRegistry serviceRegistry;
  private final Map<String, Map<String, PerTierDataObjects>> allDataObjects;
  private final Management management;
  private final Path relativeFromRoot;

  // requires this long mapper to work around the non generic nature of ehcache's KeySegmentMapper
  // as the OffheapChainMapStripe uses generics.
  private volatile KeyToSegmentLongMapper longMapper;

  public TierDataLogManager(FrsDataLogIdentifier dataLogIdentifier,
                            MetadataProvider<String, RestartableServerSideConfiguration, RestartableServerStoreConfiguration> metadataProvider,
                            RootPathProvider pathProvider,
                            ServiceRegistry serviceRegistry, boolean randomAccess, boolean largeInstallation, Management management) {
    this.metadataProvider = metadataProvider;
    this.dataLogIdentifier = dataLogIdentifier;
    this.pathProvider = pathProvider;
    this.serviceRegistry = serviceRegistry;
    this.management = management;
    this.cacheObjectManager = new RegisterableObjectManager<>();
    this.cacheDataStoreArea = initCacheStore(randomAccess, largeInstallation);
    this.dataStoreLifecycle = new RestartLogLifecycle(idToDataFile(), cacheDataStoreArea);
    this.allDataObjects = new HashMap<>();
    this.relativeFromRoot = Paths.get(dataLogIdentifier.getRootPathName())
        .resolve(pathProvider.getConfiguredRoot(dataLogIdentifier.getRootPathName()).toPath().relativize(idToDataFile()
            .toPath()));
  }

  /**
   * Initialize the cache data area where all cache data are stored.
   */
  public void bootstrap(Map<String, RestartableServerSideConfiguration> tierManagers, KeySegmentMapper mapper) {
    if (longMapper == null) {
      // bootstrap can only be done once
      longMapper = new KeyToSegmentLongMapper(mapper);
      bootstrapAllDataObjects(tierManagers);
      this.dataStoreLifecycle.startStoreAsync();
    }
  }

  public void bootstrapEmpty(KeySegmentMapper mapper) {
    if (longMapper == null) {
      longMapper = new KeyToSegmentLongMapper(mapper);
      this.dataStoreLifecycle.startStore();
    }
  }

  /**
   * Close and shutdown the cache data area of FRS.
   */
  public void close() {
    LOGGER.info("Shutting down cache store {}", dataLogIdentifier);
    bootstrapWait();
    this.management.deRegisterRestartStore(dataLogIdentifier);
    this.dataStoreLifecycle.shutStore();
  }

  /**
   * Wait for the boot strapping process to complete for the container.
   */
  public void bootstrapWait() {
    this.dataStoreLifecycle.waitOnCompletion();
  }

  public Map<String, PerTierDataObjects> getDetachedDataObjectsForTierManager(String tierManagerId) {
    Map<String, PerTierDataObjects> map = allDataObjects.getOrDefault(tierManagerId, Collections.emptyMap());
    return map.entrySet().stream()
        .filter((e) -> !e.getValue().isAttached())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public void attachTierManager(String tierManagerId) {
    final Map<String, PerTierDataObjects> mgrMap = allDataObjects.get(tierManagerId);
    if (mgrMap != null) {
      for (PerTierDataObjects p : mgrMap.values()) {
        ((IndividualTierData)p).attach();
      }
    }
  }

  public PerTierDataObjects addTier(String tierManagerId, String tierId,
                             RestartableServerStoreConfiguration storeConfiguration) throws ConfigurationException {
    Map<String, PerTierDataObjects> mgrMap = allDataObjects.get(tierManagerId);
    if (mgrMap == null || !mgrMap.containsKey(tierId)) {
      if (mgrMap == null) {
        // first time any object of this tier manager is created..
        mgrMap = new HashMap<>();
        allDataObjects.put(tierManagerId, mgrMap);
      }
      mgrMap.put(tierId, createChainMap(tierManagerId,
          IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId), storeConfiguration, false));
    }
    return mgrMap.get(tierId);
  }

  public RestartableGenericMap<Object, Object> addStateRepository(String tierManagerId, String tierId, String mapId) {
    Map<String, PerTierDataObjects> mgrMap = allDataObjects.get(tierManagerId);
    if (mgrMap == null || !mgrMap.containsKey(tierId)) {
      // this cannot happen for state repository
      throw new AssertionError("Tier Manager map not found!");
    }
    IndividualTierData data = (IndividualTierData)mgrMap.get(tierId);
    final String composedRepoId = IdentifierComposer.repoIdToComposedRepoId(tierManagerId, tierId, mapId);
    RestartableGenericMap<Object, Object> repoMap = data.getStateRepositories().get(mapId);
    if (repoMap == null) {
      repoMap = createRestartableMap(toStateRepositoryIdentifier(composedRepoId));
      data.addStateRepositoryMap(mapId, repoMap);
    }
    return repoMap;
  }

  public boolean destroyTierManager(String tierManagerId) {
    Map<String, PerTierDataObjects> mgrMap = allDataObjects.remove(tierManagerId);
    if (mgrMap != null) {
      mgrMap.forEach((tierId, tierData) -> destroyTier(tierManagerId, tierId, tierData));
    }
    return allDataObjects.size() <= 0;
  }

  private void destroyStateRepositoryData(String tierManagerId, String tierId,
                                          Map<String, RestartableGenericMap<Object, Object>> stateRepositories) {
    stateRepositories.forEach((repoId, repo) -> {
      final String composedIdentifier = IdentifierComposer.repoIdToComposedRepoId(tierManagerId, tierId, repoId);
      final ByteBuffer repoIdentifier = toStateRepositoryIdentifier(composedIdentifier);
      repo.clear();
      cacheObjectManager.unregisterStripe(repoIdentifier);
    });
  }

  public void destroyTier(String tierManagerId, String tierId, PerTierDataObjects tierData) {
    destroyStateRepositoryData(tierManagerId, tierId, tierData.getStateRepositories());
    cacheObjectManager.unregisterStripe(toInvalidationTrackerIdentifier(tierManagerId, tierId));
    final String composedIdentifier = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    final ByteBuffer tierIdentifier = toTierIdentifier(composedIdentifier);
    cacheObjectManager.unregisterStripe(tierIdentifier);
  }

  public void destroyTier(String tierManagerId, String tierId) {
    Map<String, PerTierDataObjects> mgrMap = allDataObjects.get(tierManagerId);
    if (mgrMap != null && mgrMap.containsKey(tierId)) {
      destroyTier(tierManagerId, tierId, mgrMap.remove(tierId));
    }
  }

  public void beginGlobalTransaction(String composedTierId) {
    cacheDataStoreArea.beginGlobalTransaction(true, toTierIdentifier(composedTierId));
  }

  public void endGlobalTransaction(String composedTierId) {
    try {
      cacheDataStoreArea.commitGlobalTransaction(toTierIdentifier(composedTierId));
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  public void beginTransaction(String composedTiedId) {
    cacheDataStoreArea.beginControlledTransaction(true, toTierIdentifier(composedTiedId));
  }

  public void endTransaction(String composedTierId) {
    try {
      cacheDataStoreArea.commitControlledTransaction(toTierIdentifier(composedTierId));
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private void bootstrapAllDataObjects(Map<String, RestartableServerSideConfiguration> tierManagers) {
    tierManagers.forEach((mgrId, config) -> {
      final Map<String, RestartableServerStoreConfiguration> tierMap =
          metadataProvider.getFirstLevelChildConfigurationByRoot(mgrId);
      tierMap.forEach((tierId, tierConfig) -> {
        bootstrapChainMap(mgrId, tierId, tierConfig);
        Map<String, RestartableStateRepositoryConfig> repoMaps =
            metadataProvider.getNthLevelOtherChildConfigurationByParent(tierId, RestartableStateRepositoryConfig.class);
        repoMaps.forEach((repoId, repoConfig) -> bootstrapRepoMap(mgrId, tierId, repoId, repoConfig));
      });
    });
  }

  private void bootstrapRepoMap(String tierManagerId, String composedTierId,
                                String composedRepoId,
                                RestartableStateRepositoryConfig repoConfig) {
    final String tierId = IdentifierComposer.composedTierIdToTierId(tierManagerId, composedTierId);
    final String repoMapId = IdentifierComposer.composedRepoIdToRepoId(tierManagerId, tierId, composedRepoId);
    final Map<String, PerTierDataObjects> mgrMap = allDataObjects.get(tierManagerId);
    // mgrMap cannot be null as chain map is already bootstrapped
    IndividualTierData dataObjects = (IndividualTierData)mgrMap.get(tierId);
    RestartableGenericMap<Object, Object> repoMap = createRestartableMap(
        toStateRepositoryIdentifier(composedRepoId, repoConfig));
    dataObjects.addStateRepositoryMap(repoMapId, repoMap);
  }

  private void bootstrapChainMap(String tierManagerId, String composedTierId,
                                 RestartableServerStoreConfiguration tierConfig) {
    final PerTierDataObjects dataObjects;
    try {
      dataObjects = createChainMap(tierManagerId, composedTierId, tierConfig, true);
    } catch (ConfigurationException e) {
      throw new RuntimeException(String.format("Unable to allocate page source during recovery of \'%1$s\' " +
                                               "of tier manager \'%2$s\'", composedTierId, tierManagerId), e);
    }
    if (dataObjects == null) {
      return;
    }
    Map<String, PerTierDataObjects> mgrMap = allDataObjects.computeIfAbsent(tierManagerId, k -> new HashMap<>());
    final String tierId = IdentifierComposer.composedTierIdToTierId(tierManagerId, composedTierId);
    mgrMap.put(tierId, dataObjects);
  }

  private <K, V> RestartableGenericMap<K, V> createRestartableMap(Class<K> keyClass,
                                                                  Class<V> valueClass,
                                                                  ByteBuffer identifier, boolean synchronousWrites) {
    RestartableGenericMap<K, V> mapToReturn = new RestartableGenericMap<>(keyClass, valueClass,
            identifier, this.cacheDataStoreArea, synchronousWrites);
    cacheObjectManager.registerObject(mapToReturn);
    return mapToReturn;
  }

  private RestartableGenericMap<Object, Object> createRestartableMap(ByteBuffer identifier) {
    return createRestartableMap(Object.class, Object.class, identifier, true);
  }

  private PerTierDataObjects createChainMap(String tierManagerId, String composedTierId,
                                            RestartableServerStoreConfiguration tierConfig, boolean detached) throws ConfigurationException {
    final PoolAllocation poolAllocation = tierConfig.getServerStoreConfiguration().getPoolAllocation();
    if(poolAllocation instanceof EnterprisePoolAllocation.DedicatedRestartable||
       poolAllocation instanceof EnterprisePoolAllocation.SharedRestartable) {
      final String tierId = IdentifierComposer.composedTierIdToTierId(tierManagerId, composedTierId);

      final ResourcePoolManager resourcePoolManager;
      try {
        resourcePoolManager = this.serviceRegistry.getService(new ResourcePoolManagerConfiguration(tierManagerId));
      } catch (ServiceException e) {
        throw new ConfigurationException("Failed to obtain the singleton ResourcePoolManager instance for " + tierManagerId, e);
      }

      PageSource cachingPageSource = null;
      ResourcePoolMode rpMode = resourcePoolManager.getResourcePoolMode();
      final ResourcePageSource resourcePageSource = resourcePoolManager.getPageSource(tierId,
          tierConfig.getServerStoreConfiguration().getPoolAllocation());
      if (rpMode.equals(ResourcePoolMode.RESTARTABLE_PARTIAL)) {
        cachingPageSource = resourcePoolManager.getCachingPageSource(tierId,
            tierConfig.getServerStoreConfiguration()
            .getPoolAllocation());
      }
      long maxSize = ResourcePoolManager.getMaxSize(resourcePageSource.getPool().getSize());
      final ByteBuffer tierIdentifier = toTierIdentifier(composedTierId);
      final Factory<? extends RestartableChainStorageEngine<ByteBuffer, Long>> storageEngineFactory;
      if (rpMode.equals(ResourcePoolMode.RESTARTABLE_FULL)) {
        storageEngineFactory = RestartableOffHeapChainStorageEngine.createFactory(resourcePageSource,
            LongPortability.INSTANCE, KILOBYTES.toBytes(4), (int) (maxSize), false, false, tierIdentifier,
            cacheDataStoreArea, true);
      } else {
        storageEngineFactory = RestartableHybridChainStorageEngine.createFactory(resourcePageSource,
            LongPortability.INSTANCE, KILOBYTES.toBytes(4), (int) (maxSize), false, false, tierIdentifier,
            cacheDataStoreArea, true, cachingPageSource);
      }

      OffHeapChainMapStripe<ByteBuffer, Long> stripe = new OffHeapChainMapStripe<>(tierIdentifier,
          this.longMapper, resourcePageSource, LongPortability.INSTANCE, storageEngineFactory);
      cacheObjectManager.registerObject(stripe);

      RestartableGenericMap<Long, Integer> invalidationTracker = createRestartableMap(Long.class, Integer.class,
          toInvalidationTrackerIdentifier(tierManagerId, tierId), true);

      return new IndividualTierData(tierConfig.getServerStoreConfiguration(), resourcePageSource, stripe,
          invalidationTracker, detached);
    } else {
      // not failing startup as of now for individual object issues
      LOGGER.error("Internal Error: Unexpected non restartable pools seen during recovery for \'{}\'", composedTierId);
      return null;
    }
  }

  private ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> initCacheStore(boolean randomAccess,
                                                                                               boolean largeInstallation) {
    File logDirectory = idToDataFile();
    boolean exists = logDirectory.mkdirs();
    LOGGER.info("{} store area for caches at location {}", exists ? "Loading" : "Creating", logDirectory);
    Properties frsConfiguration = new Properties();
    frsConfiguration.setProperty(IO_NIO_POOL_MEMORY_SIZE.shortName(), Long.toString(64L * 1024 * 1024));
    if (randomAccess) {
      frsConfiguration.setProperty(IO_NIO_ACCESS_METHOD.shortName(), "STREAM");
      frsConfiguration.setProperty(IO_RANDOM_ACCESS.shortName(), "true");
      if (largeInstallation) {
        frsConfiguration.setProperty(IO_NIO_RANDOM_ACCESS_MEMORY_SIZE.shortName(), Long.toString(256L * 1024 * 1024));
        frsConfiguration.setProperty(IO_NIO_RECOVERY_MEMORY_SIZE.shortName(), Long.toString(1024 * 1024 * 1024));
      }
    } else {
      frsConfiguration.setProperty(IO_NIO_ACCESS_METHOD.shortName(), "STREAM");
    }
    final ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> dataLog;
    try {
      dataLog = new ControlledTransactionRestartStore<>(
          RestartStoreFactory.createStore(cacheObjectManager, logDirectory, frsConfiguration));
      management.registerRestartStore(dataLogIdentifier, logDirectory, dataLog);
    } catch (IOException | RestartStoreException e) {
      throw new RuntimeException("Cannot initialize FRS store area for cache data", e);
    }
    return dataLog;
  }

  private File idToDataFile() {
    final File rootFile = pathProvider.getRootPath(dataLogIdentifier.getRootPathName());
    if (rootFile == null) {
      throw new RuntimeException(String.format("Root path \'%1$s\' missing in the configuration file." +
                                               " Unable to open data log file", dataLogIdentifier.getRootPathName()));
    }
    return new File(new File(rootFile, dataLogIdentifier.getContainerName()), dataLogIdentifier.getDataLogName());
  }

  private ByteBuffer toTierIdentifier(String composedTierId) {
    return toByteBuffer(TIER_PREFIX + composedTierId + DIVIDER +
                        metadataProvider.getChildConfigEntry(dataLogIdentifier, composedTierId).getObjectIndex());
  }

  private ByteBuffer toInvalidationTrackerIdentifier(String tierManagerId, String tierId) {
    return toByteBuffer(INVALIDATION_TRACKER_PREFIX + tierId + tierManagerId);
  }

  private ByteBuffer toStateRepositoryIdentifier(String composedRepoId) {
    return toByteBuffer(STATE_REPOSITORY_PREFIX + composedRepoId + DIVIDER +
                        metadataProvider.getNthLevelOtherChildConfigEntry(dataLogIdentifier, composedRepoId,
                            RestartableStateRepositoryConfig.class).getObjectIndex());
  }

  private ByteBuffer toStateRepositoryIdentifier(String composedRepoId, RestartableStateRepositoryConfig config) {
    return toByteBuffer(STATE_REPOSITORY_PREFIX + composedRepoId + DIVIDER + config.getObjectIndex());
  }

  @Override
  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getStore() {
    return cacheDataStoreArea;
  }

  @Override
  public Path getRelativeToRoot() {
    return relativeFromRoot;
  }

  private static class IndividualTierData implements PerTierDataObjects {
    private final ServerStoreConfiguration serverStoreConfiguration;
    private final ResourcePageSource pageSource;
    private final RestartableGenericMap<Long, Integer> invalidationTrackerMap;
    private final Map<String, RestartableGenericMap<Object, Object>> repoMaps;
    private final OffHeapChainMapStripe<ByteBuffer, Long> chainMapStripe;
    private boolean detached = true;

    private IndividualTierData(ServerStoreConfiguration serverStoreConfiguration, ResourcePageSource pageSource,
                               OffHeapChainMapStripe<ByteBuffer, Long> chainMapStripe,
                               RestartableGenericMap<Long, Integer> invalidationTrackerMap,
                               boolean detached) {
      this.serverStoreConfiguration = serverStoreConfiguration;
      this.pageSource = pageSource;
      this.repoMaps = new HashMap<>();
      this.chainMapStripe = chainMapStripe;
      this.invalidationTrackerMap = invalidationTrackerMap;
      this.detached = detached;
    }

    @Override
    public OffHeapChainMapStripe<ByteBuffer, Long> getStripe() {
      return chainMapStripe;
    }

    @Override
    public Map<String, RestartableGenericMap<Object, Object>> getStateRepositories() {
      return repoMaps;
    }

    @Override
    public RestartableGenericMap<Long, Integer> getInvalidationTrackerMap() {
      return invalidationTrackerMap;
    }

    @Override
    public ServerStoreConfiguration getServerStoreConfiguration() {
      return serverStoreConfiguration;
    }

    @Override
    public ResourcePageSource getPageSource() {
      return pageSource;
    }

    @Override
    public List<OffHeapChainMap<Long>> getRecoveredMaps() {
      return chainMapStripe.segments();
    }

    @Override
    public boolean isAttached() {
      return !detached;
    }

    private void addStateRepositoryMap(String repoMapId, RestartableGenericMap<Object, Object> repoMap) {
      repoMaps.put(repoMapId, repoMap);
    }

    private void attach() {
      detached = false;
    }
  }

  private static final class KeyToSegmentLongMapper implements KeyToSegment<Long> {
    private final KeySegmentMapper mapper;

    private KeyToSegmentLongMapper(KeySegmentMapper mapper) {
      this.mapper = mapper;
    }

    @Override
    public int getSegmentForKey(Long key) {
      return mapper.getSegmentForKey(key);
    }

    @Override
    public int getSegments() {
      return mapper.getSegments();
    }
  }
}
