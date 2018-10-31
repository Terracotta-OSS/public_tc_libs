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

import org.ehcache.clustered.server.KeySegmentMapper;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;

import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.br.ssi.frs.FRSBackupCoordinator;
import com.terracottatech.br.ssi.frs.RestartStoreLocator;
import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSService;
import com.terracottatech.ehcache.clustered.server.services.frs.IdentifierComposer;
import com.terracottatech.ehcache.clustered.server.services.frs.PerTierDataObjects;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerSideConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerStoreConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableStateRepositoryConfig;
import com.terracottatech.ehcache.clustered.server.services.frs.management.Management;
import com.terracottatech.ehcache.clustered.server.services.pool.ResourcePoolManagerConfiguration;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import com.terracottatech.ehcache.common.frs.RootPathProvider;
import com.terracottatech.ehcache.common.frs.metadata.FrsContainerIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProvider;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProviderBuilder;
import com.terracottatech.frs.RestartStore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSServiceProvider.EHCACHE_FRS_STORAGE_SPACE;

/**
 * Handles all CRUD interactions with FRS for all clustering tier managers on this server, in addition to handling
 * restart and recovery and rebuilding state on restart.
 * <p>
 *   Multiple clustering tier managers will interact with this thread safe service.
 */
public class EhcacheFRSServiceImpl implements EhcacheFRSService {
  private final MetadataProvider<String, RestartableServerSideConfiguration, RestartableServerStoreConfiguration> metadataProvider;
  private final ServiceRegistry serviceRegistry;
  private final Map<FrsDataLogIdentifier, TierDataLogManager> dataLogManagerMap;
  private final Map<String, Set<FrsContainerIdentifier>> containerMap;
  private final Map<String, String> frsIdToEntityMapping;
  private final Map<String, FrsDataLogIdentifier> tierToDataLogMapping;
  private final FRSRootPathProvider rootPathProvider;
  private final boolean largeInstallation;
  private final Management management;

  private volatile Set<FrsDataLogIdentifier> frsDataLogIdentifiers = null;

  private volatile boolean bootstrappedData = false;

  public EhcacheFRSServiceImpl(DataDirectories dataDirectories, ServiceRegistry serviceRegistry,
                               boolean largeInstallation, Management management) {
    this.management = management;
    this.rootPathProvider = new FRSRootPathProvider(dataDirectories);
    this.metadataProvider = MetadataProviderBuilder.metadata(this.rootPathProvider,
      String.class,
      RestartableServerSideConfiguration.class,
      RestartableServerStoreConfiguration.class).
      withOtherChildType(RestartableStateRepositoryConfig.class).build();
    this.serviceRegistry = serviceRegistry;
    this.dataLogManagerMap = new HashMap<>();
    this.containerMap = new HashMap<>();
    this.largeInstallation = largeInstallation;
    this.frsIdToEntityMapping = new HashMap<>();
    this.tierToDataLogMapping = new ConcurrentHashMap<>();
  }

  @Override
  public MetadataProvider<String, RestartableServerSideConfiguration, RestartableServerStoreConfiguration>
  getMetadataProvider() {
    return metadataProvider;
  }

  @Override
  public synchronized void bootstrapMetaData() {
    if (frsDataLogIdentifiers == null) {
      frsDataLogIdentifiers = metadataProvider.bootstrapAllContainers();
    }
  }

  // tierManagerId maps to frsIdentifier and tierManagerEntityId maps to the entity identifier
  @Override
  public synchronized void configure(String tierManagerId, String tierManagerEntityId, RestartableServerSideConfiguration serverSideConfiguration)
      throws ConfigurationException {
    try {
      String existingEntityMapping = frsIdToEntityMapping.get(tierManagerId);
      if (existingEntityMapping == null || existingEntityMapping.equals(tierManagerEntityId)) {
        RestartableServerSideConfiguration existing = metadataProvider.getAllRootConfiguration().get(tierManagerId);
        if (existing != null && existing.isDestroyInProgress()) {
          serverSideConfiguration.setDestroyInProgress(true);
        }
        metadataProvider.addRootConfiguration(tierManagerId, serverSideConfiguration);
        frsIdToEntityMapping.put(tierManagerId, tierManagerEntityId);
      } else {
        throw new ConfigurationException("Cluster Tier Manager Entity Mapping \'" + existingEntityMapping +
                                         "\' already exists for Cluster Tier Manager Fast Restart Store Identifier \'" +
                                         tierManagerId + "\'. Cannot map to another Cluster Tier Manager Entity \'" +
                                         tierManagerEntityId + "\'");
      }
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException(e.getMessage() + "; Cannot configure Fast Restart Store for clustered tier manager \'" +
                                       tierManagerId +
                                       "\' due to invalid configuration specified by client.");
    }
    Set<FrsContainerIdentifier> containers = containerMap.computeIfAbsent(tierManagerId, k -> new HashSet<>());
    // currently we support only one container and data log containers do not change
    containers.clear();
    containers.addAll(serverSideConfiguration.getContainers());
  }

  @Override
  public void beginGlobalTransaction(String tierManagerId, String tierId) {
    String composedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    dataLogManagerMap.get(getDataLogIdentifierForTier(tierManagerId, composedTierId)).beginGlobalTransaction(composedTierId);
  }

  @Override
  public void endGlobalTransaction(String tierManagerId, String tierId) {
    String composedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    dataLogManagerMap.get(getDataLogIdentifierForTier(tierManagerId, composedTierId)).endGlobalTransaction(composedTierId);
  }

  @Override
  public void beginTransaction(String tierManagerId, String tierId) {
    String composedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    dataLogManagerMap.get(getDataLogIdentifierForTier(tierManagerId, composedTierId)).beginTransaction(composedTierId);
  }

  @Override
  public void endTransaction(String tierManagerId, String tierId) {
    String composedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    dataLogManagerMap.get(getDataLogIdentifierForTier(tierManagerId, composedTierId)).endTransaction(composedTierId);
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    Collection<RestartStoreLocator<?, ?, ?>> restartStoreLocators =
        dataLogManagerMap.values().stream().collect(Collectors.toList());
    restartStoreLocators.addAll(
        metadataProvider.getMetaStores().entrySet().stream().map(this::metaDataStoreLocator).collect(Collectors.toList()));
    return new FRSBackupCoordinator(restartStoreLocators, "ehcache");
  }

  @Override
  public synchronized void bootstrapData(KeySegmentMapper mapper) {
    if (frsDataLogIdentifiers == null) {
      frsDataLogIdentifiers = metadataProvider.bootstrapAllContainers();
    }
    if (!bootstrappedData) {
      // bootstrap only once
      bootstrappedData = true;

      // get all root configuration
      final Map<String, RestartableServerSideConfiguration> tierManagers = this.metadataProvider.getAllRootConfiguration();
      createResourcePoolManagers(tierManagers);

      // bootstrap log by log
      frsDataLogIdentifiers.forEach((log) -> {
        Map<String, RestartableServerSideConfiguration> filteredMap = tierManagers.entrySet().stream()
            .filter((x) -> x.getValue().getContainers().contains(log2container(log)))
            .filter((x) -> x.getValue()
                .getServerSideConfiguration().getRestartConfiguration()
                .getRestartableLogName().equals(log.getDataLogName()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        boolean randomAccess = filteredMap.values().stream()
            .findFirst().map((s) -> s.getServerSideConfiguration().getRestartConfiguration().isHybrid()).orElse(false);

        final TierDataLogManager dataLogManager = new TierDataLogManager(log, metadataProvider, rootPathProvider,
            serviceRegistry, randomAccess, largeInstallation, management);
        dataLogManagerMap.put(log, dataLogManager);


        dataLogManager.bootstrap(filteredMap, mapper);
      });

      // wait for all bootstrap to complete
      dataLogManagerMap.values().forEach(TierDataLogManager::bootstrapWait);
    }
  }

  @Override
  public synchronized Map<String, PerTierDataObjects> recoverOrAdd(String tierManagerId,
                                                      RestartableServerSideConfiguration serverSideConfiguration) {
    RestartableServerSideConfiguration existing = metadataProvider.getAllRootConfiguration().get(tierManagerId);
    if (existing != null && existing.isDestroyInProgress()) {
      serverSideConfiguration.setDestroyInProgress(true);
    }
    metadataProvider.addRootConfiguration(tierManagerId, serverSideConfiguration);
    Set<FrsContainerIdentifier> containers = containerMap.computeIfAbsent(tierManagerId, k -> new HashSet<>());
    // currently we support only one container and data log containers do not change
    containers.clear();
    containers.addAll(serverSideConfiguration.getContainers());

    final Map<String, PerTierDataObjects> mapCopy = new HashMap<>();
    dataLogManagerMap.forEach((log, tierLog) -> {
      if (serverSideConfiguration.getContainers().contains(log2container(log))) {
        // a tier manager will be found only in one data log..but be generic and continue
        // now for future expansion
        final Map<String, PerTierDataObjects> detachedTiers =
            tierLog.getDetachedDataObjectsForTierManager(tierManagerId);
        tierLog.attachTierManager(tierManagerId);
        mapCopy.putAll(detachedTiers);
      }
    });
    return mapCopy;
  }

  @Override
  public synchronized PerTierDataObjects getOrCreateServerStore(
      String tierManagerId, String tierId, KeySegmentMapper mapper,
      RestartableServerStoreConfiguration storeConfiguration, boolean hybrid) throws ConfigurationException {
    final Set<FrsContainerIdentifier> containers = containerMap.get(tierManagerId);
    if (containers == null || !containers.contains(log2container(storeConfiguration.getDataLogIdentifier()))) {
      // this cannot happen
      throw new AssertionError("Internal Error: Container(s) not found for clustered tier. Currently no support for " +
                               "multiple containers");
    }
    // add to metadata
    String tierIdToComposedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    metadataProvider.addFirstLevelChildConfiguration(tierIdToComposedTierId, storeConfiguration);
    try {
      TierDataLogManager logManager = dataLogManagerMap.get(storeConfiguration.getDataLogIdentifier());
      if (logManager == null) {
        // no data log yet...create one
        logManager = new TierDataLogManager(storeConfiguration.getDataLogIdentifier(),
            metadataProvider, rootPathProvider, serviceRegistry, hybrid, largeInstallation, management);
        logManager.bootstrapEmpty(mapper);
        dataLogManagerMap.put(storeConfiguration.getDataLogIdentifier(), logManager);
      }
      return logManager.addTier(tierManagerId, tierId, storeConfiguration);
    } catch(Throwable t) {
      metadataProvider.removeChildConfigurationFrom(storeConfiguration.getDataLogIdentifier(), tierIdToComposedTierId);
      throw t;
    }
  }

  @Override
  public synchronized RestartableGenericMap<Object, Object> getOrCreateStateRepositoryMap(String tierManagerId,
                                                                                          String tierId,
                                                                                          String mapId) {
    final Set<FrsContainerIdentifier> containers = containerMap.get(tierManagerId);
    if (containers == null) {
      // this cannot happen
      throw new AssertionError("Internal Error: Container(s) not found for clustered tier. Currently no support for " +
                               "multiple containers");
    }
    RestartableServerStoreConfiguration storeConfig = null;
    final String composedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    for (FrsContainerIdentifier container : containers) {
      storeConfig = metadataProvider.getChildConfigEntry(container, composedTierId);
      if (storeConfig != null) {
        break;
      }
    }
    if (storeConfig == null || !containers.contains(log2container(storeConfig.getDataLogIdentifier()))) {
      // either of this should not happen
      throw new AssertionError("Unable to find metadata for tier " + composedTierId);
    }
    final RestartableStateRepositoryConfig repoConfig = new RestartableStateRepositoryConfig(
        IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId), storeConfig.getDataLogIdentifier());
    final TierDataLogManager logManager = dataLogManagerMap.get(storeConfig.getDataLogIdentifier());
    if (logManager == null) {
      // this cannot happen..
      throw new AssertionError("Unable to find data log for tier " + composedTierId);
    }
    metadataProvider.addNthLevelOtherChildConfiguration(
        IdentifierComposer.repoIdToComposedRepoId(tierManagerId, tierId, mapId), repoConfig,
        RestartableStateRepositoryConfig.class);
    return logManager.addStateRepository(tierManagerId, tierId, mapId);
  }

  @Override
  public synchronized void destroyServerSide(String tierManagerId) {
    Map<String, RestartableServerSideConfiguration> roots = metadataProvider.getAllRootConfiguration();
    final RestartableServerSideConfiguration mgrToDelete = roots.get(tierManagerId);
    if (mgrToDelete != null) {
      final Set<FrsContainerIdentifier> containers = containerMap.remove(tierManagerId);
      if (containers != null) {
        final Set<FrsDataLogIdentifier> logsToRemove = new HashSet<>();
        dataLogManagerMap.forEach((log, logMgr) -> {
          if (containers.contains(log2container(log))) {
            final boolean lastOne = logMgr.destroyTierManager(tierManagerId);
            if (lastOne) {
              logMgr.close();
              logsToRemove.add(log);
            }
          }
        });
        logsToRemove.forEach(dataLogManagerMap::remove);
      }
      mgrToDelete.getContainers().forEach((container) -> metadataProvider.removeRootConfigurationFrom(container,
          tierManagerId));
    }
  }

  @Override
  public synchronized void destroyServerStore(String tierManagerId, String tierId) {
    String composedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    tierToDataLogMapping.remove(composedTierId);
    Set<FrsContainerIdentifier> containers = containerMap.get(tierManagerId);
    if (containers == null) {
      // may not have been configured yet..just forcefully remove from frs
      Map<String, RestartableServerSideConfiguration> roots = metadataProvider.getAllRootConfiguration();
      final RestartableServerSideConfiguration mgr = roots.get(tierManagerId);
      if (mgr != null) {
        containers = mgr.getContainers();
      }
    }
    if (containers != null) {
      // remove from metadata
      final Set<FrsContainerIdentifier> containerToCheck = containers;
      dataLogManagerMap.forEach((log, logMgr) -> {
        if (containerToCheck.contains(log2container(log))) {
          logMgr.destroyTier(tierManagerId, tierId);
        }
      });
      containers.forEach((container) -> metadataProvider.removeChildConfigurationFrom(container,
          IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId)));
    }
  }

  @Override
  public synchronized boolean exists(String tierManagerId, String tierId) {
    Set<FrsContainerIdentifier> containers = containerMap.get(tierManagerId);
    if (containers == null) {
      // may not have been configured yet..just check in metadata
      Map<String, RestartableServerSideConfiguration> roots = metadataProvider.getAllRootConfiguration();
      final RestartableServerSideConfiguration mgr = roots.get(tierManagerId);
      if (mgr != null) {
        containers = mgr.getContainers();
      }
    }
    boolean exists = false;
    if (containers != null) {
      for (FrsContainerIdentifier container : containers) {
        final RestartableServerStoreConfiguration config = metadataProvider.getChildConfigEntry(container,
            IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId));
        if (config != null) {
          exists = true;
          break;
        }
      }
    }
    return exists;
  }

  @Override
  public synchronized void shutdown() {
    this.dataLogManagerMap.values().forEach(TierDataLogManager::close);
    this.metadataProvider.shutdownAllContainers();
  }

  private FrsDataLogIdentifier getDataLogIdentifierForTier(String tierManagerId, String composedTierId) {
    return tierToDataLogMapping.computeIfAbsent(composedTierId,
        (k) -> {
          RestartableServerStoreConfiguration restartableServerStoreConfiguration =
              metadataProvider.getFirstLevelChildConfigurationByRoot(tierManagerId).get(k);
          return restartableServerStoreConfiguration.getDataLogIdentifier();
        });
  }

  private FrsContainerIdentifier log2container(FrsDataLogIdentifier log) {
    return new FrsContainerIdentifier(log.getRootPathName(), log.getContainerName());
  }

  private void createResourcePoolManagers(Map<String, RestartableServerSideConfiguration> allRootConfiguration) {
    allRootConfiguration.forEach((mgrId, config) -> {
      try {
        this.serviceRegistry.getService(new ResourcePoolManagerConfiguration(mgrId, config.getServerSideConfiguration()));
      } catch (ServiceException e) {
        throw new AssertionError("Failed to obtain the singleton ResourcePoolManager instance for " + mgrId, e);
      }
    });
  }

  private RestartStoreLocator<ByteBuffer, ByteBuffer, ByteBuffer> metaDataStoreLocator(Entry<String,
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>> storeLocator) {
    return new RestartStoreLocator<ByteBuffer, ByteBuffer, ByteBuffer>() {
      @Override
      public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getStore() {
        return storeLocator.getValue();
      }

      @Override
      public Path getRelativeToRoot() {
        Path metadataLocation = Paths.get(storeLocator.getKey());
        for (String rootId : rootPathProvider.dataDirectories.getDataDirectoryNames()) {
          Path rootPath = rootPathProvider.dataDirectories.getDataDirectory(rootId);
          if (metadataLocation.startsWith(rootPath)) {
            return Paths.get(rootId).resolve(rootPath.relativize(metadataLocation));
          }
        }
        // this should never happen, but instead of failing, play it safe by returning the
        // full path
        return metadataLocation;
      }
    };
  }

  public static class FRSRootPathProvider implements RootPathProvider {
    private final DataDirectories dataDirectories;
    private final Set<File> containerDirs = new HashSet<>();

    public FRSRootPathProvider(DataDirectories dataDirectories) {
      this.dataDirectories = dataDirectories;
      // Add all first-level subdirectories for each data root
      for (String rootID : dataDirectories.getDataDirectoryNames()) {
        Path rootPath = dataDirectories.getDataDirectory(rootID).resolve(EHCACHE_FRS_STORAGE_SPACE);
        // create the directory if it doesn't exists
        try {
          Files.createDirectories(rootPath);
        } catch (IOException e) {
          throw new RuntimeException("Unable to create the directory " + rootPath, e);
        }

        File[] subDirectories = rootPath.toFile().listFiles();

        if (subDirectories != null) {
          for (File subDirectory : subDirectories) {
            if (subDirectory.isDirectory()) {
              containerDirs.add(subDirectory);
            }
          }
        }
      }
    }

    @Override
    public File getRootPath(String rootName) {
      return dataDirectories.getDataDirectory(rootName).resolve(EHCACHE_FRS_STORAGE_SPACE).toFile();
    }

    @Override
    public File getConfiguredRoot(String rootName) {
      return dataDirectories.getDataDirectory(rootName).toFile();
    }

    @Override
    public Set<File> getAllExistingContainers() {
      return Collections.unmodifiableSet(containerDirs);
    }
  }
}
