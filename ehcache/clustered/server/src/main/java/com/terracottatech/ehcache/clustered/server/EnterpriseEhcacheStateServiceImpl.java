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

package com.terracottatech.ehcache.clustered.server;

import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.server.frs.ClusterTierRestartManager;
import com.terracottatech.ehcache.clustered.server.frs.RestartableClusterTierManager;
import com.terracottatech.ehcache.clustered.server.frs.TransientClusterTierManager;
import com.terracottatech.ehcache.clustered.server.repo.EnterpriseStateRepositoryManager;
import com.terracottatech.ehcache.clustered.server.services.frs.PerTierDataObjects;
import com.terracottatech.ehcache.clustered.server.services.pool.ResourcePoolManagerConfiguration;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateService;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateServiceProvider;
import com.terracottatech.ehcache.clustered.server.state.RestartableInvalidationTracker;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.server.EhcacheStateServiceImpl;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.ServerStoreImpl;
import org.ehcache.clustered.server.repo.StateRepositoryManager;
import org.ehcache.clustered.server.state.EhcacheStateContext;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.ehcache.clustered.server.state.InvalidationTrackerImpl;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.TreeNode;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.ValueStatistic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.terracotta.statistics.StatisticsManager.tags;
import static org.terracotta.statistics.ValueStatistics.gauge;

/**
 *
 * Enterprise implementation of the {@link EhcacheStateService}.
 * <p>
 * NOTE: Currently the entire open source {@link EhcacheStateServiceImpl} is copied and modified directly here. This is
 * only a temporary hack to avoid too much dependency with ehcache3 open source code UNTIL the enterprise version is
 * completely implemented. Once the enterprise version is completely implemented, it will become very clear on how to
 * refactor the open source ehcache 3 code for proper re-use.
 *
 */
public class EnterpriseEhcacheStateServiceImpl implements EnterpriseEhcacheStateService {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnterpriseEhcacheStateServiceImpl.class);

  private static final String STATISTICS_STORE_TAG = "Store";
  private static final String PROPERTY_STORE_KEY = "storeName";

  private static final Map<String, Function<ServerStoreImpl, ValueStatistic<Number>>> STAT_STORE_METHOD_REFERENCES = new HashMap<>();

  static {
    STAT_STORE_METHOD_REFERENCES.put("allocatedMemory", (serverStore) -> gauge(serverStore::getAllocatedMemory));
    STAT_STORE_METHOD_REFERENCES.put("dataAllocatedMemory", (serverStore) -> gauge(serverStore::getDataAllocatedMemory));
    STAT_STORE_METHOD_REFERENCES.put("occupiedMemory", (serverStore) -> gauge(serverStore::getOccupiedMemory));
    STAT_STORE_METHOD_REFERENCES.put("dataOccupiedMemory", (serverStore) -> gauge(serverStore::getDataOccupiedMemory));
    STAT_STORE_METHOD_REFERENCES.put("entries", (serverStore) -> gauge(serverStore::getSize));
    STAT_STORE_METHOD_REFERENCES.put("usedSlotCount", (serverStore) -> gauge(serverStore::getUsedSlotCount));
    STAT_STORE_METHOD_REFERENCES.put("dataVitalMemory", (serverStore) -> gauge(serverStore::getDataVitalMemory));
    STAT_STORE_METHOD_REFERENCES.put("vitalMemory", (serverStore) -> gauge(serverStore::getVitalMemory));
    STAT_STORE_METHOD_REFERENCES.put("reprobeLength", (serverStore) -> gauge(serverStore::getReprobeLength));
    STAT_STORE_METHOD_REFERENCES.put("removedSlotCount", (serverStore) -> gauge(serverStore::getRemovedSlotCount));
    STAT_STORE_METHOD_REFERENCES.put("dataSize", (serverStore) -> gauge(serverStore::getDataSize));
    STAT_STORE_METHOD_REFERENCES.put("tableCapacity", (serverStore) -> gauge(serverStore::getTableCapacity));
  }

  private volatile boolean configured = false;

  private volatile ResourcePoolManager resourcePoolManager;

  /**
   * The clustered stores representing the server-side of a {@code ClusterStore}.
   * The index is the cache alias/identifier.
   */
  private final Map<String, ServerStoreImpl> stores = new ConcurrentHashMap<>();

  private final String frsIdentifier;
  private final ConcurrentMap<String, InvalidationTracker> invalidationTrackers = new ConcurrentHashMap<>();
  private final ServerSideConfiguration configuration;
  private final StateRepositoryManager stateRepositoryManager;
  private final KeySegmentMapper mapper;
  private final EnterpriseEhcacheStateServiceProvider.DestroyCallback destroyCallback;

  private final RestartConfiguration restartConfiguration;
  private final DataDirectories dataDirectories;
  private final ClusterTierRestartManager restartManager;
  private final ServiceRegistry services;

  public EnterpriseEhcacheStateServiceImpl(DataDirectories dataDirectories,
                                           ClusterTierManagerConfiguration config, ServiceRegistry services,
                                           final KeySegmentMapper mapper,
                                           EnterpriseEhcacheStateServiceProvider.DestroyCallback destroyCallback) {
    this.mapper = mapper;
    this.configuration = config.getConfiguration();
    this.dataDirectories = dataDirectories;
    this.services = services;

    String identifier = config.getIdentifier();

    if (this.configuration instanceof EnterpriseServerSideConfiguration) {
      this.restartConfiguration = ((EnterpriseServerSideConfiguration) this.configuration).getRestartConfiguration();
      String injectedIdentifier = restartConfiguration.getFrsIdentifier();
      this.frsIdentifier = injectedIdentifier != null ? injectedIdentifier : identifier;
      this.restartManager = new RestartableClusterTierManager(dataDirectories, services, this.frsIdentifier, identifier);
    } else {
      this.frsIdentifier = identifier;
      this.restartConfiguration = null;
      this.restartManager = new TransientClusterTierManager();
    }
    this.stateRepositoryManager = new EnterpriseStateRepositoryManager(restartManager);
    this.destroyCallback = destroyCallback;
  }

  public ServerSideServerStore getStore(String name) {
    return stores.get(name);
  }

  @Override
  public ServerSideServerStore loadStore(String name, ServerStoreConfiguration serverStoreConfiguration) {
    ServerSideServerStore store = getStore(name);
    if (store == null) {
      try {
        store = createStore(name, serverStoreConfiguration, true);
      } catch (ConfigurationException e) {
        LOGGER.warn("Unable to recover clustered tier ", name);
      }
    } else {
      if (!restartManager.isRestartable(name)) {
        invalidationTrackers.remove(name);
      }
    }
    return store;
  }

  public Set<String> getStores() {
    return Collections.unmodifiableSet(stores.keySet());
  }

  @Override
  public void prepareForDestroy() {
    restartManager.prepareForDestroy();
    stores.values().forEach(ServerStoreImpl::clear);
  }

  @Override
  public RestartConfiguration getRestartConfiguration() {
    return this.restartConfiguration;
  }

  public DataDirectories getDataRoots() {
    return this.dataDirectories;
  }

  @Override
  public void beginGlobalTransaction(String storeAlias) {
    restartManager.beginGlobalTransaction(storeAlias);
  }

  @Override
  public void endGlobalTransaction(String storeAlias) {
    restartManager.endGlobalTransaction(storeAlias);
  }

  public String getDefaultServerResource() {
    //Management code is calling this method before configure method is called...
    if (this.resourcePoolManager == null) {
      return null;
    }
    return this.resourcePoolManager.getDefaultServerResource();
  }

  @Override
  public Map<String, ServerSideConfiguration.Pool> getSharedResourcePools() {
    return this.resourcePoolManager.getSharedResourcePools();
  }

  @Override
  public ResourcePageSource getSharedResourcePageSource(String name) {
    return this.resourcePoolManager.getSharedResourcePageSource(name);
  }

  @Override
  public ServerSideConfiguration.Pool getDedicatedResourcePool(String name) {
    return this.resourcePoolManager.getDedicatedResourcePool(name);
  }

  @Override
  public ResourcePageSource getDedicatedResourcePageSource(String name) {
    return this.resourcePoolManager.getDedicatedResourcePageSource(name);
  }

  public void validate(ServerSideConfiguration configuration) throws ClusterException {
    if (restartManager.isDestroyInProgress()) {
      throw new DestroyInProgressException("Cluster Tier Manager marked in progress for destroy - clean up by destroying or re-creating");
    }
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }

    if (configuration != null) {
      checkConfigurationCompatibility(configuration);
    }
  }

  /**
   * Checks whether the {@link ServerSideConfiguration} sent from the client is equal with the ServerSideConfiguration
   * that is already configured on the server.
   * @param incomingConfig the ServerSideConfiguration to be validated.  This is sent from a client
   * @throws IllegalArgumentException if configurations do not match
   */
  private void checkConfigurationCompatibility(ServerSideConfiguration incomingConfig) throws InvalidServerSideConfigurationException {
    this.restartManager.validate(incomingConfig);
    this.resourcePoolManager.checkConfigurationCompatibility(incomingConfig);
  }

  /**
   * Configure the clustered tier manager.
   *
   * @throws ConfigurationException if a store manager is already configured
   */
  @Override
  public void configure() throws ConfigurationException {
    if (!isConfigured()) {
      LOGGER.info("Configuring server-side clustered tier manager");
      try {
        this.resourcePoolManager = this.services.getService(new ResourcePoolManagerConfiguration(frsIdentifier, configuration));
      } catch (RuntimeException re) {
        if (re.getCause() instanceof ConfigurationException) {
          throw (ConfigurationException) re.getCause();
        } else {
          throw re;
        }
      } catch (ServiceException e) {
        throw new ConfigurationException("Failed to obtain the singleton ResourcePoolManager instance for "
            + frsIdentifier, e);
      }
      this.restartManager.configure(configuration);
      configured = true;
    }
  }

  private void registerStoreStatistics(ServerStoreImpl store, String storeName) {
    STAT_STORE_METHOD_REFERENCES.forEach((name, statFactory) -> registerStatistic(store, storeName, name, STATISTICS_STORE_TAG, PROPERTY_STORE_KEY, statFactory.apply(store)));
  }

  private void unRegisterStoreStatistics(ServerStoreImpl store) {
    TreeNode node = StatisticsManager.nodeFor(store);
    if(node != null) {
      node.clean();
    }
  }

  private void registerStatistic(Object context, String name, String observerName, String tag, String propertyKey, ValueStatistic<Number> valueStatistic) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("discriminator", tag);
    properties.put(propertyKey, name);

    StatisticsManager.createPassThroughStatistic(context, observerName, tags(tag, "tier"), properties, valueStatistic);
  }

  public void destroy() {
    for (Map.Entry<String, ServerStoreImpl> storeEntry: stores.entrySet()) {
      unRegisterStoreStatistics(storeEntry.getValue());
      storeEntry.getValue().close();
    }
    stores.clear();
    this.restartManager.destroyClusterTierManager();

    if (this.resourcePoolManager != null) {
      this.resourcePoolManager.destroy();
      this.resourcePoolManager = null;
    }

    invalidationTrackers.clear();

    this.configured = false;
    destroyCallback.destroy(this);
  }

  @Override
  public ServerSideServerStore createStore(String name, ServerStoreConfiguration serverStoreConfiguration, boolean forActive) throws ConfigurationException {
    if (this.stores.containsKey(name)) {
      throw new ConfigurationException("Clustered tier '" + name + "' already exists");
    }

    // ask recovery manager to record creation of this store first. if this is a non restartable store
    // restart manager will internally ignore this step.
    PerTierDataObjects tierData = this.restartManager.createServerStore(name, mapper, serverStoreConfiguration);

    final ServerStoreImpl serverStore;
    if (tierData == null) {
      ResourcePageSource resourcePageSource = this.resourcePoolManager.getPageSource(name,
          serverStoreConfiguration.getPoolAllocation());
      serverStore = new ServerStoreImpl(serverStoreConfiguration, resourcePageSource, mapper);
      if (!forActive) {
        if (serverStoreConfiguration.getConsistency() == Consistency.EVENTUAL) {
          invalidationTrackers.put(name, new InvalidationTrackerImpl());
        }
      }
    } else {
      serverStore = new ServerStoreImpl(tierData.getServerStoreConfiguration(), tierData.getPageSource(),
          mapper, tierData.getRecoveredMaps());
      if (serverStoreConfiguration.getConsistency() == Consistency.EVENTUAL) {
        invalidationTrackers.put(name, new RestartableInvalidationTracker(tierData.getInvalidationTrackerMap()));
      }
    }
    stores.put(name, serverStore);
    registerStoreStatistics(serverStore, name);
    return serverStore;
  }

  /**
   * Re-creates all the persistent stores on a restart.
   *
   * @param recoveredStores stores that are restored from the fast restart store
   */
  private void reCreateStores(Map<String, PerTierDataObjects> recoveredStores) {
    recoveredStores.forEach((name, tier) -> {
      final ServerStoreImpl store = new ServerStoreImpl(tier.getServerStoreConfiguration(), tier.getPageSource(),
          mapper, tier.getRecoveredMaps());
      stores.put(name, store);

      if (tier.getServerStoreConfiguration().getConsistency() == Consistency.EVENTUAL) {
        invalidationTrackers.put(name, new RestartableInvalidationTracker(tier.getInvalidationTrackerMap()));
      }
      ((EnterpriseStateRepositoryManager) stateRepositoryManager).loadMaps(name, tier.getStateRepositories());
      registerStoreStatistics(store, name);
    });
  }

  public void destroyServerStore(String name) throws ClusterException {

    final ServerStoreImpl store = stores.remove(name);
    if (store == null) {
      throw new InvalidStoreException("Clustered tier '" + name + "' does not exist");
    } else {
      unRegisterStoreStatistics(store);
      this.resourcePoolManager.releaseDedicatedPool(name, store.getPageSource());
      store.close();

      // destroy persistent state last
      stateRepositoryManager.destroyStateRepository(name);
      InvalidationTracker invalidationTracker = this.invalidationTrackers.remove(name);
      if (invalidationTracker != null) {
        invalidationTracker.clear();
      }
      this.restartManager.destroyServerStore(name);
    }
  }

  @Override
  public InvalidationTracker getInvalidationTracker(String name) {
    return invalidationTrackers.get(name);
  }

  public boolean isConfigured() {
    return configured;
  }

  @Override
  public StateRepositoryManager getStateRepositoryManager() {
    return this.stateRepositoryManager;
  }

  /**
   * Loads an existing restartable entity.
   */
  @Override
  public void loadExisting(ServerSideConfiguration serverSideConfiguration) {
    this.restartManager.restart(mapper);
    Map<String, PerTierDataObjects> recoveredStores = this.restartManager
        .createOrRestartClusterTierManager(serverSideConfiguration);
    reCreateStores(recoveredStores);
  }

  @Override
  public EhcacheStateContext beginProcessing(EhcacheOperationMessage message, String name) {
    if (EhcacheMessageType.isTrackedOperationMessage(message.getMessageType())) {
      return this.restartManager.getTransactionContext(name);
    } else {
      return () -> {};
    }
  }
}
