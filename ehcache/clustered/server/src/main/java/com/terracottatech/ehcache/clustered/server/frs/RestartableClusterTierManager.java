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
package com.terracottatech.ehcache.clustered.server.frs;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.state.EhcacheStateContext;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;

import com.tc.classloader.CommonComponent;
import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSService;
import com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSServiceConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.IdentifierComposer;
import com.terracottatech.ehcache.clustered.server.services.frs.PerTierDataObjects;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerSideConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerStoreConfiguration;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import com.terracottatech.ehcache.common.frs.metadata.FrsContainerIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProvider;
import com.terracottatech.persistence.RestartablePlatformPersistence;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Simplifies interaction with restart store service {@link EhcacheFRSService}.
 * There will be one {@link RestartableClusterTierManager} for each clustered tier manager.
 *
 * @author vmad
 */
@CommonComponent
public class RestartableClusterTierManager implements ClusterTierRestartManager {
  private final MetadataProvider<String, RestartableServerSideConfiguration, RestartableServerStoreConfiguration> metadataProvider;
  private final EhcacheFRSService ehcacheFRSService;
  private final String tierManagerId;
  // there can only be atmost one mapping between a tierManagerId and a tierManagerEntityId, if they are different
  private final String tierManagerEntityId;
  private final ConfigurationException configurationException;
  private final Set<String> restartableTiers;

  private volatile RestartableServerSideConfiguration restartableServerSideConfiguration;

  public RestartableClusterTierManager(DataDirectories dataRootConfig, ServiceRegistry services, String frsIdentifier, String entityIdentifier) {
    if (dataRootConfig == null) {
      configurationException = new ConfigurationException("Restartable not supported without data directories configured");
      ehcacheFRSService = null;
      metadataProvider = null;
    } else {
      RestartablePlatformPersistence restartablePlatformPersistence;
      try {
        restartablePlatformPersistence = services.getService(new BasicServiceConfiguration<>(RestartablePlatformPersistence.class));
      } catch (ServiceException e) {
        throw new AssertionError("Failed to obtain the singleton RestartablePlatformPersistence instance", e);
      }
      if(restartablePlatformPersistence == null) {
        configurationException = new ConfigurationException("RestartableClusterTierManager is not supported on non-restartable server");
      } else {
        configurationException = null;
      }

      try {
        this.ehcacheFRSService = services.getService(new EhcacheFRSServiceConfiguration(services));
      } catch (ServiceException e) {
        throw new AssertionError("Failed to obtain the singleton EhcacheFRSService instance for "
            + dataRootConfig.getDataDirectoryNames(), e);
      }
      if (this.ehcacheFRSService == null) {
        throw new AssertionError("Server failed to retrieve EhcacheFRSService.");
      }
      this.metadataProvider = ehcacheFRSService.getMetadataProvider();
    }
    this.tierManagerId = frsIdentifier;
    this.tierManagerEntityId = entityIdentifier;
    this.restartableTiers = new HashSet<>();
  }

  @Override
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public void configure(ServerSideConfiguration configuration) throws ConfigurationException {
    if (configurationException != null) {
      throw configurationException;
    }
    EnterpriseServerSideConfiguration enterpriseConfiguration = (EnterpriseServerSideConfiguration) configuration;
    ehcacheFRSService.bootstrapMetaData();
    this.restartableServerSideConfiguration = new RestartableServerSideConfiguration(enterpriseConfiguration);
    ehcacheFRSService.configure(tierManagerId, tierManagerEntityId,  restartableServerSideConfiguration);
  }

  /**
   * Trigger a restart of the entire restart store system. If the restart is already done or triggered, this will return
   * immediately.
   * <p>
   *   Note: Current assumption is that each entity is loaded in sequence. If entities are loaded in parallel,
   *   this implementation will require changes.
   * </p>
   *
   * @param mapper the key segment mapper.
   */
  @Override
  public void restart(final KeySegmentMapper mapper) {
    ehcacheFRSService.bootstrapData(mapper);
  }


  /**
   * Adds the clustered tier manager given by the {@link ServerSideConfiguration}. If the configuration for the given
   * clustered tier manager exists in the persistent store, this method will attempt to attach all the associated
   * restarted stores and its associated data.
   * <p>
   *   The composed ids are transformed properly by {@link EhcacheFRSService} before returning the recovered data
   *   tiers.
   *
   *  @param serverSideConfiguration {@link ServerSideConfiguration} to be added
   *
   */
  @Override
  public Map<String, PerTierDataObjects> createOrRestartClusterTierManager(ServerSideConfiguration serverSideConfiguration) {
    if (serverSideConfiguration instanceof EnterpriseServerSideConfiguration) {
      this.restartableServerSideConfiguration = new RestartableServerSideConfiguration(
          (EnterpriseServerSideConfiguration)serverSideConfiguration);
      return ehcacheFRSService.recoverOrAdd(tierManagerId, restartableServerSideConfiguration);
    } else {
      throw new AssertionError("Unexpected type of server side configuration: " + serverSideConfiguration);
    }
  }

  /**
   * Adds a given tier, represented by {@code tierId}, with provided {@link ServerStoreConfiguration} to metadata and data.
   * This method has no effect if the clustered tier is non-restartable.
   *
   * @param tierId                  unique id for given {@link ServerStoreConfiguration}
   * @param mapper                  the segment mapper used by underlying tier
   * @param serverStoreConfiguration {@link ServerStoreConfiguration} of the store to be added.
   *
   * @return a server store impl. if this store is already existing, it will return that instead of throwing an error.
   */
  @Override
  public PerTierDataObjects createServerStore(String tierId, KeySegmentMapper mapper,
                                              ServerStoreConfiguration serverStoreConfiguration) throws ConfigurationException {
    final PoolAllocation allocation = serverStoreConfiguration.getPoolAllocation();
    final boolean isRestartable = allocation instanceof EnterprisePoolAllocation.DedicatedRestartable ||
                                  allocation instanceof EnterprisePoolAllocation.SharedRestartable;
    if (isRestartable) { // restartable server store
      restartableTiers.add(tierId);
      RestartableServerStoreConfiguration restartableServerStoreConfiguration =
          new RestartableServerStoreConfiguration(tierManagerId, serverStoreConfiguration,
              this.restartableServerSideConfiguration.getServerSideConfiguration().getRestartConfiguration());
      return ehcacheFRSService.getOrCreateServerStore(tierManagerId, tierId, mapper, restartableServerStoreConfiguration,
          restartableServerSideConfiguration.getServerSideConfiguration().getRestartConfiguration().isHybrid());
    } else { //non-restartable config
      ServerStoreConfiguration existingServerStoreConfiguration = getServerStoreConfiguration(tierId);
      if (existingServerStoreConfiguration != null) {
        throw new IllegalArgumentException(String.format(
            "A restartable clustered tier configuration \'{%1$s}\' already exists for clustered tier manager \'{%2$s}\'",
            tierId, tierManagerId));
      }
    }
    return null;
  }

  /**
   * Creates a restartable concurrent map and returns it.
   *
   * @param tierId Clustered tier identifier
   * @param mapId Map identifier
   *
   * @return a restartable map
   */
  @Override
  public RestartableGenericMap<Object, Object> createStateRepositoryMap(String tierId, String mapId) {
    return ehcacheFRSService.getOrCreateStateRepositoryMap(tierManagerId, tierId, mapId);
  }

  /**
   * Removes existing metadata and all associated data for {@code this} clustered tier.
   */
  @Override
  public void destroyClusterTierManager() {
    if (ehcacheFRSService != null) {
      ehcacheFRSService.destroyServerSide(tierManagerId);
      restartableTiers.clear();
    }
  }

  /**
   * Destroy the server store specified by the given tierId.
   *
   * @param tierId the ID of the clustered tier
   */
  @Override
  public void destroyServerStore(String tierId) {
    restartableTiers.remove(tierId);
    ehcacheFRSService.destroyServerStore(tierManagerId, tierId);
  }

  /**
   * Is the given tier restartable?
   *
   * @param tierId id of the clustered tier
   * @return true, if restartable, false otherwise
   */
  @Override
  public boolean isRestartable(String tierId) {
    return restartableTiers.contains(tierId);
  }

  /**
   * Validates the incoming config against current config for restartability
   *
   * @param incomingConfig incoming configuration from client
   */
  @Override
  public void validate(ServerSideConfiguration incomingConfig) throws InvalidServerSideConfigurationException {
    if (!(incomingConfig instanceof EnterpriseServerSideConfiguration)) {
      throw new InvalidServerSideConfigurationException("Fast Restart configuration missing from client request. " +
                                                        "Server is configured for fast restart. ");
    } else {
      try {
        this.restartableServerSideConfiguration.getServerSideConfiguration()
            .getRestartConfiguration()
            .checkCompatibility(((EnterpriseServerSideConfiguration)incomingConfig).getRestartConfiguration(), true);
      } catch (IllegalArgumentException e) {
        throw new InvalidServerSideConfigurationException(e.getMessage());
      }
    }
  }

  private ServerStoreConfiguration getServerStoreConfiguration(String tierId) {
    String composedTierId = IdentifierComposer.tierIdToComposedTierId(tierManagerId, tierId);
    for (FrsContainerIdentifier frsContainerIdentifier : this.restartableServerSideConfiguration.getContainers()) {
      RestartableServerStoreConfiguration childConfigEntry = this.metadataProvider
          .getChildConfigEntry(frsContainerIdentifier, composedTierId);
      if (childConfigEntry != null) {
        return childConfigEntry.getServerStoreConfiguration();
      }
    }
    return null;
  }

  @Override
  public void prepareForDestroy() {
    restartableServerSideConfiguration.setDestroyInProgress(true);
    metadataProvider.addRootConfiguration(tierManagerId, restartableServerSideConfiguration);
  }

  @Override
  public boolean isDestroyInProgress() {
    return restartableServerSideConfiguration.isDestroyInProgress();
  }

  @Override
  public void beginGlobalTransaction(String tierId) {
    if (isRestartable(tierId)) {
      ehcacheFRSService.beginGlobalTransaction(tierManagerId, tierId);
    }
  }

  @Override
  public void endGlobalTransaction(String tierId) {
    if (isRestartable(tierId)) {
      ehcacheFRSService.endGlobalTransaction(tierManagerId, tierId);
    }
  }

  @Override
  public EhcacheStateContext getTransactionContext(String tierId) {
    if (isRestartable(tierId)) {
      return new EnterpriseTransactionContext(ehcacheFRSService, tierManagerId, tierId);
    } else {
      return () -> {};
    }
  }
}