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

package com.terracottatech.ehcache.clustered.server.state;

import com.tc.classloader.BuiltinService;
import com.tc.classloader.OverrideService;
import com.terracottatech.config.data_roots.DataDirectoriesConfig;
import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.ehcache.clustered.server.EnterpriseEhcacheStateServiceImpl;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.ehcache.clustered.server.state.config.EhcacheStoreStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.entity.StateDumpCollector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides the enterprise version of {@link EhcacheStateService}.
 *
 * @author RKAV
 */
@BuiltinService
@OverrideService("org.ehcache.clustered.server.state.EhcacheStateServiceProvider")
public class EnterpriseEhcacheStateServiceProvider implements ServiceProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnterpriseEhcacheStateServiceProvider.class);

  private ConcurrentMap<String, EhcacheStateService> serviceMap = new ConcurrentHashMap<>();
  private DataDirectories dataDirectories;

  @Override
  public void addStateTo(StateDumpCollector dump) {
    for (Map.Entry<String, EhcacheStateService> entry : new HashMap<>(serviceMap).entrySet()) {
      StateDumpCollector clusterTierManagerStateDump = dump.subStateDumpCollector(entry.getKey());
      EhcacheStateService ehcacheStateService = entry.getValue();
      EnterpriseEhcacheStateServiceDump.dump(ehcacheStateService, clusterTierManagerStateDump);
    }
  }

  @Override
  public boolean initialize(ServiceProviderConfiguration configuration, PlatformConfiguration platformConfiguration) {
    // construct dataDirectories object
    Collection<DataDirectoriesConfig> dataDirectoriesConfigs = platformConfiguration.getExtendedConfiguration(DataDirectoriesConfig.class);
    if(dataDirectoriesConfigs.size() > 1) {
      throw new UnsupportedOperationException("There are " + dataDirectoriesConfigs.size() + "DataDirectoriesConfig , this is not supported. " +
                                              "There must be only one!");
    }

    Iterator<DataDirectoriesConfig> dataRootConfigIterator = dataDirectoriesConfigs.iterator();
    if(dataRootConfigIterator.hasNext()) {
      this.dataDirectories = dataRootConfigIterator.next().getDataDirectoriesForServer(platformConfiguration);
      if(dataDirectories.getDataDirectoryNames().isEmpty()) {
        throw new UnsupportedOperationException("There are no data-root-configs defined, this is not supported. There must be at least one!");
      }
    } else {
      LOGGER.warn("No data-root-config defined - this will prevent provider from offering any EnterpriseEhcacheStateService.");
    }

    // TODO: validate offheap resources with config in the restartable metadata
    return true;
  }

  @Override
  public <T> T getService(long consumerID, ServiceConfiguration<T> configuration) {
    if (configuration != null && configuration.getServiceType().equals(EhcacheStateService.class)) {
      EhcacheStateService result;
      if (configuration instanceof EhcacheStateServiceConfig) {
        EhcacheStateServiceConfig stateServiceConfig = (EhcacheStateServiceConfig) configuration;
        EhcacheStateService storeManagerService = new EnterpriseEhcacheStateServiceImpl(dataDirectories,
          stateServiceConfig.getConfig(), stateServiceConfig.getServiceRegistry(), stateServiceConfig.getMapper(),
          service -> serviceMap.remove(stateServiceConfig.getConfig().getIdentifier(), service));

        result = serviceMap.putIfAbsent(stateServiceConfig.getConfig().getIdentifier(), storeManagerService);
        if (result == null) {
          result = storeManagerService;
        }
      } else if (configuration instanceof EhcacheStoreStateServiceConfig) {
        EhcacheStoreStateServiceConfig storeStateServiceConfig = (EhcacheStoreStateServiceConfig) configuration;
        result = serviceMap.get(storeStateServiceConfig.getManagerIdentifier());
      } else {
        throw new IllegalArgumentException("Unexpected configuration type: " + configuration);
      }
      return configuration.getServiceType().cast(result);
    }
    throw new IllegalArgumentException("Unexpected configuration type.");
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    List<Class<?>> classes = new ArrayList<>();
    classes.add(EhcacheStateService.class);
    return classes;
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
    serviceMap.clear();
  }

  public interface DestroyCallback {
    void destroy(EhcacheStateService service);
  }
}
