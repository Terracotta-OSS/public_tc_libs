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
package com.terracottatech.store.server.state;

import com.tc.classloader.BuiltinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.entity.StateDumpCollector;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ServiceProvider for {@link DatasetEntityStateService}
 */
@BuiltinService
public class DatasetEntityStateServiceProvider implements ServiceProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetEntityStateServiceProvider.class);

  private final ConcurrentMap<String, DatasetEntityStateService<?>> serviceMap = new ConcurrentHashMap<>();

  @Override
  public void addStateTo(StateDumpCollector dump) {
    for (Map.Entry<String, DatasetEntityStateService<?>> stateServiceEntry : new HashMap<>(serviceMap).entrySet()) {
      DatasetEntityStateService<?> service = stateServiceEntry.getValue();
      StateDumpCollector serviceDump = dump.subStateDumpCollector(stateServiceEntry.getKey());
      service.addStateTo(serviceDump);
    }
  }

  @Override
  public boolean initialize(ServiceProviderConfiguration configuration, PlatformConfiguration platformConfiguration) {
    LOGGER.info("Initializing DatasetEntityStateServiceProvider.");
    return true;
  }

  @Override
  public <T> T getService(long consumerID, ServiceConfiguration<T> configuration) {
    if (configuration != null && configuration.getServiceType().equals(DatasetEntityStateService.class)) {
      try {
        DatasetEntityStateServiceConfig<?> config = (DatasetEntityStateServiceConfig) configuration;
        String datasetName = config.getEntityConfiguration().getDatasetName();
        LOGGER.info("Looking up DatasetEntityStateService for dataset : {} ", datasetName);
        DatasetEntityStateService<?> datasetEntityStateService = DatasetEntityStateService.create(
                config.getEntityConfiguration(), config.getServiceRegistry(), () -> serviceMap.remove(datasetName));
        DatasetEntityStateService<?> entityStateService = serviceMap.putIfAbsent(datasetName, datasetEntityStateService);
        if (entityStateService == null) {
          entityStateService = datasetEntityStateService;
        }
        @SuppressWarnings("unchecked")
        T service = (T) entityStateService;
        return service;
      } catch (ServiceException e) {
        throw new AssertionError(e);
      }
    }
    throw new IllegalArgumentException("Unexpected configuration type");
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    return Collections.singletonList(DatasetEntityStateService.class);
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
    serviceMap.clear();
  }

}
