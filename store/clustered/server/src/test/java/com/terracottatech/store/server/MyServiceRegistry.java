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
package com.terracottatech.store.server;

import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;

import com.terracottatech.store.server.state.DatasetEntityStateService;
import com.terracottatech.store.server.state.DatasetEntityStateServiceConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class MyServiceRegistry implements ServiceRegistry {
  private Map<Class<?>, Object> services = new HashMap<>();
  private Map<Class<?>, ServiceConfiguration<?>> configurations = new HashMap<>();

  public <T, S extends T> void addService(Class<T> serviceType, S service) {
    services.put(serviceType, service);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getService(ServiceConfiguration<T> serviceConfiguration) {
    if (serviceConfiguration.getServiceType().equals(DatasetEntityStateService.class)) {
      DatasetEntityStateServiceConfig<?> config = (DatasetEntityStateServiceConfig) serviceConfiguration;
      try {
        return (T) DatasetEntityStateService.create(config.getEntityConfiguration(), this, () -> {});
      } catch (ServiceException e) {
        throw new AssertionError(e);
      }
    }
    Class<T> serviceType = serviceConfiguration.getServiceType();
    configurations.put(serviceType, serviceConfiguration);
    return (T) services.get(serviceType);
  }

  @Override
  public <T> Collection<T> getServices(ServiceConfiguration<T> serviceConfiguration) {
    return Collections.singleton(getService(serviceConfiguration));
  }
}
