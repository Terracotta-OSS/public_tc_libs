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

package com.terracottatech.ehcache.clustered.server.services.pool;

import com.tc.classloader.BuiltinService;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.clustered.server.ResourcePoolManager;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.offheapresource.OffHeapResources;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author vmad
 */
@BuiltinService
public class ResourcePoolManagerServiceProvider implements ServiceProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourcePoolManagerServiceProvider.class);

  private final ConcurrentMap<String, ResourcePoolManager> resourcePoolManagers = new ConcurrentHashMap<>();

  private OffHeapResources offHeapResources;

  @Override
  public boolean initialize(ServiceProviderConfiguration serviceProviderConfiguration,
                            PlatformConfiguration platformConfiguration) {
    Collection<OffHeapResources> extendedConfiguration = platformConfiguration.getExtendedConfiguration(OffHeapResources.class);
    if (extendedConfiguration.size() > 1) {
      throw new UnsupportedOperationException("There are " + extendedConfiguration.size() + " OffHeapResourcesProvider, this is not supported. " +
        "There must be only one!");
    }
    Iterator<OffHeapResources> iterator = extendedConfiguration.iterator();
    if (iterator.hasNext()) {
      offHeapResources = iterator.next();
      if (offHeapResources.getAllIdentifiers().isEmpty()) {
        throw new UnsupportedOperationException("There are no offheap-resource defined, this is not supported. There must be at least one!");
      }
    } else {
      LOGGER.warn("No offheap-resource defined - this will prevent provider from offering any ResourcePoolManagerServiceProvider.");
    }
    return true;
  }

  @Override
  public synchronized <T> T getService(long l,
                          ServiceConfiguration<T> serviceConfiguration) {
    if(serviceConfiguration instanceof ResourcePoolManagerConfiguration) {
      ResourcePoolManagerConfiguration managerConfiguration = (ResourcePoolManagerConfiguration) serviceConfiguration;
      if (offHeapResources == null) {
        LOGGER.warn("ResourcePoolManager requested but no offheap-resource was defined - returning null");
        return null;
      }

      final String cacheManagerID = managerConfiguration.getCacheManagerID();
      final ServerSideConfiguration serverSideConfiguration = managerConfiguration.getServerSideConfiguration();
      if(resourcePoolManagers.get(cacheManagerID) == null) {
        if(serverSideConfiguration == null) {
          throw new AssertionError("Unexpected getService() call with null configuration");
        }
        ResourcePoolManager resourcePoolManager = new ResourcePoolManager(offHeapResources, () -> resourcePoolManagers.remove(cacheManagerID));
        try {
          resourcePoolManager.configure(serverSideConfiguration);
        } catch (ConfigurationException e) {
          throw new RuntimeException(e);
        }
        resourcePoolManagers.put(cacheManagerID, resourcePoolManager);
      }
      return serviceConfiguration.getServiceType().cast(resourcePoolManagers.get(cacheManagerID));
    }
    return null;
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    return Collections.singleton(ResourcePoolManager.class);
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
    try {
      resourcePoolManagers.values().forEach(ResourcePoolManager::destroy);
    } catch (Exception e) {
      throw new ServiceProviderCleanupException("Got exception while destroying ResourcePoolManager(s)", e);
    }
  }

  @CommonComponent
  public interface DestroyCallback {
    void destroy();
  }
}
