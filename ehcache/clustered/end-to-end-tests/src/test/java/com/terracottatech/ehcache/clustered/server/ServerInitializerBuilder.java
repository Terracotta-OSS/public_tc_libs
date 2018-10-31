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

import com.terracottatech.config.data_roots.DataDirectoriesConfigImpl;
import com.terracottatech.data.config.DataDirectories;
import com.terracottatech.data.config.DataRootMapping;
import com.terracottatech.ehcache.clustered.client.internal.EnterpriseClusterTierClientEntityService;
import com.terracottatech.ehcache.clustered.client.internal.EnterpriseEhcacheClientEntityService;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ClusterTierManagerServerEntityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * A builder for a new {@link PassthroughTestHelpers.ServerInitializer} instance.  If no services are added using
 * {@link #serverEntityService(EntityServerService)} or {@link #clientEntityService(EntityClientService)},
 * this builder defines the following services for each {@code PassthroughServer} built:
 * <ul>
 *   <li>{@link ClusterTierManagerServerEntityService}</li>
 *   <li>{@link ClusterTierManagerClientEntityService}</li>
 *   <li>{@link VoltronReadWriteLockServerEntityService}</li>
 *   <li>{@link VoltronReadWriteLockEntityClientService}</li>
 * </ul>
 */
@SuppressWarnings("unused")
public final class ServerInitializerBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerInitializerBuilder.class);

  private final List<EntityServerService<?, ?>> serverEntityServices = new ArrayList<>();
  private final List<EntityClientService<?, ?, ? extends EntityMessage, ? extends EntityResponse, ?>> clientEntityServices =
      new ArrayList<>();
  private final Map<ServiceProvider, ServiceProviderConfiguration> serviceProviders =
      new IdentityHashMap<>();
  private final Map<ServiceProvider, ServiceProviderConfiguration> overrideServiceProviders =
      new IdentityHashMap<>();

  private final OffheapResourcesType resources = new OffheapResourcesType();
  private final DataDirectories dataDirectories = new DataDirectories();

  public ServerInitializerBuilder resource(String resourceName, int size, org.ehcache.config.units.MemoryUnit unit) {
    return this.resource(resourceName, size, convert(unit));
  }

  public ServerInitializerBuilder dataRoot(String id, String path) {
    DataRootMapping dataRootMapping = new DataRootMapping();
    dataRootMapping.setName(id); dataRootMapping.setValue(path);
    dataDirectories.getDirectory().add(dataRootMapping);
    return this;
  }

  private MemoryUnit convert(org.ehcache.config.units.MemoryUnit unit) {
    MemoryUnit convertedUnit;
    switch (unit) {
      case B:
        convertedUnit = MemoryUnit.B;
        break;
      case KB:
        convertedUnit = MemoryUnit.K_B;
        break;
      case MB:
        convertedUnit = MemoryUnit.MB;
        break;
      case GB:
        convertedUnit = MemoryUnit.GB;
        break;
      case TB:
        convertedUnit = MemoryUnit.TB;
        break;
      case PB:
        convertedUnit = MemoryUnit.PB;
        break;
      default:
        throw new UnsupportedOperationException("Unrecognized unit " + unit);
    }
    return convertedUnit;
  }

  private ServerInitializerBuilder resource(String resourceName, int size, MemoryUnit unit) {
    this.resources.getResource().add(PassthroughTestHelper.getResource(resourceName, size, unit));
    return this;
  }

  public ServerInitializerBuilder serviceProvider(ServiceProvider serviceProvider, ServiceProviderConfiguration configuration) {
    this.serviceProviders.put(serviceProvider, configuration);
    return this;
  }

  public ServerInitializerBuilder overrideServiceProvider(ServiceProvider serviceProvider, ServiceProviderConfiguration configuration) {
    this.overrideServiceProviders.put(serviceProvider, configuration);
    return this;
  }

  public ServerInitializerBuilder serverEntityService(EntityServerService<?, ?> service) {
    this.serverEntityServices.add(service);
    return this;
  }

  public ServerInitializerBuilder clientEntityService(EntityClientService<?, ?, ? extends EntityMessage, ? extends EntityResponse, ?> service) {
    this.clientEntityServices.add(service);
    return this;
  }

  public PassthroughTestHelpers.ServerInitializer build() {
    return newServer -> {
      /*
       * If services have been specified, don't establish the "defaults".
       */
      if (serverEntityServices.isEmpty() && clientEntityServices.isEmpty()) {
        newServer.registerServerEntityService(new EnterpriseEhcacheServerEntityService());
        newServer.registerClientEntityService(new EnterpriseEhcacheClientEntityService());
        newServer.registerServerEntityService(new EnterpriseClusterTierServerEntityService());
        newServer.registerClientEntityService(new EnterpriseClusterTierClientEntityService());
        newServer.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
        newServer.registerClientEntityService(new VoltronReadWriteLockEntityClientService());
      }

      for (EntityServerService<?, ?> service : serverEntityServices) {
        newServer.registerServerEntityService(service);
      }

      for (EntityClientService<?, ?, ? extends EntityMessage, ? extends EntityResponse, ?> service : clientEntityServices) {
        newServer.registerClientEntityService(service);
      }

      if (!this.resources.getResource().isEmpty()) {
        newServer.registerExtendedConfiguration(new OffHeapResourcesProvider(this.resources));
      }

      if (!this.dataDirectories.getDirectory().isEmpty()) {
        newServer.registerExtendedConfiguration(new DataDirectoriesConfigImpl(null, dataDirectories));
      }

      for (Map.Entry<ServiceProvider, ServiceProviderConfiguration> entry : serviceProviders.entrySet()) {
        LOGGER.debug(entry.getKey() + " :: " + entry.getValue());
        newServer.registerServiceProvider(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<ServiceProvider, ServiceProviderConfiguration> entry : overrideServiceProviders.entrySet()) {
        LOGGER.debug("OVERRIDE PROVIDER: " + entry.getKey() + " :: " + entry.getValue());
        newServer.registerOverrideServiceProvider(entry.getKey(), entry.getValue());
      }
    };
  }
}
