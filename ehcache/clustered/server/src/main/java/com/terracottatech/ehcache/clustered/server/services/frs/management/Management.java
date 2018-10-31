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
package com.terracottatech.ehcache.clustered.server.services.frs.management;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.frs.RestartStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.service.monitoring.EntityManagementRegistry;
import org.terracotta.management.service.monitoring.ManageableServerComponent;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author Mathieu Carbou
 */
@CommonComponent
public class Management implements ManageableServerComponent {

  private static final Logger LOGGER = LoggerFactory.getLogger(Management.class);

  private final List<EntityManagementRegistry> registries = new CopyOnWriteArrayList<>();
  private final List<RestartStoreBinding> bindings = new CopyOnWriteArrayList<>();

  public Collection<RestartStoreBinding> getBindings() {
    return new ArrayList<>(bindings);
  }

  @Override
  public void onManagementRegistryCreated(EntityManagementRegistry registry) {
    long consumerId = registry.getMonitoringService().getConsumerId();
    LOGGER.trace("[{}] onManagementRegistryCreated()", consumerId);
    registry.addManagementProvider(new EhcachePersistenceSettingsManagementProvider());
    registry.addManagementProvider(new EhcachePersistenceStatisticsManagementProvider());
    bindings.forEach(registry::register);
    registry.refresh();
    registries.add(registry);
  }

  @Override
  public void onManagementRegistryClose(EntityManagementRegistry registry) {
    registries.remove(registry);
    bindings.forEach(registry::unregister);
  }

  public void registerRestartStore(FrsDataLogIdentifier frsDataLogIdentifier, File logDirectory, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore) {
    RestartStoreBinding binding = new RestartStoreBinding(frsDataLogIdentifier, restartStore, logDirectory);
    bindings.add(binding);
    registries.forEach(registry -> registry.registerAndRefresh(binding)
        .thenRun(() -> registry.pushServerEntityNotification(binding, "EHCACHE_RESTART_STORE_CREATED")));
  }

  public void deRegisterRestartStore(FrsDataLogIdentifier frsDataLogIdentifier) {
    List<RestartStoreBinding> bindings = this.bindings.stream()
        .filter(binding -> binding.getFrsDataLogIdentifier().equals(frsDataLogIdentifier))
        .collect(Collectors.toList());
    this.bindings.removeAll(bindings);
    registries.forEach(registry -> {
      bindings.forEach(binding -> {
        registry.pushServerEntityNotification(binding, "EHCACHE_RESTART_STORE_DESTROYED");
        registry.unregister(binding);
      });
      registry.refresh();
    });
  }
}
