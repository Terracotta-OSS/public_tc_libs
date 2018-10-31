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
package com.terracottatech.store.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.management.service.monitoring.ManageableServerComponent;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapresource.OffHeapResources;

import com.tc.classloader.BuiltinService;
import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.config.data_roots.DataDirectoriesConfig;
import com.terracottatech.store.server.storage.configuration.StorageType;
import com.terracottatech.store.server.storage.factory.BackupCoordinatorFactory;
import com.terracottatech.store.server.storage.factory.DiskResourceStorageMappingFactory;
import com.terracottatech.store.server.storage.factory.ErrorStorageFactory;
import com.terracottatech.store.server.storage.factory.FRSStorageFactory;
import com.terracottatech.store.server.storage.factory.HybridStorageFactory;
import com.terracottatech.store.server.storage.factory.InternalStorageFactory;
import com.terracottatech.store.server.storage.factory.InterningStorageFactory;
import com.terracottatech.store.server.storage.factory.MemoryStorageFactory;
import com.terracottatech.store.server.storage.factory.PolymorphicStorageFactory;
import com.terracottatech.store.server.storage.factory.ResourceActivatingStorageFactory;
import com.terracottatech.store.server.storage.factory.RestartabilityBackupCoordinatorFactory;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import com.terracottatech.store.server.storage.management.DatasetStorageManagement;
import com.terracottatech.store.server.storage.offheap.BufferResourceFactory;
import com.terracottatech.store.server.storage.offheap.NamedBufferResourceFactory;
import com.terracottatech.store.server.storage.offheap.OffheapBufferResource;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@BuiltinService
public class StorageFactoryProvider implements ServiceProvider, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageFactoryProvider.class);

  private InternalStorageFactory storageFactory;
  private final DatasetStorageManagement datasetStorageManagement = new DatasetStorageManagement();

  @Override
  public boolean initialize(ServiceProviderConfiguration configuration, PlatformConfiguration platformConfiguration) {
    LOGGER.info("Initializing StorageFactoryProvider");
    storageFactory = createStorageFactory(platformConfiguration);
    return true;
  }

  @Override
  public <T> T getService(long consumerID, ServiceConfiguration<T> serviceConfiguration) {
    if (serviceConfiguration.getServiceType() == ManageableServerComponent.class) {
      return serviceConfiguration.getServiceType().cast(datasetStorageManagement);
    }

    Class<T> requiredServiceType = serviceConfiguration.getServiceType();
    if (requiredServiceType.isAssignableFrom(BackupCapable.class)) {
      return requiredServiceType.cast(storageFactory.getBackupCoordinator());
    }

    if (!requiredServiceType.isAssignableFrom(StorageFactory.class)) {
      return null;
    }

    return requiredServiceType.cast(storageFactory);
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    return Arrays.asList(StorageFactory.class, ManageableServerComponent.class, BackupCapable.class);
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
    try {
      storageFactory.prepareForSynchronization();
    } catch (RuntimeException e) {
      throw new ServiceProviderCleanupException("Prepare for synchronization failed", e);
    }
  }

  public void close() throws IOException {
    try {
      storageFactory.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private InternalStorageFactory createStorageFactory(PlatformConfiguration platformConfiguration) {
    try {
      Map<String, OffheapBufferResource> bufferResources = getBufferResources(platformConfiguration);
      BufferResourceFactory bufferResourceFactory = new NamedBufferResourceFactory(bufferResources);

      Optional<DataDirectories> dataRootConfig = getDataRootConfig(platformConfiguration);

      // restartability based storage systems requires a restartability based backup coordinator
      BackupCoordinatorFactory backupCoordinatorFactory = new RestartabilityBackupCoordinatorFactory();

      InternalStorageFactory frsStorageFactory = dataRootConfig.map(drc ->
          (InternalStorageFactory) new FRSStorageFactory(bufferResourceFactory, drc, datasetStorageManagement, backupCoordinatorFactory))
              .orElse(new ErrorStorageFactory("No data directories are configured on the server"));

      InternalStorageFactory hybridStorageFactory = dataRootConfig.map(drc ->
          (InternalStorageFactory) new HybridStorageFactory(bufferResourceFactory, drc, datasetStorageManagement, backupCoordinatorFactory))
          .orElse(new ErrorStorageFactory("No data directories are configured on the server"));

      Map<StorageType, InternalStorageFactory> factoryMap = new HashMap<>();
      factoryMap.put(StorageType.MEMORY, new MemoryStorageFactory(bufferResourceFactory));
      factoryMap.put(StorageType.FRS, frsStorageFactory);
      factoryMap.put(StorageType.HYBRID, hybridStorageFactory);

      return new ResourceActivatingStorageFactory(
              new InterningStorageFactory(
                  new DiskResourceStorageMappingFactory(
                      new PolymorphicStorageFactory(factoryMap))));
    } catch (Exception e) {
      LOGGER.debug("Failed to initialize StorageFactory", e);
      return new ErrorStorageFactory("Failed to initialize StorageFactory", e);
    }
  }

  private Map<String, OffheapBufferResource> getBufferResources(PlatformConfiguration platformConfiguration) throws StorageResourceException {
    Collection<OffHeapResources> plugins = platformConfiguration.getExtendedConfiguration(OffHeapResources.class);

    if (plugins.size() > 1) {
      throw new StorageResourceException("Multiple offheap resource configurations found in the server configuration");
    }

    Map<String, OffheapBufferResource> resources = new HashMap<>();

    for (OffHeapResources offHeapResources : plugins) {
      Set<OffHeapResourceIdentifier> identifiers = offHeapResources.getAllIdentifiers();
      for (OffHeapResourceIdentifier identifier : identifiers) {
        String name = identifier.getName();
        OffHeapResource offHeapResource = offHeapResources.getOffHeapResource(identifier);
        OffheapBufferResource offheapBufferResource = new OffheapBufferResource(offHeapResource);
        OffheapBufferResource existing = resources.put(name, offheapBufferResource);
        assert(existing == null);
      }
    }

    if (resources.isEmpty()) {
      throw new StorageResourceException("No offheap resources have been configured on the server");
    }

    return resources;
  }

  private Optional<DataDirectories> getDataRootConfig(PlatformConfiguration platformConfiguration) throws StorageResourceException {
    Collection<DataDirectoriesConfig> dataDirectoriesConfigs = platformConfiguration.getExtendedConfiguration(DataDirectoriesConfig.class);

    if (dataDirectoriesConfigs.size() > 1) {
      throw new StorageResourceException("Multiple data directories configurations found in server configuration");
    }

    return dataDirectoriesConfigs.stream().findFirst().map(orig -> orig.getDataDirectoriesForServer(platformConfiguration));
  }
}
