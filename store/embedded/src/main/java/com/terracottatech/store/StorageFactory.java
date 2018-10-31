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
package com.terracottatech.store;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.sovereign.impl.persistence.base.AbstractRestartabilityBasedStorage;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.impl.persistence.hybrid.SovereignHybridStorage;
import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import com.terracottatech.store.common.ExceptionFreeAutoCloseable;
import com.terracottatech.store.common.InterruptHelper;
import com.terracottatech.sovereign.resource.NamedBufferResources;
import com.terracottatech.sovereign.resource.SizedBufferResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.terracottatech.store.configuration.PersistentStorageType.defaultEngine;
import static com.terracottatech.store.configuration.PersistentStorageType.permanentIdToStorageType;
import static com.terracottatech.store.configuration.PersistentStorageType.FRS_PERMANENT_ID;
import static com.terracottatech.store.configuration.PersistentStorageType.HYBRID_PERMANENT_ID;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.NEW;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW;

public class StorageFactory implements ExceptionFreeAutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageFactory.class);
  private static final String STORAGE_TYPE_PROPERTY = "embedded.storage.type";

  private final Map<String, SizedBufferResource> offheapResources;
  private final Map<String, DiskResource> diskResources;
  private final Map<String, Future<ConfiguredSovereignStorage>> storageFutures = new ConcurrentHashMap<>();
  private final ThreadLocal<Map<Path, PersistenceRoot>> openedRoots = new ThreadLocal<>();

  public StorageFactory(Map<String, Long> offheapResourceSizes, Map<String, DiskResource> diskResources) {
    this.offheapResources = initialiseOffheapResources(offheapResourceSizes);
    this.diskResources = new ConcurrentHashMap<>(diskResources);
  }

  public void startup(DatasetDiscoveryListener listener) throws StoreException {
    assert (storageFutures.isEmpty());
    openedRoots.set(new HashMap<>());
    try {

      rationalizePersistenceModeOfDiskResources();

      int diskResourceCount = diskResources.size();
      Map<String, ConfiguredSovereignStorage> newStorages = new HashMap<>(diskResourceCount);

      try {
        for (Map.Entry<String, DiskResource> entry : diskResources.entrySet()) {
          String diskResourceName = entry.getKey();
          DiskResource diskResource = entry.getValue();
          if (diskResource.getPersistenceMode() == null) {
            // storage may be created at dataset level
            continue;
          }

          EmbeddedDatasetManagerBuilder.FileMode fileMode = diskResource.getFileMode();
          if (fileMode == NEW) {
            Path dataRoot = diskResource.getDataRoot();
            if (containsFiles(dataRoot)) {
              throw new StoreException("FileMode.NEW and disk resource: " + diskResourceName + " maps to existing path: " + dataRoot);
            }
          } else if (fileMode == REOPEN || fileMode == REOPEN_OR_NEW) {
            String storageKey = buildStorageKey(diskResourceName);

            ConfiguredSovereignStorage configuredSovereignStorage = createStorageWithLastRecordedOffheap(diskResource);
            SovereignStorage<?, ?> storage = configuredSovereignStorage.getStorage();

            newStorages.put(storageKey, configuredSovereignStorage);

            startupExistingStorage(storage);

            for (SovereignDataset<?> dataset : storage.getManagedDatasets()) {
              listener.foundExistingDataset(diskResourceName, dataset);
            }
          }
        }
      } catch (Throwable t) {
        for (ConfiguredSovereignStorage configuredSovereignStorage : newStorages.values()) {
          try {
            SovereignStorage<?, ?> storage = configuredSovereignStorage.getStorage();
            storage.shutdown();
          } catch (IOException e) {
            LOGGER.error("Unable to shutdown storage", e);
          }
        }
        throw t;
      }

      for (Map.Entry<String, ConfiguredSovereignStorage> entry : newStorages.entrySet()) {
        String storageKey = entry.getKey();
        ConfiguredSovereignStorage configuredSovereignStorage = entry.getValue();
        Future<ConfiguredSovereignStorage> existing = storageFutures.put(storageKey, CompletableFuture.completedFuture(configuredSovereignStorage));
        assert (existing == null);
      }
    } catch (Throwable t) {
      openedRoots.get().forEach((x, y) -> y.cleanIfEmpty());
      throw t;
    } finally {
      openedRoots.remove();
    }
  }

  private boolean containsFiles(Path dataRoot) throws StoreException {
    try {
      return PersistenceRoot.containsMetaOrDataFiles(dataRoot);
    } catch (IOException e) {
      throw new StoreException("Unable to check NEW state: " + dataRoot, e);
    }
  }

  public SovereignStorage<?, ?> getStorage(EmbeddedDatasetConfiguration configuration) throws StoreException {
    String storageKey = buildStorageKey(configuration);
    openedRoots.set(new HashMap<>());
    try {

      CompletableFuture<ConfiguredSovereignStorage> newStorage = new CompletableFuture<>();
      Future<ConfiguredSovereignStorage> existingStorage = storageFutures.putIfAbsent(storageKey, newStorage);

      if (existingStorage != null) {
        try {
          ConfiguredSovereignStorage configuredSovereignStorage = InterruptHelper.getUninterruptibly(existingStorage);
          SovereignStorage<?, ?> storage = configuredSovereignStorage.getStorage();
          if (storage instanceof AbstractRestartabilityBasedStorage && !((AbstractRestartabilityBasedStorage) storage).isActive()) {
            existingStorage = null;
            storageFutures.put(storageKey, newStorage);
          }
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          throw new StoreException(cause);
        }
      }

      if (existingStorage == null) {
        try {
          ConfiguredSovereignStorage configuredSovereignStorage = createStorage(configuration);
          newStorage.complete(configuredSovereignStorage);
          existingStorage = newStorage;
        } catch (Throwable t) {
          newStorage.completeExceptionally(t);
          throw t;
        }
      }

      try {
        ConfiguredSovereignStorage configuredSovereignStorage = InterruptHelper.getUninterruptibly(existingStorage);

        String offheapResourceName = configuration.getOffheapResource();
        configuredSovereignStorage.activate(offheapResourceName);

        return configuredSovereignStorage.getStorage();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        throw new StoreException(cause);
      }
    } catch(Throwable t) {
      openedRoots.get().forEach((x, y) -> y.cleanIfEmpty());
      throw t;
    } finally {
      openedRoots.remove();
    }
  }

  void shutdownStorage(String storageKey, SovereignStorage<?,?> storage) throws IOException {
    storageFutures.remove(storageKey);
    storage.shutdown();
  }

  @Override
  public void close() {
    for (Future<ConfiguredSovereignStorage> storageFuture : storageFutures.values()) {
      try {
        ConfiguredSovereignStorage configuredSovereignStorage = InterruptHelper.getUninterruptibly(storageFuture);
        SovereignStorage<?, ?> storage = configuredSovereignStorage.getStorage();
        storage.shutdown();
      } catch (ExecutionException e) {
        // The storage was never properly created, so no need to shut it down.
      } catch (IOException e) {
        LOGGER.error("Unable to shutdown storage", e);
      }
    }
    storageFutures.clear();
  }

  private ConfiguredSovereignStorage createStorage(EmbeddedDatasetConfiguration configuration) throws StoreException {
    String offheapResourceName = configuration.getOffheapResource();
    Optional<DiskResource> possibleDiskResource = getDiskResource(configuration);

    if (possibleDiskResource.isPresent()) {
      DiskResource diskResource = possibleDiskResource.get();
      return createStorage(diskResource, offheapResourceName);
    } else {
      NamedBufferResources bufferResource = new NamedBufferResources(offheapResources, offheapResourceName);
      SovereignStorage<?, ?> storage = new StorageTransient(bufferResource);
      return new ConfiguredSovereignStorage(bufferResource, storage);
    }
  }

  private ConfiguredSovereignStorage createStorage(DiskResource diskResource, String offheapResourceName) throws StoreException {
    NamedBufferResources bufferResource = new NamedBufferResources(offheapResources, offheapResourceName);

    SovereignStorage<?, ?> sovereignStorage = createDiskStorage(diskResource, bufferResource);
    startupStorage(sovereignStorage);

    return new ConfiguredSovereignStorage(bufferResource, sovereignStorage);
  }

  private ConfiguredSovereignStorage createStorageWithLastRecordedOffheap(DiskResource diskResource) throws StoreException {
    NamedBufferResources bufferResource = new NamedBufferResources(offheapResources);
    SovereignStorage<?, ?> sovereignStorage = createDiskStorage(diskResource, bufferResource);

    return new ConfiguredSovereignStorage(bufferResource, sovereignStorage);
  }

  private void startupExistingStorage(SovereignStorage<?, ?> sovereignStorage) throws StoreException {
    NamedBufferResources bufferResource = (NamedBufferResources) sovereignStorage.getBufferResource();

    startupMetadata(sovereignStorage);

    sovereignStorage.getDataSetDescriptions().stream()
            .map(SovereignDatasetDescription::getOffheapResourceName)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .distinct()
            .forEach(bufferResource::activate);

    startupData(sovereignStorage);
  }

  private SovereignStorage<?, ?> createDiskStorage(DiskResource diskResource, SovereignBufferResource bufferResource)
      throws StoreException {
    PersistenceRoot persistenceRoot = getPersistenceRoot(diskResource);
    PersistentStorageType storageType = diskResource.getPersistenceMode();
    writeProperty(persistenceRoot, storageType);

    switch (storageType.getPermanentId()) {
      case FRS_PERMANENT_ID:
        return new SovereignFRSStorage(persistenceRoot, bufferResource);
      case HYBRID_PERMANENT_ID:
        return new SovereignHybridStorage(persistenceRoot, bufferResource);
      default:
        throw new AssertionError("Unknown persistence mode: " + storageType.getShortName());
    }
  }

  private static Map<String, SizedBufferResource> initialiseOffheapResources(Map<String, Long> offheapResourceSizes) {
    int offheapResourcesCount = offheapResourceSizes.size();
    Map<String, SizedBufferResource> offheapResources = new ConcurrentHashMap<>(offheapResourcesCount);

    for (Map.Entry<String, Long> entry : offheapResourceSizes.entrySet()) {
      String offheapResourceName = entry.getKey();
      long offheapResourceSize = entry.getValue();
      SizedBufferResource offheapResource = new SizedBufferResource(offheapResourceSize);
      offheapResources.put(offheapResourceName, offheapResource);
    }

    return offheapResources;
  }

  private void rationalizePersistenceModeOfDiskResources() throws StoreException {
    try {
      for (Map.Entry<String, DiskResource> entry : diskResources.entrySet()) {
        String dataRoot = entry.getKey();
        DiskResource diskResource = entry.getValue();
        PersistenceRoot persistenceRoot = getPersistenceRoot(diskResource);
        Properties props = persistenceRoot.loadProperties();
        PersistentStorageType specifiedStorageType = diskResource.getPersistenceMode();
        PersistentStorageType actualStorageType = matchStorageTypeToMode(specifiedStorageType,
            props.getProperty(STORAGE_TYPE_PROPERTY));
        if (specifiedStorageType != actualStorageType) {
          // overwrite with correct storage type
          diskResources.put(dataRoot, new DiskResource(diskResource.getDataRoot(), actualStorageType,
              diskResource.getFileMode()));
        }
      }
    } catch (IOException e) {
      throw new StoreException(e);
    }
  }

  private PersistentStorageType matchStorageTypeToMode(PersistentStorageType specifiedStorageType,
                                                 String storageType) throws StoreException {
    if (storageType == null || storageType.isEmpty()) {
      return specifiedStorageType;
    }
    int storageId = Integer.parseInt(storageType);
    PersistentStorageType actualStorageType = permanentIdToStorageType(storageId);
    if (actualStorageType == null) {
      throw new StoreException("Unexpected Corrupt Properties File Detected. " +
                               "Please delete the properties file to continue");
    }
    if (specifiedStorageType == null || storageId == specifiedStorageType.getPermanentId()) {
      return actualStorageType;
    } else {
      throw new StoreException("Incompatible Storage Types Specified. Specified storage type `"
                               + specifiedStorageType.getShortName()
                               + "` not compatible with current storage type `"
                               + actualStorageType.getShortName());
    }
  }

  private Optional<DiskResource> getDiskResource(EmbeddedDatasetConfiguration configuration) {
    Optional<String> diskResourceName = configuration.getDiskResource();
    return diskResourceName.map(diskResources::get);
  }

  private static void startupStorage(SovereignStorage<?, ?> storage) throws StoreException {
    startupMetadata(storage);
    startupData(storage);
  }

  private static void startupMetadata(SovereignStorage<?, ?> storage) throws StoreException {
    boolean interrupted = false;

    try {
      Future<Void> metadata = storage.startupMetadata();
      while (true) {
        try {
          metadata.get();
          break;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          throw new IOException("Metadata recovery failed.", cause);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (IOException e) {
      throw new StoreException(e);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void startupData(SovereignStorage<?, ?> storage) throws StoreException {
    boolean interrupted = false;

    try {
      Future<Void> metadata = storage.startupData();
      while (true) {
        try {
          metadata.get();
          break;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          throw new IOException("Data recovery failed.", cause);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (IOException e) {
      throw new StoreException(e);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private PersistenceRoot getPersistenceRoot(DiskResource diskResource) throws StoreException {
    Path dataRoot = diskResource.getDataRoot();
    Map<Path, PersistenceRoot> rootMap = openedRoots.get();
    PersistenceRoot newRoot = rootMap.get(dataRoot);
    if (newRoot == null) {
      newRoot = createNewPersistentRoot(dataRoot, diskResource);
      rootMap.put(dataRoot, newRoot);
    }
    return newRoot;
  }

  private PersistenceRoot createNewPersistentRoot(Path dataRoot, DiskResource diskResource) throws StoreException {
    File dataRootAsFile = dataRoot.toFile();

    EmbeddedDatasetManagerBuilder.FileMode fileMode = diskResource.getFileMode();
    PersistenceRoot.Mode sovereignFileMode = mapToSovereignFileMode(fileMode);

    try {
      return new PersistenceRoot(dataRootAsFile, sovereignFileMode);
    } catch (IOException e) {
      throw new StoreException(e);
    }
  }

  private static PersistenceRoot.Mode mapToSovereignFileMode(EmbeddedDatasetManagerBuilder.FileMode mode) {
    switch (mode) {
      case NEW:
        return PersistenceRoot.Mode.ONLY_IF_NEW;
      case REOPEN:
        return PersistenceRoot.Mode.REOPEN;
      case OVERWRITE:
        return PersistenceRoot.Mode.CREATE_NEW;
      case REOPEN_OR_NEW:
        return PersistenceRoot.Mode.DONTCARE;
      default:
        throw new AssertionError("Unknown FileMode: " + mode);
    }
  }

  static String buildStorageKey(EmbeddedDatasetConfiguration configuration) {
    String offheapResource = configuration.getOffheapResource();
    Optional<String> diskResource = configuration.getDiskResource();
    return buildStorageKey(offheapResource, diskResource);
  }

  private static String buildStorageKey(String offheapResource, Optional<String> diskResource) {
    return diskResource.map(StorageFactory::buildStorageKey).orElse("OFFHEAP:" + offheapResource);
  }

  static String buildStorageKey(String diskResourceName) {
    return "DISK:" + diskResourceName;
  }

  public void checkConfiguration(EmbeddedDatasetConfiguration configuration) {
    String offheapResourceName = configuration.getOffheapResource();
    if (!offheapResources.containsKey(offheapResourceName)) {
      throw new IllegalArgumentException("Unknown offheap resource: " + offheapResourceName);
    }

    Optional<String> diskResource = configuration.getDiskResource();
    diskResource.ifPresent(diskResourceName -> validateDatasetPersistentMode(diskResourceName,
        configuration.getPersistentStorageType().orElse(null)));
  }

  private void validateDatasetPersistentMode(String diskResourceName,
                                             PersistentStorageType datasetStorageType) {
    DiskResource mgrConfig = diskResources.get(diskResourceName);
    if (mgrConfig == null) {
      throw new IllegalArgumentException("Unknown disk resource: " + diskResourceName);
    }
    PersistentStorageType effectiveStorageType = getEffectiveStorageType(mgrConfig.getPersistenceMode(),
        datasetStorageType);
    if (effectiveStorageType != null) {
      if (mgrConfig.getPersistenceMode() == null) {
        // update the effective mode
        diskResources.put(diskResourceName, new DiskResource(mgrConfig.getDataRoot(), effectiveStorageType,
            mgrConfig.getFileMode()));
      }
    } else {
      throw new IllegalArgumentException("Incompatible Storage Types Specified. Specified storage type `"
                               + mgrConfig.getPersistenceMode().getShortName()
                               + "` not compatible with current storage type `"
                               + datasetStorageType.getShortName());

    }
  }

  private PersistentStorageType getEffectiveStorageType(PersistentStorageType mgrMode,
                                                        PersistentStorageType datasetMode) {
    PersistentStorageType defaultType = defaultEngine();
    if (mgrMode == null) {
      // dataset level specification
      return (datasetMode == null) ? defaultType : datasetMode;
    } else {
      // manager level specification OR existing storage for the disk resource
      return (datasetMode != null && datasetMode.getPermanentId() != mgrMode.getPermanentId()) ? null : mgrMode;
    }
  }

  private void writeProperty(PersistenceRoot persistenceRoot, PersistentStorageType mode) throws StoreException {
    Properties props = new Properties();
    props.setProperty(STORAGE_TYPE_PROPERTY, Integer.toString(mode.getPermanentId()));
    try {
      persistenceRoot.storeProperties(props);
    } catch (IOException e) {
      throw new StoreException(e);
    }
  }
}
