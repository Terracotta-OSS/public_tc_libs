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
package com.terracottatech.ehcache.common.frs.metadata.internal;

import com.terracottatech.ehcache.common.frs.RootPathProvider;
import com.terracottatech.ehcache.common.frs.metadata.ChildConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.FrsContainerIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProvider;
import com.terracottatech.ehcache.common.frs.metadata.RootConfiguration;
import com.terracottatech.frs.RestartStore;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of a metadata provider.
 * <p>
 * This implementation has the knowledge of multiple FRS spaces (a.k.a <i>FRS container</i>) spread across one or more
 * data roots. This implementation is now made thread safe, as the server side ehcache requires it to be thread safe
 * <p>
 * Note: Currently this is written compatible to JAVA 6 so that both standalone and clustered versions
 * of ehcache (and possibly TCStore in future) can use this library.
 */
public class GlobalMetadataManager<K1, V1 extends RootConfiguration, V2 extends ChildConfiguration<K1>>
    implements MetadataProvider<K1, V1, V2> {
  private final Class<K1> parentKeyType;
  private final Class<V1> parentType;
  private final Class<V2> mainChildType;
  private final Set<Class<? extends ChildConfiguration<String>>> otherChildTypes;
  private final Map<String, PerContainerMetadataStorage<K1, V1, V2>> metadataStorageMap;
  private final RootPathProvider rootPathProvider;

  public GlobalMetadataManager(final RootPathProvider rootPathProvider,
                               final Class<K1> parentKeyType,
                               final Class<V1> parentType,
                               final Class<V2> mainChildType,
                               final Set<Class<? extends ChildConfiguration<String>>> otherChildTypes) {
    this.parentKeyType = parentKeyType;
    this.parentType = parentType;
    this.mainChildType = mainChildType;
    this.otherChildTypes = otherChildTypes;
    this.rootPathProvider = rootPathProvider;
    this.metadataStorageMap = new ConcurrentHashMap<>();
    for (File containerDir : rootPathProvider.getAllExistingContainers()) {
      PerContainerMetadataStorage<K1, V1, V2> metadataStorage = new PerContainerMetadataStorage<>(
          containerDir, parentKeyType, parentType, mainChildType, otherChildTypes);
      metadataStorageMap.put(containerDir.getAbsolutePath(), metadataStorage);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<FrsDataLogIdentifier> bootstrapAllContainers() {
    final Set<FrsDataLogIdentifier> dataLogDirs = new HashSet<>();
    for (PerContainerMetadataStorage<K1, V1, V2> metadataStorage : metadataStorageMap.values()) {
      metadataStorage.bootstrapAsync();
    }
    for (PerContainerMetadataStorage<K1, V1, V2> metadataStorage : metadataStorageMap.values()) {
      dataLogDirs.addAll(metadataStorage.bootstrapWait());
    }
    return dataLogDirs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdownAllContainers() {
    for (PerContainerMetadataStorage<K1, V1, V2> metadataStorage : metadataStorageMap.values()) {
      metadataStorage.shutdown();
    }
    metadataStorageMap.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<K1, V1> getAllRootConfiguration() {
    final Map<K1, V1> parentConfigMap = new HashMap<>();
    for (PerContainerMetadataStorage<K1, V1, V2> metadataStorage : metadataStorageMap.values()) {
      parentConfigMap.putAll(metadataStorage.getRootConfiguration());
    }
    return parentConfigMap;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, V2> getFirstLevelChildConfigurationByRoot(K1 rootId) {
    final Map<String, V2> childConfigMap = new HashMap<>();
    for (PerContainerMetadataStorage<K1, V1, V2> metadataStorage : metadataStorageMap.values()) {
      childConfigMap.putAll(metadataStorage.getChildConfigurationByRoot(rootId));
    }
    return childConfigMap;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, V2> getFirstLevelChildConfigurationByDataLog(FrsDataLogIdentifier dataLogId) {
    try {
      return getMetadataStorage(dataLogId).getChildConfigurationByDataLog(dataLogId.getDataLogName());
    } catch (IllegalArgumentException e) {
      return Collections.emptyMap();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V3 extends ChildConfiguration<String>> Map<String, V3>
  getNthLevelOtherChildConfigurationByParent(String parentId, Class<V3> childType) {
    final Map<String, V3> childConfigMap = new HashMap<>();
    for (PerContainerMetadataStorage<K1, V1, V2> metadataStorage : metadataStorageMap.values()) {
      childConfigMap.putAll(metadataStorage.getOtherChildConfigurationByParent(parentId, childType));
    }
    return childConfigMap;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V3 extends ChildConfiguration<String>> Map<String, V3>
  getNthLevelOtherChildConfigurationByDataLog(FrsDataLogIdentifier dataLogId, Class<V3> childType) {
    try {
      return getMetadataStorage(dataLogId).getOtherChildConfigurationByLog(dataLogId.getDataLogName(), childType);
    } catch (IllegalArgumentException e) {
      return Collections.emptyMap();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V2 getChildConfigEntry(FrsContainerIdentifier containerId, String composedId) {
    try {
      return getMetadataStorage(containerId).getChildConfigurationById(composedId);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V3 extends ChildConfiguration<String>> V3 getNthLevelOtherChildConfigEntry(
      FrsContainerIdentifier containerId, String composedId, Class<V3> otherChildType) {
    try {
      return getMetadataStorage(containerId).getOtherChildConfigurationById(composedId, otherChildType);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addRootConfiguration(K1 rootId, V1 parentConfiguration) {
    boolean added = false;
    for (FrsContainerIdentifier containerId : parentConfiguration.getContainers()) {
      final File containerDir = idToContainerFile(containerId);
      if (containerDir == null) {
        throw new IllegalArgumentException("Raw root path " + containerId.getRootPathName() + " does not exist");
      }
      PerContainerMetadataStorage<K1, V1, V2> metadataStorage = metadataStorageMap.get(containerDir.getAbsolutePath());
      if (metadataStorage == null) {
        metadataStorage = new PerContainerMetadataStorage<>(containerDir,
            parentKeyType, parentType, mainChildType, otherChildTypes);
        metadataStorage.bootstrapAsync();
        metadataStorage.bootstrapWait();
        metadataStorageMap.put(containerDir.getAbsolutePath(), metadataStorage);
      }
      boolean wasAdded = metadataStorage.addRootConfiguration(rootId, parentConfiguration);
      if (wasAdded) {
        // was it existing in any container??
        added = true;
      }
    }
    return added;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addFirstLevelChildConfiguration(String composedId, V2 childConfiguration) {
    return getMetadataStorage(childConfiguration.getDataLogIdentifier())
        .addChildConfiguration(composedId, childConfiguration);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V3 extends ChildConfiguration<String>> boolean addNthLevelOtherChildConfiguration(String composedId,
                                                                                            V3 childConfiguration,
                                                                                            Class<V3> childType) {
    return getMetadataStorage(childConfiguration.getDataLogIdentifier())
        .addOtherChildConfiguration(composedId, childConfiguration, childType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeRootConfigurationFrom(FrsContainerIdentifier containerId, K1 rootId) {
    getMetadataStorage(containerId).removeRootConfiguration(rootId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeChildConfigurationFrom(FrsContainerIdentifier containerId, String childConfigurationId) {
    getMetadataStorage(containerId).removeChildConfiguration(childConfigurationId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V3 extends ChildConfiguration<String>> void removeNthLevelOtherChildConfigurationFrom(FrsContainerIdentifier containerId,
                                                                                                String childConfigurationId,
                                                                                                Class<V3> otherChildType) {
    getMetadataStorage(containerId).removeOtherChildConfiguration(childConfigurationId, otherChildType);
  }

  @Override
  public Map<String, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>> getMetaStores() {
    Map<String, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>> metadataMap =
        new HashMap<>();

    for (PerContainerMetadataStorage<K1, V1, V2> metadataStorage : metadataStorageMap.values()) {
      metadataMap.put(metadataStorage.getMetaStorePathAsString(), metadataStorage.getMetaStore());
    }
    return metadataMap;
  }

  private File idToContainerFile(FrsContainerIdentifier containerId) {
    final File rootFile = rootPathProvider.getRootPath(containerId.getRootPathName());
    if (rootFile == null) {
      return null;
    }
    return new File(rootFile, containerId.getContainerName());
  }

  private PerContainerMetadataStorage<K1, V1, V2> getMetadataStorage(FrsContainerIdentifier containerIdentifier) {
    final File containerDir = idToContainerFile(containerIdentifier);
    PerContainerMetadataStorage<K1, V1, V2> metadataStorage = null;
    if (containerDir != null) {
      metadataStorage =  metadataStorageMap.get(containerDir.getAbsolutePath());
    }
    if (metadataStorage == null) {
      final String container = containerDir == null ? "unknown" : containerDir.getAbsolutePath();
      throw new IllegalArgumentException("Metadata Storage for container " + container + " Not found");
    }
    return metadataStorage;
  }
}
