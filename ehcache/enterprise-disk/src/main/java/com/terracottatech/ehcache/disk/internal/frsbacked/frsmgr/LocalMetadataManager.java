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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.store.Store.Configuration;

import com.terracottatech.ehcache.common.frs.metadata.FrsContainerIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProvider;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProviderBuilder;
import com.terracottatech.ehcache.common.frs.RootPathProvider;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.terracottatech.ehcache.common.frs.FrsCodecFactory.toByteBuffer;

/**
 * Manages and stores the metadata required for fast restart store functionality. Currently the local
 * (standalone) FRS version is single root, single container per cache manager system, multi data log system.
 * <p>
 * Note: This class is not thread safe as it is assumed that client does appropriate protection, if needed
 *
 * @author RKAV
 */
final class LocalMetadataManager implements LocalMetadataProvider {
  // for local ehcache we have only a single root. Make it a well known hardcoded name for now.
  private static final String DEFAULT_ROOT = "root";

  private static final String CACHE_PREFIX = "__cache__";
  private static final String STATE_REPOSITORY_PREFIX = "__sr__";
  private static final String DIVIDER = ":";

  private final FileBasedPersistenceContext frsContext;
  private MetadataProvider<String, SavedCacheManagerConfig, SavedStoreConfig> metadataProvider;
  private final String containerName;
  private final FrsContainerIdentifier frsContainerId;

  @SuppressWarnings("unchecked")
  LocalMetadataManager(FileBasedPersistenceContext fileContext) {
    this.frsContext = fileContext;
    this.containerName = frsContext.getDirectory().getName();

    this.frsContainerId = new FrsContainerIdentifier(DEFAULT_ROOT, containerName);
  }

  @SuppressWarnings("unchecked")
  void init() {
    MetadataProviderBuilder<String, SavedCacheManagerConfig, SavedStoreConfig> metadataStoreBuilder =
        MetadataProviderBuilder.metadata(new LocalPathProvider(), String.class,
            SavedCacheManagerConfig.class, SavedStoreConfig.class);
    metadataStoreBuilder = metadataStoreBuilder.withOtherChildType(SavedStateRepositoryConfig.class);
    metadataProvider = metadataStoreBuilder.build();
    metadataProvider.bootstrapAllContainers();
    SavedCacheManagerConfig cmConfig = new SavedCacheManagerConfig(containerName,
        Collections.singleton(frsContainerId));
    metadataProvider.addRootConfiguration(containerName, cmConfig);
  }

  void close() {
    final MetadataProvider<String, SavedCacheManagerConfig, SavedStoreConfig> localRef = metadataProvider;
    if (localRef != null) {
      metadataProvider = null;
      localRef.shutdownAllContainers();
    }
  }

  @Override
  public <K, V> boolean validateAndAddStore(String storeId, String frsId,
                                            Configuration<K, V> storeConfig,
                                            long sizeInBytes) {
    final FrsDataLogIdentifier dataLogId = new FrsDataLogIdentifier(DEFAULT_ROOT, containerName, frsId);
    SavedStoreConfig newConfig = new SavedStoreConfig(dataLogId, containerName, storeConfig, sizeInBytes);
    return !metadataProvider.addFirstLevelChildConfiguration(storeId, newConfig);
  }

  @Override
  public void removeStore(String storeId) {
    metadataProvider.removeChildConfigurationFrom(frsContainerId, storeId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void removeStateRepository(String composedId) {
    metadataProvider.removeNthLevelOtherChildConfigurationFrom(frsContainerId, composedId, SavedStateRepositoryConfig.class);
  }

  @Override
  public ByteBuffer toStoreIdentifier(String storeId) {
    return toByteBuffer(CACHE_PREFIX + storeId + DIVIDER +
                        metadataProvider.getChildConfigEntry(frsContainerId, storeId).getObjectIndex());
  }

  @SuppressWarnings("unchecked")
  @Override
  public ByteBuffer toStateRepositoryIdentifier(String composedId) {
    return toByteBuffer(STATE_REPOSITORY_PREFIX + composedId + DIVIDER +
                        metadataProvider.getNthLevelOtherChildConfigEntry(frsContainerId, composedId,
                            SavedStateRepositoryConfig.class).getObjectIndex());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K1, V1> boolean addStateRepository(String composedId, String storeId, String frsId,
                                             Class<K1> keyType, Class<V1> valueType) {
    final FrsDataLogIdentifier dataLogId = new FrsDataLogIdentifier(DEFAULT_ROOT, containerName, frsId);
    SavedStateRepositoryConfig<K1, V1> newConfig = new SavedStateRepositoryConfig<>(storeId, dataLogId, keyType, valueType);
    return !metadataProvider.addNthLevelOtherChildConfiguration(composedId, newConfig, SavedStateRepositoryConfig.class);
  }

  @Override
  public Map<String, SavedStoreConfig> getExistingRestartableStores(final String frsId) {
    final FrsDataLogIdentifier dataLogId = new FrsDataLogIdentifier(DEFAULT_ROOT, containerName, frsId);
    return metadataProvider.getFirstLevelChildConfigurationByDataLog(dataLogId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, SavedStateRepositoryConfig<?, ?>> getExistingRestartableStateRepositories(final String frsId) {
    final FrsDataLogIdentifier dataLogId = new FrsDataLogIdentifier(DEFAULT_ROOT, containerName, frsId);
    return (Map<String, SavedStateRepositoryConfig<?, ?>>) (Map<?, ?>) metadataProvider
            .getNthLevelOtherChildConfigurationByDataLog(dataLogId, SavedStateRepositoryConfig.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, SavedStateRepositoryConfig<?, ?>> getRestartableStateRepositoriesForStore(final String storeId) {
    return (Map<String, SavedStateRepositoryConfig<?, ?>>) (Map<?, ?>) metadataProvider
            .getNthLevelOtherChildConfigurationByParent(storeId, SavedStateRepositoryConfig.class);
  }

  @Override
  public SavedStoreConfig getRestartableStoreConfig(String storeId) {
    return metadataProvider.getChildConfigEntry(frsContainerId, storeId);
  }

  private class LocalPathProvider implements RootPathProvider {
    @Override
    public File getRootPath(String rootName) {
      return frsContext.getDirectory().getParentFile();
    }

    @Override
    public File getConfiguredRoot(String rootName) {
      return getRootPath(rootName);
    }

    @Override
    public Set<File> getAllExistingContainers() {
      return Collections.singleton(frsContext.getDirectory());
    }
  }
}
