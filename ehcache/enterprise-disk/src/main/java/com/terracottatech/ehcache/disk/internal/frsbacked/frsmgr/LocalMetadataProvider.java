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

import org.ehcache.core.spi.store.Store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Metadata Provider interface required by {@link CacheStoreContainerManager}
 *
 * @author RKAV
 */
interface LocalMetadataProvider {
  Map<String, SavedStoreConfig> getExistingRestartableStores(String frsId);
  Map<String, SavedStateRepositoryConfig<?, ?>> getExistingRestartableStateRepositories(String frsId);
  Map<String, SavedStateRepositoryConfig<?, ?>> getRestartableStateRepositoriesForStore(String storeId);
  SavedStoreConfig getRestartableStoreConfig(String storeId);
  <K, V> boolean addStateRepository(String composedId, String storeId, String frsId, Class<K> keyType,
                                    Class<V> valueType);
  <K, V> boolean validateAndAddStore(String storeId, String frsId, Store.Configuration<K, V> storeConfig, long sizeInBytes);
  void removeStore(String storeId);
  void removeStateRepository(String composedId);
  ByteBuffer toStoreIdentifier(String storeId);
  ByteBuffer toStateRepositoryIdentifier(String composedId);
}