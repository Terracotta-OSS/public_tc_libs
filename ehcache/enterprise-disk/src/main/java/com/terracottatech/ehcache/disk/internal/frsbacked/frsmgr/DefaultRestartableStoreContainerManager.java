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

import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.store.Store.Configuration;
import org.ehcache.impl.internal.store.offheap.EhcacheConcurrentOffHeapClockCache;
import org.ehcache.impl.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.impl.internal.store.offheap.SwitchableEvictionAdvisor;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.EvictionListener;

import com.terracottatech.ehcache.disk.internal.common.RestartableLocalMap;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerManager;

import java.io.Serializable;

/**
 * Default implementation of the FRS store manager, which is used by FRS backed offHeap
 * stores belonging to caches of this cache manager.
 *
 * @author RKAV
 */
class DefaultRestartableStoreContainerManager implements FastRestartStoreContainerManager, RestartableMapProvider, CloseFrsContainer {
  private final LocalMetadataProvider metadataProvider;
  private final CacheStoreContainerManager cacheStoreContainerManager;
  private final FastRestartStoreStateRepository stateRepository;
  private final String frsId;

  private boolean started;
  private boolean bootstrapped;

  DefaultRestartableStoreContainerManager(LocalMetadataProvider metadataProvider, FileBasedPersistenceContext fileContext, String frsId) {
    this.metadataProvider = metadataProvider;
    this.stateRepository = new FastRestartStoreStateRepository(this);
    this.cacheStoreContainerManager = new CacheStoreContainerManager(fileContext, metadataProvider, this.stateRepository, frsId);
    this.started = false;
    this.bootstrapped = false;
    this.frsId = frsId;
  }

  synchronized void init() {
    if (started) {
      throw new IllegalStateException("Store manager is already initialized");
    }
    try {
      cacheStoreContainerManager.init();
      started = true;
    } finally {
      if (!started) {
        cacheStoreContainerManager.close();
      }
    }
  }

  @Override
  public synchronized void closeContainer() {
    if (started) {
      cacheStoreContainerManager.close();
    }
  }

  synchronized void destroyStore(String storeId) throws CachePersistenceException {
    if (!bootstrapped) {
      cacheStoreContainerManager.bootstrapWait();
      bootstrapped = true;
    }
    cacheStoreContainerManager.destroyStore(storeId);
  }

  FastRestartStoreStateRepository getLocalStateRepository() {
    return stateRepository;
  }

  @Override
  public synchronized <K, V> EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createOrAttachBackingMap(
      long sizeInBytes, String storeId,
      Configuration<K, V> storeConfig,
      SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor,
      EvictionListener<K, OffHeapValueHolder<V>> mapEvictionListener) {
    if (!bootstrapped) {
      cacheStoreContainerManager.bootstrapWait();
      bootstrapped = true;
    }
    return cacheStoreContainerManager.createOrAttachMap(
        metadataProvider.validateAndAddStore(storeId, frsId, storeConfig, sizeInBytes),
        storeId, storeConfig, sizeInBytes, evictionAdvisor, mapEvictionListener);
  }

  @Override
  public synchronized void detachBackingMap(String storeId) {
    cacheStoreContainerManager.detachBackingMap(storeId);
  }

  @Override
  public synchronized <K extends Serializable, V extends Serializable> RestartableLocalMap<K, V> createRestartableMap(
      String storeId, String composedId, Class<K> keyClass, Class<V> valueClass) {
    if (!bootstrapped) {
      cacheStoreContainerManager.bootstrapWait();
      bootstrapped = true;
    }
    boolean existing = metadataProvider.addStateRepository(composedId, storeId, frsId, keyClass, valueClass);
    if (existing) {
      // this will happen only if there was a late bootstrap
      return null;
    }
    return cacheStoreContainerManager.createRestartableMap(composedId, keyClass, valueClass);
  }
}