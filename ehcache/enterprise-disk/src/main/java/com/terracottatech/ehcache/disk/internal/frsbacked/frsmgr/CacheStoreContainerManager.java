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
import org.ehcache.impl.internal.store.offheap.HeuristicConfiguration;
import org.ehcache.impl.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.impl.internal.store.offheap.SwitchableEvictionAdvisor;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.EvictionListener;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;
import com.terracottatech.ehcache.common.frs.RestartLogLifecycle;
import com.terracottatech.ehcache.disk.internal.common.RestartableLocalMap;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerManager;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.ehcache.impl.internal.store.offheap.OffHeapStoreUtils.getBufferSource;

/**
 * Creates and manages the cache data area of the FRS store.
 * Uses the {@link LocalMetadataProvider} interface to access metadata managed by the
 * {@link LocalMetadataManager} of this {@link FastRestartStoreContainerManager}
 * <p>
 * It is assumed that the calls to this class are already protected by the main {@link DefaultRestartableStoreContainerManager}
 * which manages this manager.
 *
 * @author RKAV
 */
final class CacheStoreContainerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheStoreContainerManager.class);

  private final RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> cacheObjectManager;
  private final FileBasedPersistenceContext frsContext;
  private final LocalMetadataProvider metadataProvider;
  private final RepositoryLoader stateRepositoryLoader;
  private final Map<String, WrapperEnclosure<?, ?>> allStores;
  private final List<EhcacheConcurrentOffHeapClockCache<?, ?>> toDestroyMapList;
  private final String frsId;

  private ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> cacheDataStoreArea;
  private RestartLogLifecycle dataStoreLifecycle;

  CacheStoreContainerManager(FileBasedPersistenceContext fileContext,
                             LocalMetadataProvider metadataProvider,
                             RepositoryLoader stateRepositoryLoader,
                             String frsId) {
    this.metadataProvider = metadataProvider;
    this.frsContext = fileContext;
    this.cacheObjectManager = new RegisterableObjectManager<>();
    this.allStores = new HashMap<>();
    this.toDestroyMapList = new ArrayList<>();
    this.stateRepositoryLoader = stateRepositoryLoader;
    this.frsId = frsId;
    this.dataStoreLifecycle = null;
  }

  /**
   * Initialize the cache data area where all cache data are stored.
   */
  void init() {
    initCacheStore();
    this.dataStoreLifecycle = new RestartLogLifecycle(this.frsContext.getDirectory(), cacheDataStoreArea);
    bootstrapAllExistingStores();
    this.dataStoreLifecycle.startStoreAsync();
  }

  /**
   * Close and shutdown the cache data area of FRS.
   */
  void close() {
    final ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> localRef = cacheDataStoreArea;
    LOGGER.info("Shutting down cache store");
    if (localRef != null) {
      bootstrapWait();
      closeAllExistingStores();
      cacheDataStoreArea = null;
      dataStoreLifecycle = null;
    }
  }

  /**
   * Wait for the boot strapping process to complete for the container.
   */
  void bootstrapWait() {
    dataStoreLifecycle.waitOnCompletion();
  }

  /**
   * Destroy the store and all its dependent restartable objects.
   *
   * @param storeId Id of the store to destroy
   */
  void destroyStore(String storeId) throws CachePersistenceException {
    // first destroy state repositories
    Map<String, SavedStateRepositoryConfig<?, ?>> stateRepositoryMap = metadataProvider.getRestartableStateRepositoriesForStore(storeId);
    try {
      for (String composedId : stateRepositoryMap.keySet()) {
        stateRepositoryLoader.destroy(composedId);
        cacheObjectManager.unregisterStripe(metadataProvider.toStateRepositoryIdentifier(composedId));
        metadataProvider.removeStateRepository(composedId);
      }
      WrapperEnclosure<?, ?> mapEnclosure = allStores.remove(storeId);
      if (mapEnclosure == null) {
        // if it is neither attached or detached..this is a non existing store
        throw new CachePersistenceException("Trying to destroy a non existing store with alias " + storeId);
      }
      final EhcacheConcurrentOffHeapClockCache<?, ?> localMap = mapEnclosure.getMap();
      if (localMap != null) {
        localMap.clear();
        // cannot destroy map now due to compaction
        toDestroyMapList.add(localMap);
      }
      cacheObjectManager.unregisterStripe(metadataProvider.toStoreIdentifier(storeId));
      metadataProvider.removeStore(storeId);
    } catch (RuntimeException te) {
      throw new CachePersistenceException("Unable to destroy all objects for store " + storeId + " within " + frsId
                                                                                               + " container");
    }
  }

  /**
   * Attach a recovered map if it exists, otherwise create a new backing map.
   *
   * @param existingStore true, if the store was already existing
   * @param storeId Store Identifier
   * @param storeConfig Store Configuration
   * @param sizeInBytes Size of the FRS pool in bytes for this store
   * @param evictionAdvisor Eviction advisor that needs to be passed to the backing map
   * @param mapEvictionListener Eviction listener that needs to be passed to the backing map
   * @param <K> Key type
   * @param <V> Value type
   * @return the attached or created backing map.
   */
  <K, V> EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createOrAttachMap(
      boolean existingStore, String storeId, Configuration<K, V> storeConfig, long sizeInBytes,
      SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor,
      EvictionListener<K, OffHeapValueHolder<V>> mapEvictionListener) {
    if (!existingStore) {
      buildMapWithWrappers(storeId, sizeInBytes);
    }
    return attachBackingMap(storeId, storeConfig, evictionAdvisor, mapEvictionListener);
  }

  /**
   * Detach the backing map from the store as the store is being closed.
   *
   * @param storeId Id of the store
   */
  void detachBackingMap(String storeId) {
    allStores.get(storeId).detach();
  }

  /**
   * Creates a generic concurrent restartable map, given key type and value type.
   * <p>
   *   Used for state repository maps.
   *
   * @param uniqueId Unique Id representing the map
   * @param keyClass Key type in the restartable map
   * @param valueClass value type in the restartable map
   * @param <K> Key type for the restartable map; must be serializable as of now
   * @param <V> Value type for the restartable map; must be serializable as of now
   * @return a created restartable map which is registered within the cache data store area
   */
  <K extends Serializable, V extends Serializable> RestartableLocalMap<K, V> createRestartableMap(String uniqueId,
                                                                                                  Class<K> keyClass,
                                                                                                  Class<V> valueClass) {

    RestartableLocalMap<K, V> mapToReturn = new RestartableLocalMap<>(keyClass, valueClass,
        metadataProvider.toStateRepositoryIdentifier(uniqueId), this.cacheDataStoreArea, true);
    cacheObjectManager.registerObject(mapToReturn);
    return mapToReturn;
  }

  private void initCacheStore() {
    File logDirectory = new File(frsContext.getDirectory(), frsId);
    boolean exists = logDirectory.mkdirs();
    LOGGER.info("{} store area for caches at location {}", exists ? "Loading" : "Creating", logDirectory);
    Properties properties = new Properties();
    try {
      cacheDataStoreArea = new ControlledTransactionRestartStore<>(
          RestartStoreFactory.createStore(cacheObjectManager, logDirectory, properties));
    } catch (IOException | RestartStoreException e) {
      throw new RuntimeException("Cannot initialize FRS store area for cache data", e);
    }
  }

  private void bootstrapAllExistingStores() {
    for (Map.Entry<String, SavedStoreConfig> store : metadataProvider.getExistingRestartableStores(frsId).entrySet()) {
      buildMapForRecovery(store.getKey(), store.getValue());
    }
    for (Map.Entry<String, SavedStateRepositoryConfig<?,?>> repo :
        metadataProvider.getExistingRestartableStateRepositories(frsId).entrySet()) {
      @SuppressWarnings("unchecked")
      SavedStateRepositoryConfig<? extends Serializable, ? extends Serializable> config =
              (SavedStateRepositoryConfig<? extends Serializable, ? extends Serializable>) repo.getValue();
      loadStateRepository(repo.getKey(), config);
    }
  }

  /**
   * Close existing stores and shutdown
   */
  private void closeAllExistingStores() {
    dataStoreLifecycle.shutStore();
    for (String storeId : metadataProvider.getExistingRestartableStores(frsId).keySet()) {
      WrapperEnclosure<?, ?> enclosure = allStores.remove(storeId);
      if (enclosure != null) {
        cacheObjectManager.unregisterStripe(metadataProvider.toStoreIdentifier(storeId));
        final EhcacheConcurrentOffHeapClockCache<?, ?> localMap = enclosure.getMap();
        if (localMap != null) {
          localMap.destroy();
        }
      }
    }
    for (String composedId : metadataProvider.getExistingRestartableStateRepositories(frsId).keySet()) {
      stateRepositoryLoader.close(composedId);
    }
    allStores.clear();
    // destroy all maps queued up for destroy
    for (Iterator<EhcacheConcurrentOffHeapClockCache<?, ?>> iterator = toDestroyMapList.iterator(); iterator.hasNext(); ) {
      final EhcacheConcurrentOffHeapClockCache<?, ?> map = iterator.next();
      iterator.remove();
      map.destroy();
    }
  }

  /**
   * Build the map early to recover other stores in anticipation of a future creation of the store.
   * Without re-building maps for all stores tied to a single FRS restart store, it is not possible
   * to start the FRS restart store.
   *
   * Use dynamic proxy wrappers to wrap objects passed to the map, so that the real object instances
   * can be attached when the store is created and real instances become available.
   *
   * Note: Due to custom class loaders that may be used by caches, type information is completely
   * omitted when building these maps for recovery. The type information is obtained and validated
   * when the corresponding store is created during creation of the corresponding cache.
   *
   * @param storeId Id/alias of the store
   * @param config Stored configuration from FRS.
   */
  @SuppressWarnings("unchecked")
  private void buildMapForRecovery(String storeId, SavedStoreConfig config) {
    LOGGER.info("Bootstrapping: Building map for recovery of ID = {}  Size = {}", storeId, config.getSizeInBytes());
    buildMapWithWrappers(storeId, config.getSizeInBytes());
  }

  @SuppressWarnings("unchecked")
  private <K, V> void buildMapWithWrappers(String storeId, long sizeInBytes) {
    WrapperFactory<Portability<K>> keyWrapperFactory = new WrapperFactory<>();
    WrapperFactory<SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>>> evictionAdvisorWrapperFactory =
            new WrapperFactory<>();
    WrapperFactory<EvictionListener<K, OffHeapValueHolder<V>>> evictionListenerWrapperFactory = new WrapperFactory<>();
    Portability<K> keyPortability = keyWrapperFactory.wrap((Class<Portability<K>>) (Class) Portability.class);
    WrapperSerializer<V> wrapperSerializer = new WrapperSerializer<>();
    Portability<OffHeapValueHolder<V>> valueSerializer = new OffHeapValueHolderPortability<>(wrapperSerializer);
    Portability<LinkedNode<OffHeapValueHolder<V>>> linkedPortability = new LinkedNodePortability<>(valueSerializer);
    SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor = evictionAdvisorWrapperFactory
            .wrap((Class<SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>>>) (Class) SwitchableEvictionAdvisor.class);
    EvictionListener<K, OffHeapValueHolder<V>> evictionListener = evictionListenerWrapperFactory
            .wrap((Class<EvictionListener<K, OffHeapValueHolder<V>>>) (Class) EvictionListener.class);
    EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> m =
            createBackingMap(storeId, sizeInBytes,
                    keyPortability, linkedPortability, evictionAdvisor, evictionListener);
    allStores.put(storeId, new WrapperEnclosure<>(keyWrapperFactory.getWrapper(),
            wrapperSerializer,
            evictionAdvisorWrapperFactory.getWrapper(),
            evictionListenerWrapperFactory.getWrapper(),
            m));
  }

  private <K1 extends Serializable, V1 extends Serializable> void loadStateRepository(String composedId,
                                                                                      SavedStateRepositoryConfig<K1, V1> repositoryConfig) {
    LOGGER.info("Bootstrapping: Recovering State Repository for ID = {}", composedId);
    RestartableLocalMap<K1, V1> stateMap = new RestartableLocalMap<>(
        repositoryConfig.getKeyType(), repositoryConfig.getValueType(),
        metadataProvider.toStateRepositoryIdentifier(composedId),
        cacheDataStoreArea, true);
    cacheObjectManager.registerObject(stateMap);
    stateRepositoryLoader.load(composedId, stateMap);
  }

  @SuppressWarnings("unchecked")
  private <K, V> EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> attachBackingMap(
      String storeId,
      Configuration<K, V> storeConfig,
      SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor,
      EvictionListener<K, OffHeapValueHolder<V>> mapEvictionListener) {
    WrapperEnclosure<K, V> enclosure = (WrapperEnclosure<K, V>) allStores.get(storeId);
    if (enclosure == null || enclosure.isAttached()) {
      throw new AssertionError("Store is already attached. This is unexpected");
    }
    Portability<K> keyPortability = new SerializerPortability<>(storeConfig.getKeySerializer());
    enclosure.keyPortability.changeWrapped(keyPortability);
    enclosure.valueSerializer.changeSerializer(storeConfig.getValueSerializer());
    enclosure.mapEvictionListener.changeWrapped(mapEvictionListener);
    enclosure.evictionAdvisor.changeWrapped(evictionAdvisor);
    enclosure.attach();
    return enclosure.getMap();
  }

  private <K, V> EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createBackingMap(
      String storeId,
      long sizeInBytes,
      Portability<K> keyPortability,
      Portability<LinkedNode<OffHeapValueHolder<V>>> valuePortability,
      SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor,
      EvictionListener<K, OffHeapValueHolder<V>> mapEvictionListener) {

    ByteBuffer identifier = metadataProvider.toStoreIdentifier(storeId);
    HeuristicConfiguration config = new HeuristicConfiguration(sizeInBytes);
    PageSource source = new UpfrontAllocatingPageSource(getBufferSource(),
        config.getMaximumSize(), config.getMaximumChunkSize(), config.getMinimumChunkSize());

    Factory<OffHeapBufferStorageEngine<K, LinkedNode<OffHeapValueHolder<V>>>> delegateFactory =
        OffHeapBufferStorageEngine.createFactory(PointerSize.INT, source,
            config.getSegmentDataPageSize(), keyPortability, valuePortability, false, true);

    Factory<? extends RestartableStorageEngine<OffHeapBufferStorageEngine<K, LinkedNode<OffHeapValueHolder<V>>>,
        ByteBuffer, K, OffHeapValueHolder<V>>> storageEngineFactory =
        RestartableStorageEngine.createFactory(identifier,
            cacheDataStoreArea, delegateFactory, true);

    Factory<? extends PinnableSegment<K, OffHeapValueHolder<V>>> segmentFactory =
        new EhcacheSegmentFactory<>(
            source,
            storageEngineFactory,
            config.getInitialSegmentTableSize(),
            evictionAdvisor,
            mapEvictionListener);

    EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> m =
        new EhcacheConcurrentOffHeapClockCache<>(
            evictionAdvisor, segmentFactory, config.getConcurrency());

    cacheObjectManager.registerStripe(identifier, new OffHeapObjectManagerStripe<>(identifier, m));
    return m;
  }

  /**
   * Per Store enclosure for wrapper classes used to store bootstrapped map and associated wrapper classes until
   * the store is attached.
   *
   * Note: Type information is not known until the FRS store corresponding to a given store
   * is attached to the store. Due to this, type information for key and value is completely omitted
   * for detached stores.
   */
  private static final class WrapperEnclosure<K, V> {
    private final WrapperProxy<Portability<K>> keyPortability;
    private final WrapperSerializer<V> valueSerializer;
    private final WrapperProxy<SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>>> evictionAdvisor;
    private final WrapperProxy<EvictionListener<K, OffHeapValueHolder<V>>> mapEvictionListener;
    private final EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;
    private boolean attached;

    private WrapperEnclosure(WrapperProxy<Portability<K>> keyPortability,
                             WrapperSerializer<V> wrapperSerializer,
                             WrapperProxy<SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>>> evictionAdvisor,
                             WrapperProxy<EvictionListener<K, OffHeapValueHolder<V>>> mapEvictionListener,
                             EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map) {
      this.keyPortability = keyPortability;
      this.valueSerializer = wrapperSerializer;
      this.evictionAdvisor = evictionAdvisor;
      this.mapEvictionListener = mapEvictionListener;
      this.map = map;
      this.attached = false;
    }

    private EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> getMap() {
      return map;
    }

    private boolean isAttached() {
      return attached;
    }

    private void attach() {
      attached = true;
    }

    private void detach() {
      attached = false;
    }
  }

  /**
   * Dynamic proxy to wrap objects passed to the map during recovery.
   * Assumed to be always under protection when {@code changeWrapped} is called.
   *
   * @param <T> type which is proxied
   */
  private static final class WrapperProxy<T> implements InvocationHandler {
    volatile T wrapped;

    private WrapperProxy() {
      wrapped = null;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (wrapped == null) {
        throw new AssertionError("This call was unexpected to be called while store is in recovery");
      }
      return method.invoke(wrapped, args);
    }

    void changeWrapped(T newWrapped) {
      wrapped = newWrapped;
    }
  }

  /**
   * Factory for Wrapper proxy of {@code T}. This factory creates the {@link WrapperProxy} and
   * gives an accessor method to the proxy.
   *
   * @param <T> the type which is proxied dynamically
   */
  private static class WrapperFactory<T> {
    final WrapperProxy<T> newProxy;

    WrapperFactory() {
      newProxy = new WrapperProxy<>();
    }

    @SuppressWarnings("unchecked")
    T wrap(Class<T> orig) {
      return (T)
              Proxy.newProxyInstance(orig.getClassLoader(),
                      new Class<?>[]{orig},
                      newProxy);
    }

    WrapperProxy<T> getWrapper() {
      return newProxy;
    }
  }
}
