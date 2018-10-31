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
package com.terracottatech.ehcache.disk.internal.frsbacked.store;

import org.ehcache.CachePersistenceException;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.impl.internal.events.ThreadLocalStoreEventDispatcher;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.impl.internal.store.offheap.EhcacheConcurrentOffHeapClockCache;
import org.ehcache.impl.internal.store.offheap.EhcacheOffHeapBackingMap;
import org.ehcache.impl.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.impl.internal.store.offheap.SwitchableEvictionAdvisor;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import com.terracottatech.ehcache.disk.config.EnterpriseDiskResourceType;
import com.terracottatech.ehcache.disk.config.FastRestartStoreResourcePool;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerManager;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerService;
import com.terracottatech.ehcache.disk.service.FastRestartStoreContainerService.FastRestartStoreSpaceIdentifier;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

/**
 * FRS backed offHeap store implementation.
 *
 * @author RKAV
 */
final class RestartableOffHeapStore<K, V> extends AbstractOffHeapStore<K, V> implements AuthoritativeTier<K, V> {
  private final long sizeInBytes;

  private final String storeId;
  private final SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor;

  private volatile EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;

  private RestartableOffHeapStore(final Configuration<K, V> config, TimeSource timeSource,
                                  final StoreEventDispatcher<K, V> eventDispatcher,
                                  final long sizeInBytes,
                                  final String storeId) {
    super(config, timeSource, eventDispatcher);
    EvictionAdvisor<? super K, ? super V> evictionAdvisor = config.getEvictionAdvisor();
    if (evictionAdvisor != null) {
      this.evictionAdvisor = wrap(evictionAdvisor);
    } else {
      this.evictionAdvisor = wrap(Eviction.noAdvice());
    }
    this.sizeInBytes = sizeInBytes;
    this.storeId = storeId;
  }

  @Override
  protected String getStatisticsTag() {
    return "RestartStore";
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

  @Override
  protected EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> backingMap() {
    return map;
  }

  @Override
  protected SwitchableEvictionAdvisor<K, OffHeapValueHolder<V>> evictionAdvisor() {
    return evictionAdvisor;
  }

  private EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createBackingMap(
      BackingMapConfig<K, V> backingMapConfig) {
    return backingMapConfig.frsMgr.createOrAttachBackingMap(this.sizeInBytes, this.storeId,
        backingMapConfig.storeConfig, this.evictionAdvisor, this.mapEvictionListener);
  }

  @ServiceDependencies({TimeSourceService.class, SerializationProvider.class, FastRestartStoreContainerService.class})
  static class Provider implements Store.Provider, AuthoritativeTier.Provider {

    private volatile ServiceProvider<Service> serviceProvider;
    private volatile FastRestartStoreContainerService frsService;
    private final Map<Store<?, ?>, BackingMapConfig<?, ?>> createdStores =
        new ConcurrentWeakIdentityHashMap<>();

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      return resourceTypes.equals(Collections.singleton(EnterpriseDiskResourceType.Types.FRS)) ? 1 : 0;
    }

    @Override
    public int rankAuthority(ResourceType<?> authorityResource, Collection<ServiceConfiguration<?>> serviceConfigs) {
      return authorityResource.equals(EnterpriseDiskResourceType.Types.FRS) ? 1 : 0;
    }

    @Override
    public <K, V> RestartableOffHeapStore<K, V> createStore(Configuration<K, V> storeConfig,
                                                            ServiceConfiguration<?>... serviceConfigs) {
      return createStoreInternal(storeConfig,
          new ThreadLocalStoreEventDispatcher<>(storeConfig.getDispatcherConcurrency()), serviceConfigs);
    }

    private <K, V> RestartableOffHeapStore<K, V> createStoreInternal(Configuration<K, V> storeConfig,
                                                                     StoreEventDispatcher<K, V> eventDispatcher,
                                                                     ServiceConfiguration<?>... serviceConfigs) {
      if (serviceProvider == null) {
        throw new NullPointerException("ServiceProvider is null in OffHeapStore.Provider.");
      }
      final TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

      final FastRestartStoreSpaceIdentifier frsId = findSingletonAmongst(FastRestartStoreSpaceIdentifier.class, (Object[]) serviceConfigs);
      if (frsId == null) {
        throw new IllegalStateException("No local persistence service found - did you configure it at the CacheManager level");
      }
      final FastRestartStoreResourcePool frsPool = storeConfig.getResourcePools().getPoolForResource(EnterpriseDiskResourceType.Types.FRS);
      final MemoryUnit unit = frsPool.getUnit();
      final RestartableOffHeapStore<K, V> offHeapStore = new RestartableOffHeapStore<>(storeConfig, timeSource,
          eventDispatcher, unit.toBytes(frsPool.getSize()), frsId.getId());
      final BackingMapConfig<K, V> backingMapConfig = new BackingMapConfig<>(storeConfig,
          frsService.getRestartableStoreManager(frsId), frsId);
      createdStores.put(offHeapStore, backingMapConfig);
      return offHeapStore;
    }

    @Override
    @SuppressWarnings({"rawtype", "unchecked"})
    public void releaseStore(Store<?, ?> resource) {
      final BackingMapConfig<?, ?> config = createdStores.remove(resource);
      if (config == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      close((RestartableOffHeapStore)resource, config);
    }

    private static <K, V> void close(final RestartableOffHeapStore<K, V> resource, final BackingMapConfig<K, V> config) {
      final EhcacheConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> localMap = resource.map;
      if (localMap != null) {
        // cannot destroy the map as this restartable object may be opened again.
        resource.map = null;
        config.frsMgr.detachBackingMap(resource.storeId);
      }
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void initStore(Store<?, ?> resource) {
      final BackingMapConfig<?,?> config = createdStores.get(resource);
      if (config == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      Serializer<?> keySerializer = config.storeConfig.getKeySerializer();
      if (keySerializer instanceof StatefulSerializer) {
        StateRepository stateRepository;
        try {
          stateRepository = frsService.getStateRepositoryWithin(config.frsId, "key-serializer");
        } catch (CachePersistenceException e) {
          throw new RuntimeException(e);
        }
        ((StatefulSerializer)keySerializer).init(stateRepository);
      }
      Serializer<?> valueSerializer = config.storeConfig.getValueSerializer();
      if (valueSerializer instanceof StatefulSerializer) {
        StateRepository stateRepository;
        try {
          stateRepository = frsService.getStateRepositoryWithin(config.frsId, "value-serializer");
        } catch (CachePersistenceException e) {
          throw new RuntimeException(e);
        }
        ((StatefulSerializer)valueSerializer).init(stateRepository);
      }
      init((RestartableOffHeapStore)resource, config);
    }

    private static <K, V> void init(final RestartableOffHeapStore<K, V> resource,
                                    final BackingMapConfig<K, V> backingMapConfig) {
      resource.map = resource.createBackingMap(backingMapConfig);
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
      this.frsService = serviceProvider.getService(FastRestartStoreContainerService.class);
      if (this.frsService == null) {
        throw new IllegalStateException("Unable to find fast restart store (frs) service");
      }
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      createdStores.clear();
      frsService = null;
    }

    @Override
    public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStore(storeConfig, serviceConfigs);
    }

    @Override
    public void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      releaseStore(resource);
    }

    @Override
    public void initAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      initStore(resource);
    }
  }

  /**
   * Config required to create a backing map
   *
   * @param <K> key type for the store
   * @param <V> value type for the store
   */
  private static final class BackingMapConfig<K, V> {
    private final Configuration<K, V> storeConfig;
    private final FastRestartStoreContainerManager frsMgr;
    private final FastRestartStoreSpaceIdentifier frsId;

    private BackingMapConfig(Configuration<K, V> storeConfig,
                             FastRestartStoreContainerManager frsMgr,
                             FastRestartStoreSpaceIdentifier frsId) {
      this.storeConfig = storeConfig;
      this.frsMgr = frsMgr;
      this.frsId = frsId;
    }
  }
}
