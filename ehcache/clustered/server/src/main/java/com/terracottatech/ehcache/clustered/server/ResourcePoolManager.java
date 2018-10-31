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

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.ehcache.clustered.server.services.pool.ResourcePoolManagerServiceProvider;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.TreeNode;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.ValueStatistic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.terracotta.offheapresource.OffHeapResourceIdentifier.identifier;
import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;
import static org.terracotta.statistics.StatisticsManager.tags;
import static org.terracotta.statistics.ValueStatistics.gauge;

/**
 * Note: This code is extracted from {@link EnterpriseEhcacheStateServiceImpl}
 *
 * @author vmad
 */
@CommonComponent
public class ResourcePoolManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourcePoolManager.class);
  private static final long MAX_PAGE_SIZE = BYTES.convert(8, MEGABYTES);

  public enum ResourcePoolMode {NORMAL, RESTARTABLE_FULL, RESTARTABLE_PARTIAL}

  private static final String STATISTICS_POOL_TAG = "Pool";
  private static final String PROPERTY_POOL_KEY = "poolName";
  private static final String POOL_NAME_CACHE_SUFFIX = "-cache";

  private static final Map<String, Function<ResourcePageSource, ValueStatistic<Number>>> STAT_POOL_METHOD_REFERENCES = new HashMap<>();

  static {
    STAT_POOL_METHOD_REFERENCES.put("allocatedSize", (pageSource) -> gauge(pageSource::getAllocatedSize));
  }

  private final OffHeapResources offHeapResources;
  private final ResourcePoolManagerServiceProvider.DestroyCallback serviceProviderCleanupCallback;

  /**
   * The name of the resource to use for dedicated resource pools not identifying a resource from which
   * space for the pool is obtained.  This value may be {@code null};
   */
  private volatile String defaultServerResource;
  private volatile ResourcePoolMode resourcePoolMode = ResourcePoolMode.NORMAL;

  /**
   * The clustered shared resource pools specified by the CacheManager creating this {@code EhcacheActiveEntity}.
   * The index is the name assigned to the shared resource pool in the cache manager configuration.
   */
  private final Map<String, ResourcePageSource> sharedResourcePools = new ConcurrentHashMap<>();

  /**
   * The clustered dedicated resource pools specified by caches defined in CacheManagers using this
   * {@code EhcacheActiveEntity}.  The index is the cache identifier (alias).
   */
  private final Map<String, ResourcePageSource> dedicatedResourcePools = new ConcurrentHashMap<>();
  private final Map<String, ResourcePageSource> dedicatedCachingPools = new ConcurrentHashMap<>();

  public ResourcePoolManager(OffHeapResources offHeapResources, ResourcePoolManagerServiceProvider.DestroyCallback serviceProviderCleanupCallback) {
    this.offHeapResources = offHeapResources;
    this.serviceProviderCleanupCallback = serviceProviderCleanupCallback;
  }

  public static long getMaxSize(long poolSize) {
    return Long.min(Long.highestOneBit(poolSize) >>> 5, MAX_PAGE_SIZE);
  }

  public Set<String> getSharedResourcePoolIds() {
    return Collections.unmodifiableSet(sharedResourcePools.keySet());
  }

  public Set<String> getDedicatedResourcePoolIds() {
    return Collections.unmodifiableSet(dedicatedResourcePools.keySet());
  }

  public ResourcePoolMode getResourcePoolMode() {
    return resourcePoolMode;
  }

  public String getDefaultServerResource() {
    return this.defaultServerResource;
  }

  public Map<String, ServerSideConfiguration.Pool> getSharedResourcePools() {
    return sharedResourcePools.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue().getPool()));
  }

  public ResourcePageSource getSharedResourcePageSource(String name) {
    return sharedResourcePools.get(name);
  }

  public ServerSideConfiguration.Pool getDedicatedResourcePool(String name) {
    ResourcePageSource resourcePageSource = dedicatedResourcePools.get(name);
    return resourcePageSource == null ? null : resourcePageSource.getPool();
  }

  public ResourcePageSource getDedicatedResourcePageSource(String name) {
    return dedicatedResourcePools.get(name);
  }

  public void checkConfigurationCompatibility(ServerSideConfiguration incomingConfig) throws InvalidServerSideConfigurationException {
    if (!nullSafeEquals(this.defaultServerResource, incomingConfig.getDefaultServerResource())) {
      throw new InvalidServerSideConfigurationException("Default resource not aligned. "
                                                        + "Client: " + incomingConfig.getDefaultServerResource() + " "
                                                        + "Server: " + defaultServerResource);
    } else if (!sharedResourcePools.keySet().equals(incomingConfig.getResourcePools().keySet())) {
      throw new InvalidServerSideConfigurationException("Pool names not equal. "
                                                        + "Client: " + incomingConfig.getResourcePools().keySet() + " "
                                                        + "Server: " + sharedResourcePools.keySet().toString());
    }

    try {
      for (Map.Entry<String, ServerSideConfiguration.Pool> pool : resolveResourcePools(incomingConfig).entrySet()) {
        ServerSideConfiguration.Pool serverPool = this.sharedResourcePools.get(pool.getKey()).getPool();

        if (!serverPool.equals(pool.getValue())) {
          throw new InvalidServerSideConfigurationException("Pool '" + pool.getKey() + "' not equal. "
                                                            + "Client: " + pool.getValue() + " "
                                                            + "Server: " + serverPool);
        }
      }
    } catch (ConfigurationException e) {
      throw new InvalidServerSideConfigurationException(e.getMessage());
    }
  }

  private static Map<String, ServerSideConfiguration.Pool> resolveResourcePools(ServerSideConfiguration configuration) throws ConfigurationException {
    Map<String, ServerSideConfiguration.Pool> pools = new HashMap<>();
    for (Map.Entry<String, ServerSideConfiguration.Pool> e : configuration.getResourcePools().entrySet()) {
      ServerSideConfiguration.Pool pool = e.getValue();
      if (pool.getServerResource() == null) {
        if (configuration.getDefaultServerResource() == null) {
          throw new ConfigurationException("Pool '" + e.getKey() + "' has no defined server resource, and no default value was available");
        } else {
          pools.put(e.getKey(), new ServerSideConfiguration.Pool(pool.getSize(), configuration.getDefaultServerResource()));
        }
      } else {
        pools.put(e.getKey(), pool);
      }
    }
    return Collections.unmodifiableMap(pools);
  }

  public void configure(ServerSideConfiguration configuration) throws ConfigurationException {
    this.defaultServerResource = configuration.getDefaultServerResource();
    if (configuration instanceof EnterpriseServerSideConfiguration) {
      this.resourcePoolMode = ((EnterpriseServerSideConfiguration)configuration).getRestartConfiguration()
          .getOffHeapMode().equals(RestartableOffHeapMode.FULL) ? ResourcePoolMode.RESTARTABLE_FULL :
          ResourcePoolMode.RESTARTABLE_PARTIAL;
    } else {
      this.resourcePoolMode = ResourcePoolMode.NORMAL;
    }
    if (this.defaultServerResource != null) {
      if (!offHeapResources.getAllIdentifiers().contains(identifier(this.defaultServerResource))) {
        throw new ConfigurationException("Default server resource '" + this.defaultServerResource
                                         + "' is not defined. Available resources are: " + offHeapResources.getAllIdentifiers());
      }
    }

    this.sharedResourcePools.putAll(createPools(resolveResourcePools(configuration)));
  }

  private Map<String, ResourcePageSource> createPools(Map<String, ServerSideConfiguration.Pool> resourcePools) throws ConfigurationException {
    Map<String, ResourcePageSource> pools = new HashMap<>();
    try {
      for (Map.Entry<String, ServerSideConfiguration.Pool> e : resourcePools.entrySet()) {
        pools.put(e.getKey(), createPageSource(e.getKey(), e.getValue()));
      }
    } catch (ConfigurationException | RuntimeException e) {
      /*
       * If we fail during pool creation, back out any pools successfully created during this call.
       */
      if (!pools.isEmpty()) {
        LOGGER.warn("Failed to create shared resource pools; reversing reservations", e);
        releasePools("shared", pools);
      }
      throw e;
    }
    return pools;
  }

  private ResourcePageSource createPageSource(String poolName, ServerSideConfiguration.Pool pool) throws ConfigurationException {
    ResourcePageSource pageSource;
    OffHeapResource source = offHeapResources.getOffHeapResource(identifier(pool.getServerResource()));
    if (source == null) {
      throw new ConfigurationException("Non-existent server side resource '" + pool.getServerResource() +
                                               "'. Available resources are: " + offHeapResources.getAllIdentifiers());
    } else if (source.reserve(pool.getSize())) {
      try {
        pageSource = new ResourcePageSource(pool);
        registerPoolStatistics(poolName, pageSource);
      } catch (RuntimeException t) {
        source.release(pool.getSize());
        throw new ConfigurationException("Failure allocating pool " + pool, t);
      }
      LOGGER.info("Reserved {} bytes from resource '{}' for pool '{}'", pool.getSize(), pool.getServerResource(), poolName);
    } else {
      throw new ConfigurationException("Insufficient defined resources to allocate pool " + poolName + "=" + pool);
    }
    return pageSource;
  }

  private void registerPoolStatistics(String poolName, ResourcePageSource pageSource) {
    STAT_POOL_METHOD_REFERENCES.forEach((name, statFactory) -> registerStatistic(pageSource, poolName, name, STATISTICS_POOL_TAG, PROPERTY_POOL_KEY, statFactory.apply(pageSource)));
  }

  private void unRegisterPoolStatistics(ResourcePageSource pageSource) {
    TreeNode node = StatisticsManager.nodeFor(pageSource);

    if(node != null) {
      node.clean();
    }
  }

  private void registerStatistic(Object context, String name, String observerName, String tag, String propertyKey, ValueStatistic<Number> valueStatistic) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("discriminator", tag);
    properties.put(propertyKey, name);

    StatisticsManager.createPassThroughStatistic(context, observerName, tags(tag, "tier"), properties, valueStatistic);
  }

  public void releaseDedicatedPool(String name, PageSource pageSource) {
    /*
       * A ServerStore using a dedicated resource pool is the only referent to that pool.  When such a
       * ServerStore is destroyed, the associated dedicated resource pool must also be discarded.
       */
    ResourcePageSource expectedPageSource = dedicatedResourcePools.get(name);
    if (expectedPageSource != null) {
      if (pageSource == expectedPageSource) {
        dedicatedResourcePools.remove(name);
        releasePool("dedicated", name, expectedPageSource);
        if (resourcePoolMode.equals(ResourcePoolMode.RESTARTABLE_PARTIAL)) {
          ResourcePageSource cachingPageSource = dedicatedCachingPools.remove(name);
          if (cachingPageSource != null) {
            releasePool("dedicated-cache", name + POOL_NAME_CACHE_SUFFIX, cachingPageSource);
          }
        }
      } else {
        LOGGER.error("Client {} attempting to destroy clustered tier '{}' with unmatched page source", name);
      }
    }
  }

  public void destroy() {
    this.defaultServerResource = null;
    /*
     * Remove the reservation for resource pool memory of resource pools.
     */
    releasePools("shared", this.sharedResourcePools);
    releasePools("dedicated", this.dedicatedResourcePools);
    releasePools("dedicated-cache", this.dedicatedCachingPools);

    this.sharedResourcePools.clear();
    this.serviceProviderCleanupCallback.destroy();
  }

  private void releasePools(String poolType, Map<String, ResourcePageSource> resourcePools) {
    if (resourcePools == null) {
      return;
    }
    final Iterator<Map.Entry<String, ResourcePageSource>> dedicatedPoolIterator = resourcePools.entrySet().iterator();
    while (dedicatedPoolIterator.hasNext()) {
      Map.Entry<String, ResourcePageSource> poolEntry = dedicatedPoolIterator.next();
      releasePool(poolType, poolEntry.getKey(), poolEntry.getValue());
      dedicatedPoolIterator.remove();
    }
  }

  private void releasePool(String poolType, String poolName, ResourcePageSource resourcePageSource) {
    ServerSideConfiguration.Pool pool = resourcePageSource.getPool();
    OffHeapResource source = offHeapResources.getOffHeapResource(identifier(pool.getServerResource()));
    if (source != null) {
      unRegisterPoolStatistics(resourcePageSource);
      source.release(pool.getSize());
      LOGGER.info("Released {} bytes from resource '{}' for {} pool '{}'", pool.getSize(), pool.getServerResource(), poolType, poolName);
    }
  }

  public ResourcePageSource getPageSource(String name, PoolAllocation allocation) throws ConfigurationException {

    ResourcePageSource resourcePageSource;

    if (allocation instanceof PoolAllocation.Dedicated || allocation instanceof EnterprisePoolAllocation.DedicatedRestartable) {
      /*
       * Dedicated allocation pools are taken directly from a specified resource, not a shared pool, and
       * identified by the cache identifier/name.
       */
      if (dedicatedResourcePools.containsKey(name)) {
        throw new ConfigurationException("Fixed resource pool for clustered tier '" + name + "' already exists");
      } else {
        DedicatedAllocation dedicatedAllocation = new DedicatedAllocation(allocation);
        if (resourcePoolMode.equals(ResourcePoolMode.RESTARTABLE_PARTIAL) &&
            dedicatedAllocation.isRestartable() && dedicatedAllocation.getDataPercent() > 99) {
          throw new ConfigurationException("Percentage of pool space for user data must be within range (between 0 and 99, inclusive)");
        }
        if (resourcePoolMode.equals(ResourcePoolMode.RESTARTABLE_FULL)
            && dedicatedAllocation.isRestartable() && dedicatedAllocation.isDataPercentSpecified()) {
          throw new ConfigurationException("dataPercent attribute has no meaning for a clustered tier manager with " +
                                           "FULL restartable offheap mode");
        }

        String resourceName = dedicatedAllocation.getResourceName();
        if (resourceName == null) {
          if (defaultServerResource == null) {
            throw new ConfigurationException("Fixed pool for clustered tier '" + name + "' not defined; default server resource not configured");
          } else {
            resourceName = defaultServerResource;
          }
        }
        long poolSize = dedicatedAllocation.getSize();
        if (dedicatedAllocation.getDataPercent() > 0 && resourcePoolMode.equals(ResourcePoolMode.RESTARTABLE_PARTIAL)) {
          poolSize = (poolSize * (100 - dedicatedAllocation.getDataPercent())) / 100;
        }
        resourcePageSource = createPageSource(name, new ServerSideConfiguration.Pool(poolSize, resourceName));
        dedicatedResourcePools.put(name, resourcePageSource);
      }
    } else if (allocation instanceof PoolAllocation.Shared || allocation instanceof EnterprisePoolAllocation.SharedRestartable) {
      /*
       * Shared allocation pools are created during EhcacheActiveEntity configuration.
       */
      SharedAllocation sharedAllocation = new SharedAllocation(allocation);
      if (resourcePoolMode.equals(ResourcePoolMode.RESTARTABLE_PARTIAL) && sharedAllocation.isRestartable()) {
        throw new ConfigurationException("Shared pools cannot be restartable in hybrid mode");
      }
      resourcePageSource = sharedResourcePools.get(sharedAllocation.getResourcePoolName());
      if (resourcePageSource == null) {
        throw new ConfigurationException("Shared pool named '" + sharedAllocation.getResourcePoolName() + "' undefined.");
      }

    } else {
      throw new ConfigurationException("Unexpected PoolAllocation type: " + allocation.getClass().getName());
    }
    return resourcePageSource;

  }

  public PageSource getCachingPageSource(String name, PoolAllocation allocation) throws ConfigurationException {
    ResourcePageSource resourcePageSource = null;
    if (allocation instanceof EnterprisePoolAllocation.DedicatedRestartable) {
      DedicatedAllocation dedicatedAllocation = new DedicatedAllocation(allocation);
      if (dedicatedAllocation.getDataPercent() <= 0) {
        return null;
      }
      String resourceName = dedicatedAllocation.getResourceName();
      long poolSize = (dedicatedAllocation.getDataPercent() * dedicatedAllocation.getSize()) / 100;
      if (resourceName == null) {
        resourceName = defaultServerResource;
      }
      resourcePageSource = createPageSource(name + POOL_NAME_CACHE_SUFFIX, new ServerSideConfiguration.Pool(poolSize, resourceName));
      dedicatedCachingPools.put(name, resourcePageSource);
    }
    return resourcePageSource;
  }

  private static boolean nullSafeEquals(Object s1, Object s2) {
    return (s1 == null ? s2 == null : s1.equals(s2));
  }

}
