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
package com.terracottatech.ehcache.clustered.client.config.builders;

import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.config.units.MemoryUnit;

import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredDedicatedResourcePoolImpl;
import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredSharedResourcePoolImpl;

/**
 * Constructs a {@link org.ehcache.config.ResourcePool ResourcePool} for a restartable clustered resource.
 */
public final class ClusteredRestartableResourcePoolBuilder {

  /** Private, niladic constructor to prevent instantiation. */
  private ClusteredRestartableResourcePoolBuilder() {
  }

  /**
   * Creates a new restartable clustered resource pool using dedicated clustered resources supporting synchronous
   * writes to restart log.
   *
   * @param size           the size
   * @param unit           the unit for the size
   */
  public static DedicatedClusteredResourcePool clusteredRestartableDedicated(long size,
                                                                             MemoryUnit unit) {
    return new EnterpriseClusteredDedicatedResourcePoolImpl(null, size, unit);
  }

  /**
   * Creates a new restartable clustered resource pool using dedicated clustered resources. The {@code dataPercent}
   * allows the user to override the percentage of the offHeap that will be reserved for user data.
   * <p>
   *   Applicable for hybrid with partial data caching.
   *
   * @param size           the size
   * @param unit           the unit for the size
   * @param dataPercent  controls what percentage of offHeap to use for user data cache
   */
  public static DedicatedClusteredResourcePool clusteredRestartableDedicated(long size,
                                                                             MemoryUnit unit,
                                                                             int dataPercent) {
    return new EnterpriseClusteredDedicatedResourcePoolImpl(null, size, unit, dataPercent);
  }

  /**
   * Creates a new restartable clustered resource pool with synchronous writes to restart log using dedicated
   * clustered resources, given a server based resource name.
   *
   * @param fromResource the name of the server-based resource from which this dedicated resource pool
  is reserved; may be {@code null}
   * @param size          the size
   * @param unit          the unit for the size
   */
  public static DedicatedClusteredResourcePool clusteredRestartableDedicated(String fromResource,
                                                                             long size, MemoryUnit unit) {
    return new EnterpriseClusteredDedicatedResourcePoolImpl(fromResource, size, unit);
  }

  /**
   * Creates a new restartable clustered resource pool with synchronous writes to restart log using
   * dedicated clustered resources, given a server based resource name.
   * <p>
   *   Available only for hybrid cache managers.
   *
   * @param fromResource the name of the server-based resource from which this dedicated resource pool
  is reserved; may be {@code null}
   * @param size          the size
   * @param unit          the unit for the size
   * @param dataPercent controls the percentage of offheap to be reserved for user data for partial data caching
   */
  public static DedicatedClusteredResourcePool clusteredRestartableDedicated(String fromResource,
                                                                             long size, MemoryUnit unit,
                                                                             int dataPercent) {
    return new EnterpriseClusteredDedicatedResourcePoolImpl(fromResource, size, unit, dataPercent);
  }

  /**
   * Creates a new restartable shared resource pool based on the provided parameters.
   *
   * @param sharedResource the non-{@code null} name of the server-based resource pool whose space is shared
   *                       by this pool
   */
  public static SharedClusteredResourcePool clusteredRestartableShared(String sharedResource) {
    return new EnterpriseClusteredSharedResourcePoolImpl(sharedResource);
  }
}