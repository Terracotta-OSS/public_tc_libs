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
package com.terracottatech.ehcache.disk.config.builders;

import org.ehcache.config.units.MemoryUnit;

import com.terracottatech.ehcache.disk.config.FastRestartStoreResourcePool;
import com.terracottatech.ehcache.disk.internal.config.FastRestartStoreResourcePoolImpl;


/**
 * Builder capable of building enterprise ready disk resource pools.
 *
 * @author RKAV
 */
public final class EnterpriseDiskResourcePoolBuilder {

  /** Private, niladic constructor to prevent instantiation. */
  private EnterpriseDiskResourcePoolBuilder() {
  }

  /**
   * Creates a frs backed offHeap pool, given size and unit.
   * <p>
   *   Disk writes to this pool will always be synchronous.
   *
   * @param size       the size
   * @param unit       the unit for the size
   * @return a resource pool that can hold FRS backed offHeap resources.
   */
  public static FastRestartStoreResourcePool diskRestartable(long size, MemoryUnit unit) {
    return new FastRestartStoreResourcePoolImpl(size, unit);
  }
}