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
package com.terracottatech.ehcache.disk.internal.config;

import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.SizedResourcePoolImpl;

import com.terracottatech.ehcache.disk.config.EnterpriseDiskResourceType;
import com.terracottatech.ehcache.disk.config.FastRestartStoreResourcePool;

/**
 * Default implementation for FRS backed offHeap resource pools.
 *
 * @author RKAV
 */
public class FastRestartStoreResourcePoolImpl extends SizedResourcePoolImpl<FastRestartStoreResourcePool>
    implements FastRestartStoreResourcePool {

  public FastRestartStoreResourcePoolImpl(final long size, final MemoryUnit unit) {
    super(EnterpriseDiskResourceType.Types.FRS, size, unit, true);
  }

  @Override
  public EnterpriseDiskResourceType<FastRestartStoreResourcePool> getType() {
    return (EnterpriseDiskResourceType<FastRestartStoreResourcePool>)super.getType();
  }

  @Override
  public MemoryUnit getUnit() {
    return (MemoryUnit)super.getUnit();
  }

}