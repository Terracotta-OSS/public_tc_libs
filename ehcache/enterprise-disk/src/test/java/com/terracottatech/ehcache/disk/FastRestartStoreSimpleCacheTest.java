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
package com.terracottatech.ehcache.disk;

import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;

import com.terracottatech.ehcache.disk.config.builders.EnterpriseDiskResourcePoolBuilder;

/**
 * Single tier FRS tests.
 *
 * @author RKAV
 */
public class FastRestartStoreSimpleCacheTest extends AbstractFastRestartStoreTest {
  @Override
  ResourcePoolsBuilder buildTestResourcePool(int cacheIdx, ResourcePoolsBuilder resourcePoolsBuilder) {
    return resourcePoolsBuilder.with(EnterpriseDiskResourcePoolBuilder.diskRestartable(cacheIdx, MemoryUnit.MB));
  }
}