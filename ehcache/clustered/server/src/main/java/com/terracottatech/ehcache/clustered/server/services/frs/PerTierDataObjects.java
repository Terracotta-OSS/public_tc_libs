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

package com.terracottatech.ehcache.clustered.server.services.frs;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.clustered.server.offheap.frs.OffHeapChainMapStripe;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.server.offheap.OffHeapChainMap;
import org.ehcache.clustered.server.state.ResourcePageSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Data objects for each tier
 */
@CommonComponent
public interface PerTierDataObjects {
  OffHeapChainMapStripe<ByteBuffer, Long> getStripe();
  Map<String, RestartableGenericMap<Object, Object>> getStateRepositories();
  RestartableGenericMap<Long, Integer> getInvalidationTrackerMap();
  ServerStoreConfiguration getServerStoreConfiguration();
  ResourcePageSource getPageSource();
  List<OffHeapChainMap<Long>> getRecoveredMaps();
  boolean isAttached();
}
