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

package com.terracottatech.ehcache.clustered.server.repo;

import com.terracottatech.ehcache.clustered.server.frs.ClusterTierRestartManager;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class RestartableServerStateRepository extends AbstractStateRepository {
  private final ConcurrentMap<String, RestartableGenericMap<Object, Object>> concurrentMapRepo;
  private final String tierId;
  private final ClusterTierRestartManager restartManager;

  public RestartableServerStateRepository(String tierId, ClusterTierRestartManager restartManager) {
    super();
    this.concurrentMapRepo = new ConcurrentHashMap<>();
    this.tierId = tierId;
    this.restartManager = restartManager;
  }

  public RestartableServerStateRepository(String tierId, ClusterTierRestartManager restartManager,
                                          Map<String, RestartableGenericMap<Object, Object>> stateRepositories) {
    this(tierId, restartManager);
    this.concurrentMapRepo.putAll(stateRepositories);
  }

  @Override
  public ConcurrentMap<Object, Object> getOrCreateRepositoryMap(String mapId) {
    ConcurrentMap<Object, Object> map = concurrentMapRepo.get(mapId);
    if (map == null) {
      RestartableGenericMap<Object, Object> newMap = restartManager.createStateRepositoryMap(tierId, mapId);
      map = concurrentMapRepo.putIfAbsent(mapId, newMap);
      if (map == null) {
        map = newMap;
      }
    }
    return map;
  }
}