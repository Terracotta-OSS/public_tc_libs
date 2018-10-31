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
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.server.repo.StateRepositoryManager;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages {@link RestartableServerStateRepository} and {@link ServerStateRepository}.
 * <p>
 *   Completely overrides OSS {@link StateRepositoryManager}
 *
 * @author RKAV
 */
@CommonComponent
public class EnterpriseStateRepositoryManager extends StateRepositoryManager {
  private final ClusterTierRestartManager restartManager;
  private final ConcurrentMap<String, AbstractStateRepository> mapRepositoryMap;

  public EnterpriseStateRepositoryManager(ClusterTierRestartManager restartManager) {
    this.restartManager = restartManager;
    this.mapRepositoryMap = new ConcurrentHashMap<>();
  }

  @Override
  public EhcacheEntityResponse invoke(StateRepositoryOpMessage message) {
    final String tierId = message.getCacheId();
    AbstractStateRepository currentRepo = this.mapRepositoryMap.get(tierId);
    if (currentRepo == null) {
      AbstractStateRepository newRepo = restartManager.isRestartable(tierId) ?
          new RestartableServerStateRepository(tierId, restartManager) : new ServerStateRepository();
      currentRepo = this.mapRepositoryMap.putIfAbsent(tierId, newRepo);
      if (currentRepo == null) {
        currentRepo = newRepo;
      }
    }
    return currentRepo.invoke(message);
  }

  /**
   * Restores all state repository maps from FRS during configure time.
   * <p>
   *   Only state repositories of restartable tiers are loaded.
   *
   * @param tierId The clustered tier ID
   * @param stateRepositories map of state repositories for this tier
   */
  public void loadMaps(String tierId, Map<String, RestartableGenericMap<Object, Object>> stateRepositories) {
    // currently the map should be empty
    this.mapRepositoryMap.put(tierId, new RestartableServerStateRepository(tierId, restartManager, stateRepositories));
  }
}
