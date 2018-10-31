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
package com.terracottatech.ehcache.clustered.server.state;

import com.terracottatech.ehcache.clustered.common.RestartConfiguration;

import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.EhcacheStateServiceDump;
import org.terracotta.entity.StateDumpCollector;

class EnterpriseEhcacheStateServiceDump {
  static void dump(EhcacheStateService ehcacheStateService, StateDumpCollector clusterTierManagerStateDump) {

    EhcacheStateServiceDump.dump(ehcacheStateService, clusterTierManagerStateDump);

    if(ehcacheStateService instanceof EnterpriseEhcacheStateService) {
      EnterpriseEhcacheStateService enterpriseEhcacheStateService = (EnterpriseEhcacheStateService) ehcacheStateService;
      RestartConfiguration restartConfiguration = enterpriseEhcacheStateService.getRestartConfiguration();
      if(restartConfiguration != null) {
        String restartableStoreId = String.join("#",
            restartConfiguration.getRestartableLogRoot(),
            restartConfiguration.getRestartableLogContainer(),
            restartConfiguration.getRestartableLogName());
        StateDumpCollector restartStoreDump = clusterTierManagerStateDump.subStateDumpCollector("restartStore");
        restartStoreDump.addState("id", restartableStoreId); // matches 'alias' in exposed objects "EhcacheRestartStore" in provider "EhcachePersistenceSettings"
        restartStoreDump.addState("root", restartConfiguration.getRestartableLogRoot()); // matches dataRootId config
        restartStoreDump.addState("container", restartConfiguration.getRestartableLogContainer());
        restartStoreDump.addState("name", restartConfiguration.getRestartableLogName());
        restartStoreDump.addState("offheapMode", restartConfiguration.getOffHeapMode().name());
      }
    }
  }
}