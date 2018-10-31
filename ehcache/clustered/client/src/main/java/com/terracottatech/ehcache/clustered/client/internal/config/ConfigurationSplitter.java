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
package com.terracottatech.ehcache.clustered.client.internal.config;

import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * ConfigurationSplitter
 */
public class ConfigurationSplitter {

  public ServerSideConfiguration splitServerSideConfiguration(ServerSideConfiguration config, int totalStripes) {
    if (config == null) {
      return null;
    }
    Map<String, ServerSideConfiguration.Pool> resourcePools = splitPools(config, totalStripes);

    ServerSideConfiguration resultingConfiguration;
    String defaultServerResource = config.getDefaultServerResource();
    if (config instanceof EnterpriseServerSideConfiguration) {
      EnterpriseServerSideConfiguration eeConfig = ((EnterpriseServerSideConfiguration) config);
      if (defaultServerResource == null) {
        resultingConfiguration = new EnterpriseServerSideConfiguration(resourcePools, eeConfig.getRestartConfiguration());
      } else {
        resultingConfiguration = new EnterpriseServerSideConfiguration(defaultServerResource, resourcePools, eeConfig.getRestartConfiguration());
      }
    } else {
      if (defaultServerResource == null) {
        resultingConfiguration = new ServerSideConfiguration(resourcePools);
      } else {
        resultingConfiguration = new ServerSideConfiguration(defaultServerResource, resourcePools);
      }
    }
    return resultingConfiguration;
  }

  private Map<String, ServerSideConfiguration.Pool> splitPools(ServerSideConfiguration config, int totalStripes) {
    Map<String, ServerSideConfiguration.Pool> resourcePools = new HashMap<>();
    for (Map.Entry<String, ServerSideConfiguration.Pool> entry : config.getResourcePools()
        .entrySet()) {
      resourcePools.put(entry.getKey(), new ServerSideConfiguration.Pool(entry.getValue().getSize() / totalStripes, entry.getValue().getServerResource()));
    }
    return resourcePools;
  }

  public ServerStoreConfiguration splitServerStoreConfiguration(ServerStoreConfiguration config, int totalStripes) {
    PoolAllocation poolAllocation = config.getPoolAllocation();
    if (poolAllocation instanceof PoolAllocation.Dedicated) {
      PoolAllocation.Dedicated dedicated = (PoolAllocation.Dedicated) poolAllocation;
      poolAllocation = new PoolAllocation.Dedicated(dedicated.getResourceName(), dedicated.getSize() / totalStripes);
    } else if (poolAllocation instanceof EnterprisePoolAllocation.DedicatedRestartable) {
      EnterprisePoolAllocation.DedicatedRestartable dedicatedRestartable = (EnterprisePoolAllocation.DedicatedRestartable) poolAllocation;
      poolAllocation = new EnterprisePoolAllocation.DedicatedRestartable(dedicatedRestartable.getResourceName(),
          dedicatedRestartable.getSize() / totalStripes, dedicatedRestartable.getDataPercent());
    }
    ServerStoreConfiguration resultingConfiguration = new ServerStoreConfiguration(poolAllocation,
        config.getStoredKeyType(), config.getStoredValueType(),
        config.getKeySerializerType(), config.getValueSerializerType(), config.getConsistency());
    return resultingConfiguration;
  }
}
