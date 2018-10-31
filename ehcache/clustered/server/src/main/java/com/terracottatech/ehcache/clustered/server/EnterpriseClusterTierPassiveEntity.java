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

import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.store.ClusterTierPassiveEntity;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceRegistry;

import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateService;

public class EnterpriseClusterTierPassiveEntity extends ClusterTierPassiveEntity {

  public EnterpriseClusterTierPassiveEntity(ServiceRegistry registry, ClusterTierEntityConfiguration config,
                                            KeySegmentMapper defaultMapper) throws ConfigurationException {
    super(registry, config, defaultMapper);
  }

  @Override
  protected EnterpriseEhcacheStateService getStateService() {
    return (EnterpriseEhcacheStateService) super.getStateService();
  }

  @Override
  public void startSyncEntity() {
    super.startSyncEntity();
    getStateService().beginGlobalTransaction(getStoreIdentifier());
  }

  @Override
  public void endSyncEntity() {
    getStateService().endGlobalTransaction(getStoreIdentifier());
    super.endSyncEntity();
  }
}
