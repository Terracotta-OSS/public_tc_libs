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
package com.terracottatech.ehcache.clustered.server.frs;

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.state.EhcacheStateContext;
import org.terracotta.entity.ConfigurationException;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.server.services.frs.PerTierDataObjects;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;

import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * TransientClusterTierManager
 */
@CommonComponent
public class TransientClusterTierManager implements ClusterTierRestartManager {

  private volatile boolean destroyInProgress = false;

  @Override
  public void configure(ServerSideConfiguration configuration) throws ConfigurationException {
    // no-op
  }

  @Override
  public void restart(KeySegmentMapper mapper) {
    // no-op
  }

  @Override
  public Map<String, PerTierDataObjects> createOrRestartClusterTierManager(ServerSideConfiguration serverSideConfiguration) {
    return emptyMap();
  }

  @Override
  public PerTierDataObjects createServerStore(String tierId, KeySegmentMapper mapper, ServerStoreConfiguration serverStoreConfiguration) throws ConfigurationException {
    return null;
  }

  @Override
  public RestartableGenericMap<Object, Object> createStateRepositoryMap(String tierId, String mapId) {
    return null;
  }

  @Override
  public void destroyClusterTierManager() {
    // no-op
  }

  @Override
  public void destroyServerStore(String tierId) {
    // no-op
  }

  @Override
  public boolean isRestartable(String tierId) {
    return false;
  }

  @Override
  public void validate(ServerSideConfiguration incomingConfig) throws InvalidServerSideConfigurationException {
    if (incomingConfig instanceof EnterpriseServerSideConfiguration) {
      throw new InvalidServerSideConfigurationException("Fast Restart Feature requested by client. " +
                                                        "Server side is not configured for fast restart");
    }
  }

  @Override
  public void prepareForDestroy() {
    this.destroyInProgress = true;
  }

  @Override
  public boolean isDestroyInProgress() {
    return this.destroyInProgress;
  }

  @Override
  public void beginGlobalTransaction(String tierId) {
    // no-op
  }

  @Override
  public void endGlobalTransaction(String tierId) {
    // no-op
  }

  @Override
  public EhcacheStateContext getTransactionContext(String tierId) {
    return () -> {};
  }
}
