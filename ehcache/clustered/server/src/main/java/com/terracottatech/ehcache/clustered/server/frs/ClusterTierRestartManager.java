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
import com.terracottatech.ehcache.clustered.server.services.frs.PerTierDataObjects;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;

import java.util.Map;

/**
 * ClusterTierRestartManager
 */
@CommonComponent
public interface ClusterTierRestartManager {
  void configure(ServerSideConfiguration configuration) throws ConfigurationException;

  void restart(KeySegmentMapper mapper);

  Map<String, PerTierDataObjects> createOrRestartClusterTierManager(ServerSideConfiguration serverSideConfiguration);

  PerTierDataObjects createServerStore(String tierId, KeySegmentMapper mapper,
                                       ServerStoreConfiguration serverStoreConfiguration) throws ConfigurationException;

  RestartableGenericMap<Object, Object> createStateRepositoryMap(String tierId, String mapId);

  void destroyClusterTierManager();

  void destroyServerStore(String tierId);

  boolean isRestartable(String tierId);

  void validate(ServerSideConfiguration incomingConfig) throws InvalidServerSideConfigurationException;

  void prepareForDestroy();

  boolean isDestroyInProgress();

  void beginGlobalTransaction(String tierId);

  void endGlobalTransaction(String tierId);

  EhcacheStateContext getTransactionContext(String tierId);
}
