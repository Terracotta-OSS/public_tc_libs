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

import org.ehcache.clustered.server.KeySegmentMapper;
import org.terracotta.entity.ConfigurationException;

import com.tc.classloader.CommonComponent;
import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProvider;

import java.util.Map;

/**
 * Service for getting singleton {@link MetadataProvider} on server-side
 *
 * @author vmad
 */
@CommonComponent
public interface EhcacheFRSService {
  /**
   * Returns the {@link MetadataProvider}
   *
   * @return {@link MetadataProvider}
   */
  MetadataProvider<String, RestartableServerSideConfiguration, RestartableServerStoreConfiguration> getMetadataProvider();

  /**
   * Recovers the entire clustering tier manager and returns a map of data objects that were recovered against
   * this clustering tier manager.
   *
   * If this is a new clustering tier manager it will simply add the tier manager to metadata and returns an empty map.
   *
   *
   * @param tierManagerId Clustered tier manager id
   * @param serverSideConfiguration the restartable server side configuration
   * @return a map containing
   */
  Map<String, PerTierDataObjects> recoverOrAdd(String tierManagerId,
                                               RestartableServerSideConfiguration serverSideConfiguration);

  /**
   * Get an existing (or create new) server store in both data and metadata.
   *
   * @param tierManagerId clustering tier manager id
   * @param tierId clustering tier id
   * @param restartableServerStoreConfiguration configuration for the tier
   *
   * @return information of data object(s) created for the tier
   */
  PerTierDataObjects getOrCreateServerStore(String tierManagerId, String tierId, KeySegmentMapper mapper,
                                            RestartableServerStoreConfiguration restartableServerStoreConfiguration,
                                            boolean hybrid) throws ConfigurationException;

  RestartableGenericMap<Object,Object> getOrCreateStateRepositoryMap(String tierManagerId, String tierId, String mapId);

  void destroyServerSide(String tierManagerId);

  void destroyServerStore(String tierManagerId, String tierId);

  boolean exists(String tierManagerId, String tierId);

  void shutdown();

  void bootstrapData(KeySegmentMapper mapper);

  void bootstrapMetaData();

  void configure(String tierManagerId, String tierManagerEntityId, RestartableServerSideConfiguration restartableServerSideConfiguration)
      throws ConfigurationException;

  void beginGlobalTransaction(String tierManagerId, String tierId);

  void endGlobalTransaction(String tierManagerId, String tierId);

  void beginTransaction(String tierManagerId, String tierId);

  void endTransaction(String tierManagerId, String tierId);

  BackupCapable getBackupCoordinator();
}