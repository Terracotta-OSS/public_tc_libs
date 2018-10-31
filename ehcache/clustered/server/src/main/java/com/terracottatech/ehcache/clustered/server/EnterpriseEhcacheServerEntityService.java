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

import com.tc.classloader.OverrideService;
import com.terracottatech.licensing.services.LicenseEnforcer;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.messages.ConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EntityConfigurationCodec;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageCodec;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpCodec;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpCodec;
import org.ehcache.clustered.server.ClusterTierManagerActiveEntity;
import org.ehcache.clustered.server.ClusterTierManagerPassiveEntity;
import org.ehcache.clustered.server.ClusterTierManagerServerEntityService;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.internal.messages.EhcacheServerCodec;
import org.ehcache.clustered.server.internal.messages.EhcacheSyncMessageCodec;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessageCodec;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

import com.terracottatech.ehcache.clustered.common.internal.messages.EnterpriseConfigCodec;
import com.terracottatech.ehcache.clustered.server.management.EnterpriseManagement;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateService;

@OverrideService("org.ehcache.clustered.server.ClusterTierManagerServerEntityService")
public class EnterpriseEhcacheServerEntityService extends ClusterTierManagerServerEntityService {
  private static final int DEFAULT_CONCURRENCY = 16;
  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(DEFAULT_CONCURRENCY);
  private static final ConfigCodec CONFIG_CODEC = new EnterpriseConfigCodec();
  private final EntityConfigurationCodec configCodec = new EntityConfigurationCodec(new EnterpriseConfigCodec());

  @Override
  public ClusterTierManagerActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierManagerConfiguration clusteredTierManagerConfiguration = configCodec.decodeClusterTierManagerConfiguration(configuration);
    EnterpriseEhcacheStateService ehcacheStateService;
    try {
      ehcacheStateService = (EnterpriseEhcacheStateService) registry.getService(new EhcacheStateServiceConfig(clusteredTierManagerConfiguration, registry, DEFAULT_MAPPER));
    } catch (ServiceException e) {
      throw new ConfigurationException("Failed to obtain the singleton EnterpriseEhcacheStateService instance for "
          + clusteredTierManagerConfiguration.getIdentifier(), e);
    }

    LicenseEnforcer.ensure(registry, "Caching");
    EnterpriseManagement management = new EnterpriseManagement(registry, ehcacheStateService, true, clusteredTierManagerConfiguration.getIdentifier());
    return new ClusterTierManagerActiveEntity(clusteredTierManagerConfiguration, ehcacheStateService, management);
  }

  @Override
  public ClusterTierManagerPassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierManagerConfiguration clusteredTierManagerConfiguration = configCodec.decodeClusterTierManagerConfiguration(configuration);
    EnterpriseEhcacheStateService ehcacheStateService;
    try {
      ehcacheStateService = (EnterpriseEhcacheStateService) registry.getService(new EhcacheStateServiceConfig(clusteredTierManagerConfiguration, registry, DEFAULT_MAPPER));
    } catch (ServiceException e) {
      throw new ConfigurationException("Failed to obtain the singleton EnterpriseEhcacheStateService instance for "
          + clusteredTierManagerConfiguration.getIdentifier(), e);
    }

    LicenseEnforcer.ensure(registry, "Caching");
    EnterpriseManagement management = new EnterpriseManagement(registry, ehcacheStateService, false, clusteredTierManagerConfiguration.getIdentifier());
    return new ClusterTierManagerPassiveEntity(clusteredTierManagerConfiguration, ehcacheStateService, management);
  }

  @Override
  public MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> getMessageCodec() {
    EhcacheCodec ehcacheCodec = new EhcacheCodec(new ServerStoreOpCodec(),
      new LifeCycleMessageCodec(CONFIG_CODEC), new StateRepositoryOpCodec(), new ResponseCodec());
    return new EhcacheServerCodec(ehcacheCodec, new PassiveReplicationMessageCodec());
  }

  @Override
  public SyncMessageCodec<EhcacheEntityMessage> getSyncMessageCodec() {
    return new EhcacheSyncMessageCodec(new ResponseCodec());
  }
}
