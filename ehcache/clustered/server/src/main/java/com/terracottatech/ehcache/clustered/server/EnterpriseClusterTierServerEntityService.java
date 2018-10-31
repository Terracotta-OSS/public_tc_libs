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
import org.ehcache.clustered.common.internal.messages.ConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EntityConfigurationCodec;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageCodec;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpCodec;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpCodec;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.server.EhcacheExecutionStrategy;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.internal.messages.EhcacheServerCodec;
import org.ehcache.clustered.server.internal.messages.EhcacheSyncMessageCodec;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessageCodec;
import org.ehcache.clustered.server.store.ClusterTierActiveEntity;
import org.ehcache.clustered.server.store.ClusterTierPassiveEntity;
import org.terracotta.entity.CommonServerEntity;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

import com.terracottatech.ehcache.clustered.common.internal.messages.EnterpriseConfigCodec;

import static org.ehcache.clustered.common.EhcacheEntityVersion.ENTITY_VERSION;
import static org.ehcache.clustered.server.ConcurrencyStrategies.clusterTierConcurrency;

/**
 * EnterpriseClusterTierServerEntityService
 */
@OverrideService("org.ehcache.clustered.server.store.ClusterTierServerEntityService")
public class EnterpriseClusterTierServerEntityService implements EntityServerService<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final int DEFAULT_CONCURRENCY = 16;
  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(DEFAULT_CONCURRENCY);
  private static final ConfigCodec CONFIG_CODEC = new EnterpriseConfigCodec();

  private final EntityConfigurationCodec configCodec = new EntityConfigurationCodec(CONFIG_CODEC);

  @Override
  public long getVersion() {
    return ENTITY_VERSION;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return typeName.equals("org.ehcache.clustered.client.internal.store.InternalClusterTierClientEntity");
  }

  @Override
  public ClusterTierActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    LicenseEnforcer.ensure(registry, "Caching");
    ClusterTierEntityConfiguration clusteredTierEntityConfiguration = configCodec.decodeClusteredStoreConfiguration(configuration);
    return new ClusterTierActiveEntity(registry, clusteredTierEntityConfiguration, DEFAULT_MAPPER);
  }

  @Override
  public ClusterTierPassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    LicenseEnforcer.ensure(registry, "Caching");
    ClusterTierEntityConfiguration clusteredTierEntityConfiguration = configCodec.decodeClusteredStoreConfiguration(configuration);
    return new EnterpriseClusterTierPassiveEntity(registry, clusteredTierEntityConfiguration, DEFAULT_MAPPER);
  }

  @Override
  public ConcurrencyStrategy<EhcacheEntityMessage> getConcurrencyStrategy(byte[] configuration) {
    return clusterTierConcurrency(DEFAULT_MAPPER);
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

  @Override
  public ExecutionStrategy<EhcacheEntityMessage> getExecutionStrategy(byte[] configuration) {
    return new EhcacheExecutionStrategy();
  }

  @Override
  public <AP extends CommonServerEntity<EhcacheEntityMessage, EhcacheEntityResponse>> AP reconfigureEntity(ServiceRegistry registry, AP oldEntity, byte[] configuration) {
    throw new UnsupportedOperationException("Reconfigure not supported in Ehcache");
  }
}
