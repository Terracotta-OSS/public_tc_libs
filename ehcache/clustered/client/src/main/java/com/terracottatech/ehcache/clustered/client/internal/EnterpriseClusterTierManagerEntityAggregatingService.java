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
package com.terracottatech.ehcache.clustered.client.internal;

import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.messages.EntityConfigurationCodec;

import com.terracottatech.ehcache.clustered.client.internal.config.ConfigurationSplitter;
import com.terracottatech.ehcache.clustered.common.internal.messages.EnterpriseConfigCodec;
import com.terracottatech.entity.AggregateEndpoint;
import com.terracottatech.entity.EntityAggregatingService;

/**
 * EnterpriseClusterTierManagerEntityAggregatingService
 */
public class EnterpriseClusterTierManagerEntityAggregatingService implements EntityAggregatingService<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration> {

  private final EntityConfigurationCodec configCodec = new EntityConfigurationCodec(new EnterpriseConfigCodec());
  private final ConfigurationSplitter configSplitter = new ConfigurationSplitter();

  @Override
  public boolean handlesEntityType(Class<ClusterTierManagerClientEntity> cls) {
    return ClusterTierManagerClientEntity.class.isAssignableFrom(cls);
  }

  @Override
  public ClusterTierManagerClientEntity aggregateEntities(AggregateEndpoint<ClusterTierManagerClientEntity> endpoint) {
    return new AggregatingEhcacheClientEntity(endpoint);
  }

  @Override
  public boolean targetConnectionForLifecycle(int stripeIndex, int totalStripes, String entityName, ClusterTierManagerConfiguration config) {
    return true;
  }

  @Override
  public ClusterTierManagerConfiguration formulateConfigurationForStripe(int stripeIndex, int totalStripes, String entityName, ClusterTierManagerConfiguration config) {
    return new ClusterTierManagerConfiguration(config.getIdentifier(),
        configSplitter.splitServerSideConfiguration(config.getConfiguration(), totalStripes));
  }

  @Override
  public byte[] serializeConfiguration(ClusterTierManagerConfiguration configuration) {
    return configCodec.encode(configuration);
  }

  @Override
  public ClusterTierManagerConfiguration deserializeConfiguration(byte[] configuration) {
    return configCodec.decodeClusterTierManagerConfiguration(configuration);
  }
}
