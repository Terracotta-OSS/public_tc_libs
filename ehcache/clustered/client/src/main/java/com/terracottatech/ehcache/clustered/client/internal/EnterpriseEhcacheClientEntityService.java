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
import org.ehcache.clustered.client.internal.SimpleClusterTierManagerClientEntity;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EntityConfigurationCodec;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageCodec;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpCodec;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpCodec;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.MessageCodec;

import com.tc.classloader.OverrideService;
import com.terracottatech.ehcache.clustered.common.internal.messages.EnterpriseConfigCodec;

@OverrideService("org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityService")
public class EnterpriseEhcacheClientEntityService implements EntityClientService<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, EhcacheEntityMessage, EhcacheEntityResponse, Void> {

  private final EntityConfigurationCodec configCodec = new EntityConfigurationCodec(new EnterpriseConfigCodec());

  @Override
  public boolean handlesEntityType(Class<ClusterTierManagerClientEntity> cls) {
    return ClusterTierManagerClientEntity.class.isAssignableFrom(cls);
  }

  @Override
  public byte[] serializeConfiguration(ClusterTierManagerConfiguration configuration) {
    return configCodec.encode(configuration);
  }

  @Override
  public ClusterTierManagerConfiguration deserializeConfiguration(byte[] configuration) {
    return configCodec.decodeClusterTierManagerConfiguration(configuration);
  }

  @Override
  public ClusterTierManagerClientEntity create(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint, Void empty) {
    return new SimpleClusterTierManagerClientEntity(endpoint);
  }

  @Override
  public MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> getMessageCodec() {
    return new EhcacheCodec(new ServerStoreOpCodec(), new LifeCycleMessageCodec(new EnterpriseConfigCodec()),
      new StateRepositoryOpCodec(), new ResponseCodec());
  }
}
