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

import org.ehcache.clustered.client.internal.ClusterTierManagerValidationException;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.common.ServerSideConfiguration;

import com.terracottatech.ehcache.clustered.client.internal.config.ConfigurationSplitter;
import com.terracottatech.entity.AggregateEndpoint;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;

import java.util.Set;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptySet;

/**
 * AggregatingEhcacheClientEntity
 */
public class AggregatingEhcacheClientEntity implements ClusterTierManagerClientEntity {

  private final ConfigurationSplitter configSplitter = new ConfigurationSplitter();
  private final AggregateEndpoint<ClusterTierManagerClientEntity> endpoint;
  private final int stripeCount;

  public AggregatingEhcacheClientEntity(AggregateEndpoint<ClusterTierManagerClientEntity> endpoint) {
    this.endpoint = endpoint;
    stripeCount = endpoint.getEntities().size();
  }

  @Override
  public void validate(ServerSideConfiguration config) throws ClusterTierManagerValidationException, TimeoutException, ClusterException {
    ServerSideConfiguration splitConfig = configSplitter.splitServerSideConfiguration(config, stripeCount);
    for (ClusterTierManagerClientEntity singleEntity : endpoint.getEntities()) {
      singleEntity.validate(splitConfig);
    }
  }

  @Override
  public Set<String> prepareForDestroy() {
    Set<String> result = emptySet();
    for (ClusterTierManagerClientEntity singleEntity : endpoint.getEntities()) {
      result = singleEntity.prepareForDestroy();
    }
    return result;
  }

  @Override
  public void close() {
    endpoint.close();
  }

}
