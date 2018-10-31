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

package com.terracottatech.store.client;

import com.terracottatech.entity.AggregateEndpoint;
import com.terracottatech.entity.EntityAggregatingService;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.messages.ConfigurationEncoder;

public class DatasetEntityAggregatingService<K extends Comparable<K>> implements EntityAggregatingService<DatasetEntity<K>, DatasetEntityConfiguration<K>> {

  @Override
  public boolean handlesEntityType(Class<DatasetEntity<K>> cls) {
    return DatasetEntity.class.isAssignableFrom(cls);
  }

  @Override
  public DatasetEntity<K> aggregateEntities(AggregateEndpoint<DatasetEntity<K>> endpoint) {
    if (endpoint.getEntities().size() == 1) {
      return endpoint.getEntities().get(0);
    } else {
      return new AggregatingDatasetEntity<>(endpoint);
    }
  }

  @Override
  public boolean targetConnectionForLifecycle(int stripeIndex, int totalStripes, String entityName, DatasetEntityConfiguration<K> config) {
    return true;
  }

  @Override
  public DatasetEntityConfiguration<K> formulateConfigurationForStripe(int stripeIndex, int totalStripes, String entityName, DatasetEntityConfiguration<K> config) {
    return config;
  }

  @Override
  public byte[] serializeConfiguration(DatasetEntityConfiguration<K> configuration) {
    return ConfigurationEncoder.encode(configuration);
  }

  @SuppressWarnings("unchecked")
  @Override
  public DatasetEntityConfiguration<K> deserializeConfiguration(byte[] configuration) {
    return ConfigurationEncoder.decode(configuration);
  }
}
