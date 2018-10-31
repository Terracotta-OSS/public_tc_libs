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
package com.terracottatech.store.coordination;

import com.terracottatech.entity.AggregateEndpoint;
import com.terracottatech.entity.EntityAggregatingService;

public class CoordinationEntityAggregatingService implements EntityAggregatingService<CoordinationEntity, Void> {
  @Override
  public boolean handlesEntityType(Class<CoordinationEntity> cls) {
    return CoordinationEntity.class.isAssignableFrom(cls);
  }

  @Override
  public CoordinationEntity aggregateEntities(AggregateEndpoint<CoordinationEntity> endpoint) {
    return endpoint.getEntities().get(0);
  }

  @Override
  public boolean targetConnectionForLifecycle(int stripeIndex, int totalStripes, String entityName, Void config) {
    return stripeIndex == 0;
  }

  @Override
  public Void formulateConfigurationForStripe(int stripeIndex, int totalStripes, String entityName, Void config) {
    return config;
  }

  @Override
  public byte[] serializeConfiguration(Void configuration) {
    return new byte[0];
  }

  @Override
  public Void deserializeConfiguration(byte[] configuration) {
    return null;
  }
}
