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
package org.terracotta.catalog.client;

import com.terracottatech.entity.AggregateEndpoint;
import com.terracottatech.entity.EntityAggregatingService;
import org.terracotta.catalog.SystemCatalog;

public class SystemCatalogAggregatingService implements EntityAggregatingService<SystemCatalog, Void> {
  @Override
  public boolean handlesEntityType(Class<SystemCatalog> cls) {
    return SystemCatalog.class.isAssignableFrom(cls);
  }

  @Override
  public SystemCatalog aggregateEntities(AggregateEndpoint<SystemCatalog> endpoint) {
    // system catalog is always on stripe 0, and only there
    return endpoint.getEntities().get(0);
  }

  @Override
  public boolean targetConnectionForLifecycle(int stripeIndex, int totalStripes, String entityName, Void config) {
    // system catalog is always on stripe 0, and only there
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
