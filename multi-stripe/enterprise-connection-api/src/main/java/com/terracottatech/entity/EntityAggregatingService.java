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

package com.terracottatech.entity;

import org.terracotta.connection.entity.Entity;


/**
 * @param <T> Type of the entity
 * @param <C> configuration
 */
public interface EntityAggregatingService<T extends Entity, C> {
  /**
   * Check if this service handles the given entity type.
   *
   * @param cls type to check
   * @return true if this service does handle the given type
   */
  boolean handlesEntityType(Class<T> cls);
  /**
   * Provides an implementation of the interface that aggregates the underlying stripe implementations
   *
   * @param endpoint
   * @return an aggregated entity
   */
  T aggregateEntities(AggregateEndpoint<T> endpoint);
  /**
   * Let's the implementation know if a lifecycle operation is desired on a particular stripe.  This
   * is expected to be a static function determined by the information passed in by the platform.
   *
   * @param stripeIndex zero based index of the stripe in the list of stripes that make up this cluster
   * @param totalStripes total number of stripes in the cluster
   * @param entityName the name of the entity targeted
   * @param config the entity configuration object
   * @return true if the lifecycle operation should be performed on the named connection
   */
  boolean targetConnectionForLifecycle(int stripeIndex, int totalStripes, String entityName, C config);
  /**
   * Creates a configuration of the stripe based on the configuration for the cluster.
   *
   * @param stripeIndex zero based index of the stripe in the list of stripes that make up this cluster
   * @param totalStripes total number of stripes in the cluster
   * @param entityName the name of the entity targeted
   * @param config the parent configuration for the clustered entity
   * @return a configuration for the stripe entity.  Can be the same as the parent
   */
  C formulateConfigurationForStripe(int stripeIndex, int totalStripes, String entityName, C config);

  /**
   * Serialize the configuration for this entity type out to a byte array
   *
   * @param configuration configuration to be serialized
   * @return serialized config
   */
  byte[] serializeConfiguration(C configuration);

  /**
   * Deserialize a configuration from bytes
   *
   * @param configuration bytes to be deserialized
   * @return deserialized config
   */
  C deserializeConfiguration(byte[] configuration);
}
