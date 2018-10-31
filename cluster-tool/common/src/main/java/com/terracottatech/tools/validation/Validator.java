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
package com.terracottatech.tools.validation;

import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.Stripe;

/**
 * Validator to sanitize cluster configuration.
 */
public interface Validator {

  /**
   * Validates the given cluster. Throws {@link IllegalArgumentException} if validation fails.
   *
   * @param cluster the cluster configuration
   * @param messageFragment message fragment to be displayed in error scenario
   *
   * @throws IllegalArgumentException if validation fails
   */
  void validate(Cluster cluster, String messageFragment) throws IllegalArgumentException;

  /**
   * Validates a new cluster against a configured cluster. A strict validation will pass
   * if and only if the new cluster configuration strictly matches the old cluster configuration.
   *
   *
   * @param newCluster the new cluster configuration
   * @param configuredCluster the saved configuration
   *
   * @throws IllegalArgumentException if validation fails.
   */
  void validateAgainst(Cluster newCluster, Cluster configuredCluster) throws IllegalArgumentException;

  /**
   * Validates the configuration used to start a server against the configured cluster.
   *
   * @param stripe the configuration with which the server is started
   * @param cluster the cluster configuration
   *
   * @throws IllegalArgumentException if validation fails.
   */
  void validateStripeAgainstCluster(Stripe stripe, Cluster cluster);
}
