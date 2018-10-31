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
package com.terracottatech.store.configuration;

/**
 * Specify lifecycle of a product feature.
 * <p>
 * Typically the product lifecycle of a feature would go from
 * EXPERIMENTAL -&gt; SUPPORTED -&gt; DEPRECATED -&gt; UNSUPPORTED
 */
public enum ProductFeatureStatus {
  /**
   * This feature is experimental and users should use caution in using this feature in
   * production systems.
   */
  EXPERIMENTAL,
  /**
   * This feature is supported and users can choose to use this feature in production
   * systems.
   */
  SUPPORTED,
  /**
   * This feature is deprecated and it is highly likely that this feature may become
   * unsupported in future releases. It is not recommended to start using a deprecated feature
   * in production systems.
   */
  DEPRECATED,
  /**
   * This feature is no longer supported and its use in production system is highly discouraged.
   */
  UNSUPPORTED
}