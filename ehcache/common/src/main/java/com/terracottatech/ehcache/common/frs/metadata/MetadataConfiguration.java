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
package com.terracottatech.ehcache.common.frs.metadata;

import com.tc.classloader.CommonComponent;

/**
 * Base interface that all configuration objects stored as metadata must implement.
 *
 * @author RKAV
 */
@CommonComponent
public interface MetadataConfiguration {
  /**
   * Index that helps in uniquely creating data log identifiers across multiple
   * incarnation of an object.
   * <p>
   * For e.g. a <i>cache</i> could be destroyed and another cache with the same alias could be created.
   * This index ensures that the FRS identifier for data objects does not overlap across such re-incarnation
   * of metadata objects.
   *
   * @return an integer that is unique for all
   */
  int getObjectIndex();

  /**
   * Sets the index. Implementations must ensure that the index is stored within the configuration object.
   *
   * @param index the index that needs to be set for this object.
   */
  void setObjectIndex(int index);

  /**
   * Validates the passed in configuration against {@code this} configuration and throws appropriate exceptions,
   * if incompatibilities are found.
   *
   * @param newMetadataConfiguration an incoming configuration.
   */
  void validate(MetadataConfiguration newMetadataConfiguration);
}