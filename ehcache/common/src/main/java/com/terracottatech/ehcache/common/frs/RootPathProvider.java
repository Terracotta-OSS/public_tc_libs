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

package com.terracottatech.ehcache.common.frs;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.common.frs.metadata.MetadataProvider;

import java.io.File;
import java.util.Set;

/**
 * Required interface of {@link MetadataProvider}. Provides a proper root path for a given
 * root alias. Users of the metadata provider interface must implement this interface.
 */
@CommonComponent
public interface RootPathProvider {
  /**
   * Converts a root logical name/alias to a raw disk path that is the root for this subsystem
   *
   * @param rootName root logical name
   * @return a file representing the root dir
   */
  File getRootPath(String rootName);

  /**
   * Gets the configured root, which is the root for all subsystems.
   *
   * @param rootName root logical name
   * @return a file representing the root dir
   */
  File getConfiguredRoot(String rootName);

  /**
   * Gets all existing spaces/containers from the underlying directory service
   *
   * @return a set of all existing containers
   */
  Set<File> getAllExistingContainers();

}