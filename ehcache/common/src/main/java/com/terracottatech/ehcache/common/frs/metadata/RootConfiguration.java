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

import java.util.Set;

/**
 * Root configuration object(s) (e.g <i>cache manager</i>) that needs to be stored in the metadata store
 * must implement this interface.
 *
 * @author RKAV
 */
@CommonComponent
public interface RootConfiguration extends MetadataConfiguration {
  /**
   * One or more containers in which this parent object could be spread.
   * For e.g. a ehcache cache manager and its associated objects could either all be in one
   * container or spread across multiple containers.
   *
   * @return a set of container identifiers
   */
  Set<FrsContainerIdentifier> getContainers();
}