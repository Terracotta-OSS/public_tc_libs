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
package com.terracottatech.ehcache.clustered.common;

import com.tc.classloader.CommonComponent;

/**
 * Dictate what is kept in the offheap when it is backed by a restart log.
 */
@CommonComponent
public enum RestartableOffHeapMode {
  /**
   * Indicates that all data and metadata for the restartable cache will also be in stored in offheap.
   * In this mode the restart log will act as a pure append only log.
   */
  FULL,

  /**
   * Indicates that all metadata and possibly some partial data for the restartable cache will be stored
   * in offheap. Uncached data will be fetched from restart log on demand.
   */
  PARTIAL
}