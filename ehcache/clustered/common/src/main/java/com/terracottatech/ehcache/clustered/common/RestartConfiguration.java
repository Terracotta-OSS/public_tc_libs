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

import java.io.Serializable;

/**
 * Restart configuration built by the client and send to the server for a cache manager
 * <p>
 * A restart log on the server is uniquely identified by the following:
 *    - {@code restartableLogRoot} the logical name of the root path (possibly a file system mount point)
 *    - {@code restartableLogContainer} a name for a container under which one or more data logs could exist with a single
 *    metadata log controlling it.
 *    - {@code restartableLogName} a name for the data log
 *<p>
 * NOTE: The current API only exposes the ability to specify the {@code restartableLogRoot}. The rest is defaulted.
 */
@CommonComponent
public class RestartConfiguration implements Serializable {
  private static final long serialVersionUID = 2979701131042278444L;

  public static final String DEFAULT_CONTAINER_NAME = "default-frs-container";
  public static final String DEFAULT_DATA_LOG_NAME = "default-cachedata";
  public static final String DEFAULT_DATA_LOG_NAME_HYBRID = "default-cachedata-hybrid";

  private final String restartableLogRoot;
  private final String restartableLogName;
  private final String restartableLogContainer;
  private final RestartableOffHeapMode offHeapMode;
  private final String frsIdentifier;

  public RestartConfiguration(String restartableLogRoot,
                              RestartableOffHeapMode offHeapMode,
                              String frsIdentifier) {
    if (restartableLogRoot == null || offHeapMode == null) {
      throw new AssertionError("Null Values is unexpected for this constructor");
    }
    this.restartableLogRoot = restartableLogRoot;
    // for now the container and data log is defaulted. We may expose this in the API in the future
    this.restartableLogContainer = DEFAULT_CONTAINER_NAME;
    this.restartableLogName = offHeapMode.equals(RestartableOffHeapMode.PARTIAL) ?
        DEFAULT_DATA_LOG_NAME_HYBRID : DEFAULT_DATA_LOG_NAME;
    this.offHeapMode = offHeapMode;
    this.frsIdentifier = frsIdentifier;
  }

  public String getRestartableLogRoot() {
    return restartableLogRoot;
  }

  public String getRestartableLogName() {
    return restartableLogName;
  }

  public boolean isHybrid() {
    return this.offHeapMode == RestartableOffHeapMode.PARTIAL;
  }

  public RestartableOffHeapMode getOffHeapMode() {
    return this.offHeapMode;
  }

  public String getRestartableLogContainer() {
    return restartableLogContainer;
  }

  public String getFrsIdentifier() {
    return frsIdentifier;
  }

  @Override
  public String toString() {
    return "root=" + '\'' + this.getRestartableLogRoot() + '\'' +
           ' ' + "container=" +
           '\'' + this.getRestartableLogContainer() + '\'' +
           ' ' + "data-log=" +
           '\'' + this.getRestartableLogName() + '\'' +
           ' ' + "offheap-mode=" +
           '\'' + this.getOffHeapMode() + '\'';
  }

  public void checkCompatibility(RestartConfiguration other, boolean checkFrsId) {
    if (!this.offHeapMode.equals(other.getOffHeapMode()) ||
        !this.restartableLogName.equals(other.restartableLogName) ||
        !this.restartableLogRoot.equals(other.restartableLogRoot) ||
        !this.restartableLogContainer.equals(other.restartableLogContainer)) {
      throw new IllegalArgumentException("Restart Configuration Mismatch: " +
                                         "Current Configuration is " + this.toString() +
                                         " Requested Configuration is " + other.toString());
    }
    if (checkFrsId) {
      // Frs Id for the tier manager need not be compatible during configure
      boolean compatible = true;
      if (this.frsIdentifier == null) {
        if (other.frsIdentifier != null) {
          compatible = false;
        }
      } else {
        compatible = this.frsIdentifier.equals(other.frsIdentifier);
      }
      if (!compatible) {
        throw new IllegalArgumentException("Restart configuration mismatch: " +
                                           "Current Clustered Tier Identifier: " + this.frsIdentifier +
                                           " Requested Clustered Tier Identifier: " + this.frsIdentifier);
      }
    }
  }
}