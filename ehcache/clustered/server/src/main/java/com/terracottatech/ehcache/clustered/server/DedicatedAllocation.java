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
package com.terracottatech.ehcache.clustered.server;

import org.ehcache.clustered.common.PoolAllocation;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;

/**
 * An abstraction used on the server side to convert open source and enterprise types to a single type
 */
final class DedicatedAllocation {
  private final long size;
  private final String resourceName;
  private final boolean restartable;
  private final int dataPercent;

  DedicatedAllocation(PoolAllocation allocation) {
    if (allocation instanceof PoolAllocation.Dedicated) {
      PoolAllocation.Dedicated dedicated = (PoolAllocation.Dedicated)allocation;
      this.size = dedicated.getSize();
      this.resourceName = dedicated.getResourceName();
      this.restartable = false;
      this.dataPercent = -1;
    } else {
      // has to be an enterprise pool allocation.. otherwise throws class cast exception.
      EnterprisePoolAllocation.DedicatedRestartable dedicatedRestartable = (EnterprisePoolAllocation.DedicatedRestartable)allocation;
      this.size = dedicatedRestartable.getSize();
      this.resourceName = dedicatedRestartable.getResourceName();
      this.restartable = true;
      this.dataPercent = dedicatedRestartable.getDataPercent();
    }
  }

  long getSize() {
    return size;
  }

  String getResourceName() {
    return resourceName;
  }

  public boolean isRestartable() {
    return restartable;
  }

  int getDataPercent() {
    return dataPercent == EnterprisePoolAllocation.UNINITIALIZED_DATA_PERCENT ? 0 : dataPercent;
  }

  boolean isDataPercentSpecified() {
    return dataPercent != EnterprisePoolAllocation.UNINITIALIZED_DATA_PERCENT;
  }
}