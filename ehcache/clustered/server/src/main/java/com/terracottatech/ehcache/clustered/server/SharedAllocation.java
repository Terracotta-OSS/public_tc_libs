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
 * Server side abstraction to provide a single interface
 */
final class SharedAllocation {
  private final String resourcePoolName;
  private final boolean restartable;

  SharedAllocation(PoolAllocation allocation) {
    if (allocation instanceof PoolAllocation.Shared) {
      PoolAllocation.Shared shared = (PoolAllocation.Shared)allocation;
      this.resourcePoolName = shared.getResourcePoolName();
      this.restartable = false;
    } else {
      // has to be an enterprise pool allocation.. otherwise throws class cast exception.
      EnterprisePoolAllocation.SharedRestartable sharedRestartable = (EnterprisePoolAllocation.SharedRestartable)allocation;
      this.resourcePoolName = sharedRestartable.getResourcePoolName();
      this.restartable = true;
    }
  }

  String getResourcePoolName() {
    return resourcePoolName;
  }

  public boolean isRestartable() {
    return restartable;
  }

}