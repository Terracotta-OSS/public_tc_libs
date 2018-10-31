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
import org.ehcache.clustered.common.PoolAllocation;

import java.io.Serializable;

/**
 * {@link PoolAllocation} for clustered tier along with {@link RestartConfiguration}
 */
@CommonComponent
public interface EnterprisePoolAllocation extends Serializable {
  int UNINITIALIZED_DATA_PERCENT = -1;

  @CommonComponent
  final class DedicatedRestartable implements PoolAllocation.DedicatedPoolAllocation, EnterprisePoolAllocation {
    private static final long serialVersionUID = -7538743600051587147L;

    private final long size;
    private final String resourceName;
    private final int dataPercent;

    public DedicatedRestartable(final String resourceName,
                                final long size,
                                final int dataPercent) {
      this.size = size;
      this.resourceName = resourceName;
      this.dataPercent = dataPercent; // for hybrid caches with partial value caching
    }

    public long getSize() {
      return size;
    }

    public String getResourceName() {
      return resourceName;
    }

    public int getDataPercent() {
      return dataPercent;
    }

    @Override
    public boolean isCompatible(final PoolAllocation other) {
      if (this == other) return true;
      if (other == null) return false;
      if (other.getClass().isAssignableFrom(Unknown.class)) return true;
      if (!other.getClass().isAssignableFrom(DedicatedRestartable.class)) return false;

      final DedicatedRestartable dedicated = (DedicatedRestartable)other;

      if (size != dedicated.size) return false;
      if (dataPercent != dedicated.dataPercent) return false;
      return resourceName != null ? resourceName.equals(dedicated.resourceName) : dedicated.resourceName == null;
    }

    @Override
    public String toString() {
      return "DedicatedRestartable{" +
             "size=" + size +
             ", resourceName='" + resourceName + '\'' +
             ", dataPercent=" + dataPercent +
             '}';
    }
  }

  @CommonComponent
  final class SharedRestartable implements PoolAllocation.SharedPoolAllocation, EnterprisePoolAllocation {
    private static final long serialVersionUID = 9099771362168467352L;

    private final String resourcePoolName;

    public SharedRestartable(final String resourcePoolName) {
      this.resourcePoolName = resourcePoolName;
    }

    public String getResourcePoolName() {
      return resourcePoolName;
    }

    @Override
    public boolean isCompatible(final PoolAllocation other) {
      if (this == other) return true;
      if (other == null) return false;
      if (other.getClass().isAssignableFrom(Unknown.class)) return true;
      if (!other.getClass().isAssignableFrom(SharedRestartable.class)) return false;

      final SharedRestartable shared = (SharedRestartable)other;

      return resourcePoolName.equals(shared.resourcePoolName);
    }

    @Override
    public String toString() {
      return "SharedRestartable{" + "resourcePoolName='" + resourcePoolName + "'}";
    }
  }
}