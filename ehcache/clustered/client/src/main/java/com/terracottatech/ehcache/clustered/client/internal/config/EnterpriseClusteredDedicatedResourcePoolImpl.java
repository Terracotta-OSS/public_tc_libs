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
package com.terracottatech.ehcache.clustered.client.internal.config;

import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.config.units.MemoryUnit;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;

public class EnterpriseClusteredDedicatedResourcePoolImpl extends DedicatedClusteredResourcePoolImpl {
  private final int dataPercent;

  public EnterpriseClusteredDedicatedResourcePoolImpl(String fromResource,
                                                      long size,
                                                      MemoryUnit unit) {
    super(fromResource, size, unit);
    this.dataPercent = EnterprisePoolAllocation.UNINITIALIZED_DATA_PERCENT;
  }

  public EnterpriseClusteredDedicatedResourcePoolImpl(String fromResource,
                                                      long size,
                                                      MemoryUnit unit,
                                                      int dataPercent) {
    super(fromResource, size, unit);
    if (dataPercent < 0 || dataPercent > 99) {
      throw new IllegalArgumentException("Percentage pool share for user data must be " +
                                         "between 0 and 99, both inclusive");
    }
    this.dataPercent = dataPercent;
  }

  @Override
  public PoolAllocation getPoolAllocation() {
    return new EnterprisePoolAllocation.DedicatedRestartable(
        this.getFromResource(), this.getUnit().toBytes(this.getSize()), dataPercent);
  }

  public int getDataPercent() {
    return dataPercent;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Pool {");
    sb.append(this.getSize());
    sb.append(' ');
    sb.append(this.getUnit());
    sb.append(' ');
    sb.append(this.getType());
    if(this.isPersistent()) {
      sb.append("(persistent)");
    }

    sb.append(' ');
    sb.append("from=");
    if(this.getFromResource() == null) {
      sb.append("N/A");
    } else {
      sb.append('\'').append(this.getFromResource()).append('\'');
    }

    sb.append(' ').append("(restartable)");
    if (this.dataPercent >= 0) {
      sb.append(' ').append("dataPercent=").append(this.dataPercent);
    }
    sb.append('}');
    return sb.toString();
  }
}