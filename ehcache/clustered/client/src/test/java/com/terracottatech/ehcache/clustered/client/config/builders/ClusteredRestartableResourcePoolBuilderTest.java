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
package com.terracottatech.ehcache.clustered.client.config.builders;

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.hamcrest.Matchers;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests Enterprise resource pool builder {@link ClusteredRestartableResourcePoolBuilder}.
 */
public class ClusteredRestartableResourcePoolBuilderTest {

  @Test
  public void dedicated2Arg() throws Exception {
    ResourcePool pool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is(nullValue()));

    PoolAllocation allocation = ((DedicatedClusteredResourcePool)pool).getPoolAllocation();
    assertThat(allocation, instanceOf(EnterprisePoolAllocation.DedicatedRestartable.class));
    EnterprisePoolAllocation.DedicatedRestartable dedicated = (EnterprisePoolAllocation.DedicatedRestartable)allocation;
    assertThat(dedicated.getSize(), is(16*1024*1024*1024L));
    assertThat(dedicated.getDataPercent(), is(EnterprisePoolAllocation.UNINITIALIZED_DATA_PERCENT));
  }

  @Test
  public void dedicated2ArgWithValuesPercent() throws Exception {
    ResourcePool pool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(8, MemoryUnit.KB, 20);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(8L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.KB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is(nullValue()));

    PoolAllocation allocation = ((DedicatedClusteredResourcePool)pool).getPoolAllocation();
    assertThat(allocation, instanceOf(EnterprisePoolAllocation.DedicatedRestartable.class));
    EnterprisePoolAllocation.DedicatedRestartable dedicated = (EnterprisePoolAllocation.DedicatedRestartable)allocation;
    assertThat(dedicated.getSize(), is(8*1024L));
    assertThat(dedicated.getDataPercent(), is(20));
  }

  @Test
  public void dedicated2ArgUnitNull() throws Exception {
    try {
      ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(16, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void dedicated3Arg() throws Exception {
    ResourcePool pool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated("resourceId", 8, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(8L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is("resourceId"));

    PoolAllocation allocation = ((DedicatedClusteredResourcePool)pool).getPoolAllocation();
    assertThat(allocation, instanceOf(EnterprisePoolAllocation.DedicatedRestartable.class));
    EnterprisePoolAllocation.DedicatedRestartable dedicated = (EnterprisePoolAllocation.DedicatedRestartable)allocation;
    assertThat(dedicated.getSize(), is(8*1024*1024*1024L));
    assertThat(dedicated.getDataPercent(), is(EnterprisePoolAllocation.UNINITIALIZED_DATA_PERCENT));
  }

  @Test
  public void dedicated3ArgWithDataPercent() throws Exception {
    ResourcePool pool = ClusteredRestartableResourcePoolBuilder.
        clusteredRestartableDedicated("resourceId", 16, MemoryUnit.GB, 10);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is("resourceId"));

    PoolAllocation allocation = ((DedicatedClusteredResourcePool)pool).getPoolAllocation();
    assertThat(allocation, instanceOf(EnterprisePoolAllocation.DedicatedRestartable.class));
    EnterprisePoolAllocation.DedicatedRestartable dedicated = (EnterprisePoolAllocation.DedicatedRestartable)allocation;
    assertThat(dedicated.getSize(), is(16*1024*1024*1024L));
    assertThat(dedicated.getDataPercent(), is(10));
  }

  @Test
  public void dedicated3ArgFromNull() throws Exception {
    ResourcePool pool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(null, 16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is(nullValue()));
  }

  @Test
  public void dedicated3ArgUnitNull() throws Exception {
    try {
      ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated("resourceId", 16, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void shared() throws Exception {
    ResourcePool pool = ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared("resourceId");
    assertThat(pool, is(instanceOf(SharedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.is(ClusteredResourceType.Types.SHARED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((SharedClusteredResourcePool)pool).getSharedResourcePool(), is("resourceId"));

    PoolAllocation allocation = ((SharedClusteredResourcePool)pool).getPoolAllocation();
    assertThat(allocation, instanceOf(EnterprisePoolAllocation.SharedRestartable.class));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void sharedSharedResourceNull() throws Exception {
    try {
      ClusteredResourcePoolBuilder.clusteredShared(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void dedicated3ArgWithDataPercentOutOfRange() throws Exception {
    try {
      ClusteredRestartableResourcePoolBuilder.
          clusteredRestartableDedicated("resourceId", 16, MemoryUnit.GB, 100);
      fail("Specified Data percent is out of range. Pool creation is expected to fail");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void dedicated2ArgWithDataPercentOutOfRange() throws Exception {
    try {
      ClusteredRestartableResourcePoolBuilder.
          clusteredRestartableDedicated(16, MemoryUnit.GB, -1);
      fail("Specified Data percent is out of range. Pool creation is expected to fail");
    } catch (IllegalArgumentException ignored) {
    }
  }
}