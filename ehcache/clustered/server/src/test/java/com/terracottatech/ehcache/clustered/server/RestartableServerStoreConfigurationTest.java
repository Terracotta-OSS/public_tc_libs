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

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerStoreConfiguration;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author vmad
 */
public class RestartableServerStoreConfigurationTest {
  private static final String TEST_ROOT = "test-root";
  private static final RestartableOffHeapMode OFF_HEAP_MODE = RestartableOffHeapMode.FULL;
  private static final String TEST_CACHE_MANAGER_ID = "test-cache-manager-id";

  @Test
  public void validateServerStoreConfiguration() throws Exception {
    final RestartableServerStoreConfiguration rs1 = new RestartableServerStoreConfiguration(TEST_CACHE_MANAGER_ID,
        createServerStoreConfiguration(), createRestartConfiguration());
    final RestartableServerStoreConfiguration rs2 = new RestartableServerStoreConfiguration(TEST_CACHE_MANAGER_ID,
        createServerStoreConfiguration(), createRestartConfiguration());

    try {
      rs1.validate(rs2);
    } catch (IllegalArgumentException e) {
      fail("validate() shouldn't throw an exception as both configurations are same");
    }

    final RestartableServerStoreConfiguration rs3 = new RestartableServerStoreConfiguration(TEST_CACHE_MANAGER_ID,
        createServerStoreConfiguration2(), createRestartConfiguration());

    try {
      rs2.validate(rs3);
      fail("validate() should throw an exception as configurations are different");
    } catch (IllegalArgumentException ignored) {}
  }

  @Test
  public void testRunnelSupport() throws Exception {
    PoolAllocation poolAllocation = new EnterprisePoolAllocation.DedicatedRestartable("resource", 10L, 0);
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(poolAllocation,
      String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);
    RestartConfiguration restartConfiguration = new RestartConfiguration(TEST_ROOT, OFF_HEAP_MODE, TEST_CACHE_MANAGER_ID);
    final RestartableServerStoreConfiguration restartableServerStoreConfiguration = new RestartableServerStoreConfiguration(TEST_CACHE_MANAGER_ID,
      serverStoreConfiguration, restartConfiguration);

    final ByteBuffer encodedBytes = restartableServerStoreConfiguration.encode();
    final RestartableServerStoreConfiguration decodedConfig = new RestartableServerStoreConfiguration(encodedBytes);

    try {
      restartableServerStoreConfiguration.validate(decodedConfig);
    } catch (IllegalArgumentException e) {
      fail("validate() shouldn't throw an exception as both configuration are same: " + e);
    }

    assertThat(decodedConfig.getParentId(), is(restartableServerStoreConfiguration.getParentId()));
    assertThat(decodedConfig.getDataLogIdentifier(), is(restartableServerStoreConfiguration.getDataLogIdentifier()));
    assertThat(decodedConfig.getObjectIndex(), is(restartableServerStoreConfiguration.getObjectIndex()));
  }

  private ServerStoreConfiguration createServerStoreConfiguration() {
    PoolAllocation poolAllocation = new EnterprisePoolAllocation.DedicatedRestartable("resource", 10L, 0);

    return new ServerStoreConfiguration(poolAllocation,
      String.class.getName(),
      String.class.getName(),
      String.class.getName(),
      String.class.getName(),
      Consistency.EVENTUAL);
  }

  private ServerStoreConfiguration createServerStoreConfiguration2() {
    PoolAllocation poolAllocation = new PoolAllocation.Shared("resource");

    return new ServerStoreConfiguration(poolAllocation,
        String.class.getName(),
        String.class.getName(),
        String.class.getName(),
        String.class.getName(),
        Consistency.STRONG);
  }

  private RestartConfiguration createRestartConfiguration() {
    return new RestartConfiguration(TEST_ROOT, OFF_HEAP_MODE, TEST_CACHE_MANAGER_ID);
  }
}