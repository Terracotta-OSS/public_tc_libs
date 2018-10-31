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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;
import com.terracottatech.ehcache.clustered.server.services.frs.RestartableServerSideConfiguration;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.FULL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author vmad
 */
public class RestartableServerSideConfigurationTest {
  private static final String TEST_ROOT = "test-root";
  private static final RestartableOffHeapMode OFF_HEAP_MODE = FULL;
  private static final String TEST_CACHE_MANAGER_ID = "test-cache-manager-id";
  private static final String TEST_CACHE_MANAGER_ID2 = "test-cache-manager-id2";
  private static final String DEFAULT_RESOURCE   = "default_resource";
  private static final String TEST_POOL          = "test-pool";
  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/" + TEST_CACHE_MANAGER_ID2);

  @Test
  public void validateWithOutDefaultResource() throws Exception {
    final RestartableServerSideConfiguration configuration = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(createPools(2), createRestartConfiguration(0)));

    final RestartableServerSideConfiguration sameConfiguration = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(createPools(2), createRestartConfiguration(0)));

    try {
      configuration.validate(sameConfiguration);
    } catch (IllegalArgumentException e) {
      fail("validate() shouldn't throw an exception as both configurations are same");
    }

    final RestartableServerSideConfiguration differentConfiguration =
        new RestartableServerSideConfiguration(new EnterpriseServerSideConfiguration(createPools(1),
            createRestartConfiguration(0)));

    try {
      configuration.validate(differentConfiguration);
      fail("validate() should throw an exception as configurations are different");
    } catch (IllegalArgumentException ignored) {}
  }

  @Test
  public void validateWithDefaultResource() throws Exception {
    final RestartableServerSideConfiguration configuration = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(DEFAULT_RESOURCE, createPools(2), createRestartConfiguration(0)));

    final RestartableServerSideConfiguration sameConfiguration = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(DEFAULT_RESOURCE, createPools(2), createRestartConfiguration(0)));

    try {
      configuration.validate(sameConfiguration);
    } catch (IllegalArgumentException e) {
      fail("validate() shouldn't throw an exception as both configurations are same");
    }

    final RestartableServerSideConfiguration differentConfiguration = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(DEFAULT_RESOURCE, createPools(1), createRestartConfiguration(0)));

    try {
      configuration.validate(differentConfiguration);
      fail("validate() should throw an exception as configurations are different");
    } catch (IllegalArgumentException ignored) {}
  }

  @Test
  public void validateMix() throws Exception {
    final RestartableServerSideConfiguration configurationWithDefaultResource = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(DEFAULT_RESOURCE, createPools(2), createRestartConfiguration(0)));

    final RestartableServerSideConfiguration configurationWithoutDefaultResource = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(createPools(2), createRestartConfiguration(0)));

    try {
      configurationWithDefaultResource.validate(configurationWithoutDefaultResource);
      fail("validate() should throw an exception as both configurations are same");
    } catch (IllegalArgumentException ignored) {}

    try {
      configurationWithoutDefaultResource.validate(configurationWithDefaultResource);
      fail("validate() should throw an exception as both configurations are same");
    } catch (IllegalArgumentException ignored) {}
  }

  @Test
  public void validateWithDifferentRestartConfigs() throws Exception {
    RestartableServerSideConfiguration configuration = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(createPools(2), createRestartConfiguration(0)));
    for (int i = 1; i < 4; i++) {
      RestartableServerSideConfiguration differentConfiguration = new RestartableServerSideConfiguration(
          new EnterpriseServerSideConfiguration(createPools(2), createRestartConfiguration(i)));
      try {
        configuration.validate(differentConfiguration);
        if (i < 3) {
          fail("validate() should throw an exception as configurations are different");
        }
      } catch (IllegalArgumentException e) {
        if (i >= 3) {
          // frs identifiers are allowed to be changed
          fail("Frs Identifiers could change on movement of log files. So this should be allowed");
        }
      }
    }
  }

  @Test
  public void validateWithDifferentCacheManagers() throws Exception {
    final RestartConfiguration restartConfigurationOne = createRestartConfiguration(0);
    final RestartConfiguration restartConfigurationTwo = createAnotherRestartConfiguration();
    assertThat(restartConfigurationOne.getFrsIdentifier(), is(TEST_CACHE_MANAGER_ID));
    assertNull(restartConfigurationTwo.getFrsIdentifier());

    final RestartableServerSideConfiguration configurationWithCacheManagerOne = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(DEFAULT_RESOURCE, createPools(2), restartConfigurationOne));

    final RestartableServerSideConfiguration configurationWithCacheManagerTwo = new RestartableServerSideConfiguration(
        new EnterpriseServerSideConfiguration(DEFAULT_RESOURCE, createPools(2), restartConfigurationTwo));

    try {
      configurationWithCacheManagerOne.validate(configurationWithCacheManagerTwo);
      fail("validate() should throw an exception as both configurations are same");
    } catch (IllegalArgumentException ignored) {}

    try {
      configurationWithCacheManagerTwo.validate(configurationWithCacheManagerOne);
      fail("validate() should throw an exception as both configurations are same");
    } catch (IllegalArgumentException ignored) {}
  }

  @Test
  public void testRunnelSupport() throws Exception {
    final RestartableServerSideConfiguration configuration = new RestartableServerSideConfiguration(
      new EnterpriseServerSideConfiguration(DEFAULT_RESOURCE, createPools(2), createRestartConfiguration(0)));
    final ByteBuffer encodedBytes = configuration.encode();
    final RestartableServerSideConfiguration decodedConfig = new RestartableServerSideConfiguration(encodedBytes);
    try {
      configuration.validate(decodedConfig);
    } catch (IllegalArgumentException e) {
      fail("validate() shouldn't throw an exception as both configuration are same: " + e);
    }
    assertThat(decodedConfig.getContainers(), is(configuration.getContainers()));
    assertThat(decodedConfig.getObjectIndex(), is(configuration.getObjectIndex()));
  }

  private Map<String, ServerSideConfiguration.Pool> createPools(int size) {
    Map<String, ServerSideConfiguration.Pool> pools = new HashMap<>();
    for(int i = 0; i < size; i++) {
      pools.put(TEST_POOL + i, new ServerSideConfiguration.Pool(1024, TEST_POOL + i));
    }
    return pools;
  }

  private RestartConfiguration createRestartConfiguration(int changeIdx) {
    switch (changeIdx) {
      case 0:
        return new RestartConfiguration(TEST_ROOT, OFF_HEAP_MODE, TEST_CACHE_MANAGER_ID);
      case 1:
        return new RestartConfiguration(TEST_ROOT + 1, OFF_HEAP_MODE, TEST_CACHE_MANAGER_ID);
      case 2:
        return new RestartConfiguration(TEST_ROOT, RestartableOffHeapMode.PARTIAL, TEST_CACHE_MANAGER_ID);
      case 3:
      default:
        return new RestartConfiguration(TEST_ROOT, OFF_HEAP_MODE, TEST_CACHE_MANAGER_ID2);
    }
  }

  private RestartConfiguration createAnotherRestartConfiguration() {
    return new RestartConfiguration(TEST_ROOT + 1, OFF_HEAP_MODE, null);
  }
}
