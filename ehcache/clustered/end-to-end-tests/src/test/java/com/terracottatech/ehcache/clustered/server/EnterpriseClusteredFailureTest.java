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

import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateServiceProvider;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.After;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.passthrough.PassthroughTestHelpers;

import com.terracottatech.ehcache.clustered.client.internal.EnterpriseClusterTierClientEntityService;
import com.terracottatech.ehcache.clustered.client.internal.EnterpriseEhcacheClientEntityService;
import com.terracottatech.testing.persistence.DisabledPlatformPersistenceProvider;

import java.net.URI;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class EnterpriseClusteredFailureTest extends BasicAbstractEnterprisePassthroughTest {
  static final String DEFAULT_PRIMARY_RESOURCE = "primary-server-resource";
  static final String SECONDARY_RESOURCE = "secondary-server-resource";

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    OffheapResourcesType offHeapResources = PassthroughTestHelper
        .getOffheapResources(64, org.terracotta.offheapresource.config.MemoryUnit.MB, DEFAULT_PRIMARY_RESOURCE, SECONDARY_RESOURCE);

    return server -> {
      server.registerServerEntityService(new EnterpriseEhcacheServerEntityService());
      server.registerClientEntityService(new EnterpriseEhcacheClientEntityService());
      server.registerServerEntityService(new EnterpriseClusterTierServerEntityService());
      server.registerClientEntityService(new EnterpriseClusterTierClientEntityService());
      server.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
      server.registerClientEntityService(new VoltronReadWriteLockEntityClientService());
      server.registerExtendedConfiguration(new OffHeapResourcesProvider(offHeapResources));
      server.registerOverrideServiceProvider(new EnterpriseEhcacheStateServiceProvider(), () -> EnterpriseEhcacheStateServiceProvider.class);
      server.registerOverrideServiceProvider(new DisabledPlatformPersistenceProvider(), () -> DisabledPlatformPersistenceProvider.class);
    };
  }

  @After
  public void cleanup() throws Exception {
    newCacheManagerBuilder()
        .with(EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(clusterURI()).expecting())
        .build().destroy();
  }

  private URI clusterURI() {
    return URI.create(buildClusterUri().toString() + "/invalidCM");
  }

  @Test
  public void testRestartableCacheManagerFailsWithoutDataRoots() throws Exception {
    try {
      CacheManagerBuilder
          .newCacheManagerBuilder()
          .with(enterpriseCluster(clusterURI())
              .autoCreate()
              .restartable("invalid"))
          .build(true);
    } catch (StateTransitionException e) {
      assertThat(e.getMessage(), containsString("Could not create"));
      Throwable rootCause = e;
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      assertThat(rootCause.getMessage(), containsString("not supported without data directories"));
    }
  }

}
