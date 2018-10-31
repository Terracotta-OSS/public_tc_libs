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

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateServiceProvider;
import com.terracottatech.persistence.frs.FRSPersistenceServiceProvider;
import com.terracottatech.persistence.frs.config.FRSPersistenceServiceProviderConfiguration;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Test;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.math.BigInteger;
import java.net.URI;
import java.util.Random;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;

public class LargeValueTest extends BasicAbstractEnterprisePassthroughTest {

  private static final String DATA_ROOT_ID = "root1";

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    return new ServerInitializerBuilder()
        .resource("primary-server-resource", 32, MemoryUnit.MB)
        .dataRoot(DATA_ROOT_ID, createDataRootDir())
        .serviceProvider(new FRSPersistenceServiceProvider(), new FRSPersistenceServiceProviderConfiguration(DATA_ROOT_ID))
        .overrideServiceProvider(new EnterpriseEhcacheStateServiceProvider(), () -> EnterpriseEhcacheStateServiceProvider.class)
        .build();
  }

  @After
  public void cleanup() throws Exception {
    newCacheManagerBuilder()
        .with(EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(clusterURI()).expecting())
        .build().destroy();
  }

  private URI clusterURI() {
    return URI.create(buildClusterUri().toString() + "/my-application");
  }

  @Test
  public void testLargeValues() throws Exception {
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
            newCacheManagerBuilder()
                    .with(EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster(clusterURI()).autoCreate()
                    .restartable(DATA_ROOT_ID))
                    .withCache("small-cache", newCacheConfigurationBuilder(Long.class, String.class,
                            ResourcePoolsBuilder.newResourcePoolsBuilder()
                                    .with(ClusteredRestartableResourcePoolBuilder
                                            .clusteredRestartableDedicated("primary-server-resource", 4, MemoryUnit.MB))));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    final Cache<Long, String> cache = cacheManager.getCache("small-cache", Long.class, String.class);

    Random random = new Random();
    for (int i = 0 ; i < 100; i++) {
      cache.put((long) i, new BigInteger(10 * 1024 * 128 * (1 + random.nextInt(10)), random).toString(16));
    }

    cacheManager.close();
  }
}
