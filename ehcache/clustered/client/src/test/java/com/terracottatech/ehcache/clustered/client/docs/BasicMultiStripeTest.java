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

package com.terracottatech.ehcache.clustered.client.docs;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;

/**
 * BasicMultiStripeTest
 */
public class BasicMultiStripeTest {

  /**
   * 1. Need to start the two servers, using tc-config-stripeN.xml found in resources
   * 2. Configure using cluster tool
   * 3. Run test
   * 4. Profit!
   */
  @Test
  @Ignore("see steps in above comment")
  public void testMultiStripe() throws Exception {
    // tag::multi-stripe-example[]
    PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost:9510/multi-stripe-cm")).autoCreate()) // <1>
        .withCache("myCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10)
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 10, MemoryUnit.MB)))) // <2>
        .build(true);

    Cache<Long, String> myCache = cacheManager.getCache("myCache", Long.class, String.class);

    myCache.put(42L, "Victory!");
    System.out.println(myCache.get(42L));
    // end::multi-stripe-example[]
  }
}
