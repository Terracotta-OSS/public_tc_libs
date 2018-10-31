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

package com.terracottatech.ehcache.clustered.reconnect;

import com.terracottatech.testing.delay.ClusterDelay;
import com.terracottatech.testing.delay.DelayConnectionService;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EnterpriseEhcacheReconnectTest {
  public static final String RESOURCE_CONFIG =
          "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
                  + "<ohr:offheap-resources>"
                  + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "</ohr:offheap-resources>"
                  + "</config>\n"
                  + "<config>"
                  + "<data:data-directories xmlns:data='http://www.terracottatech.com/config/data-roots'>"
                  + "<data:directory name='disk-resource'>disk</data:directory>"
                  + "</data:data-directories>"
                  + "</config>"
                  + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
                  + "<lease:connection-leasing>"
                  + "<lease:lease-length unit='seconds'>5</lease:lease-length>"
                  + "</lease:connection-leasing>"
                  + "</service>";

  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void runTest() throws Exception {
    cacheOpsDuringReconnection((time, delay) -> delay.setDefaultMessageDelay(time));
  }

  protected URI getConnectionURI() {
    return CLUSTER.getConnectionURI();
  }

  protected ClusterDelay getDelay() {
    return new ClusterDelay("cachedelay", 1);
  }

  protected void cacheOpsDuringReconnection(BiConsumer<Long, ClusterDelay> delayConsumer) throws Exception {

    try {
      ClusterDelay cachedelay = getDelay();
      DelayConnectionService.setDelay(cachedelay);

      CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
              = CacheManagerBuilder.newCacheManagerBuilder()
              .with(ClusteringServiceConfigurationBuilder.cluster(getConnectionURI().resolve("/crud-cm"))
                      .autoCreate()
                      .defaultServerResource("primary-server-resource"));
      PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
      cacheManager.init();

      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                      .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
              .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
              .build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);


      delayConsumer.accept(6000L, cachedelay);

      long start = System.currentTimeMillis();
      while ((System.currentTimeMillis() - start) < 20000L) {
        try {
          cache.put(2L, "value");
          Thread.sleep(1000L);
        } catch (InterruptedException ie){
          // ignore
        } catch (Exception e) {
          assertThat(e.getCause().getCause(), instanceOf(ReconnectInProgressException.class));
          break;
        }
      }

      if ((System.currentTimeMillis() - start) >= 20000L)
        fail("Timed-out waiting for disconnect from server!");

      delayConsumer.accept(0L, cachedelay);

      AtomicBoolean done = new AtomicBoolean();

      CompletableFuture<Void> getSucceededFuture = CompletableFuture.runAsync(() -> {
        while (!done.get()) {
          try {
            cache.get(1L);
            break;
          } catch (RuntimeException e) {
            // expected
          }
        }
      });

      try {
        getSucceededFuture.get(20000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        done.set(true);
        throw new AssertionError(e);
      }

    } finally {
      DelayConnectionService.setDelay(null);
    }
  }
}
