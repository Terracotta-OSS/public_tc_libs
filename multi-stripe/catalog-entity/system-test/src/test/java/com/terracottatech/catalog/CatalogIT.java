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
package com.terracottatech.catalog;

import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockClient;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.lease.connection.LeasedConnectionFactory;
import org.terracotta.passthrough.IClusterControl;

import com.terracottatech.testing.rules.EnterpriseCluster;

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * CatalogIT
 */
public class CatalogIT {

  private static final String RESOURCE_CONFIG = "<config>"
      + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
      + "<data:directory name=\"root\">../data</data:directory>\n"
      + "</data:data-directories>\n"
      + "</config>\n";

  @Rule
  public EnterpriseCluster cluster = newCluster(2).withPlugins(RESOURCE_CONFIG)
      .withClientReconnectWindowTimeInSeconds(10).build();

  @Test
  public void testCatalogSyncThenFailover() throws Exception {
    IClusterControl stripeControl = cluster.getStripeControl(0);
    stripeControl.terminateActive();
    stripeControl.waitForActive();

    URI connectionURI = cluster.getClusterConnectionURI();

    Connection connection = LeasedConnectionFactory.connect(connectionURI, new Properties());
    VoltronReadWriteLock lock = new VoltronReadWriteLock(connection, "lock");
    VoltronReadWriteLock.Hold hold = lock.readLock();
    assertThat(hold, notNullValue());

    EntityRef<SystemCatalog, Object, Object> entityRef = connection.getEntityRef(SystemCatalog.class, SystemCatalog.VERSION, SystemCatalog.ENTITY_NAME);
    SystemCatalog systemCatalog = entityRef.fetchEntity(null);

    stripeControl.startOneServer();
    stripeControl.waitForRunningPassivesInStandby();

    stripeControl.terminateActive();
    stripeControl.waitForActive();

    Map<String, byte[]> actual = systemCatalog.listByType(VoltronReadWriteLockClient.class);
    assertThat(actual, is(not(emptyMap())));
  }
}
