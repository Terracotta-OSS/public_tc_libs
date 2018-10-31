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
package com.terracottatech.tools.tests;

import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityConfigurationException;

import com.tc.util.Conversion;
import com.terracottatech.tools.client.TopologyEntity;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;

import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.tools.client.TopologyEntityProvider.ConnectionCloseableTopologyEntity;
import static com.terracottatech.tools.config.Stripe.ConfigType.DATA_DIRECTORIES;
import static com.terracottatech.tools.config.Stripe.ConfigType.FAILOVER_PRIORITY;
import static com.terracottatech.tools.config.Stripe.ConfigType.OFFHEAP;
import static com.terracottatech.tools.config.extractors.FailoverPriorityConfigExtractor.AVAILABILITY_ELEMENT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TopologyEntityLifeCycleIT extends BaseClusterToolActiveTest {
  @Test
  public void testIfDefaultTopologyEntityExistsOnStart() throws Exception {
    Connection connection = connect();
    EntityRef<TopologyEntity, ClusterConfiguration, Void> ref = connection.getEntityRef(TopologyEntity.class, 1, "topology-entity");
    try {
      assertThat(ref.fetchEntity(null), notNullValue());
    } finally {
      connection.close();
    }
  }

  @Test
  public void testDefaultConfig() throws Exception {
    try (ConnectionCloseableTopologyEntity entity = getCloseableTopologyEntity()) {
      TopologyEntity topologyEntity = entity.getTopologyEntity();
      assertThat(topologyEntity.getClusterConfiguration().getClusterName(), is(nullValue()));
    }
  }

  @Test
  public void testReconfigureTopologyEntity_ok() throws Exception {
    EntityRef<TopologyEntity, ClusterConfiguration, Void> ref;
    List<Stripe> stripesConfigs = new ArrayList<>();
    List<Server> servers = new ArrayList<>();

    try (Connection connection = connect()) {
      ref = connection.getEntityRef(TopologyEntity.class, 1, "topology-entity");
      Server server = new Server("myServer", "localhost", 9410);
      servers.add(server);
      Stripe.Config<Long> config = new Stripe.Config<>(OFFHEAP, RESOURCE_NAME,
          Conversion.MemorySizeUnits.MEGA.asBytes() * CONFIG_OFFHEAP_SIZE);
      Stripe.Config<String> fotConfig = new Stripe.Config<>(FAILOVER_PRIORITY, AVAILABILITY_ELEMENT, "-1");
      stripesConfigs.add(new Stripe(servers, fotConfig, config,
          new Stripe.Config<>(DATA_DIRECTORIES, "root", "../data"),
          new Stripe.Config<>(DATA_DIRECTORIES, "use-for-platform", "true")));
      ref.reconfigure(new ClusterConfiguration("NewCluster", stripesConfigs));

      ClusterConfiguration configuration = ref.fetchEntity(null).getClusterConfiguration();
      assertThat(configuration.getClusterName(), is("NewCluster"));
      assertThat(configuration.getCluster().getStripes().size(), equalTo(1));
      assertThat(configuration.getCluster().getStripes().get(0).getServers().size(), equalTo(1));
      assertThat(configuration.getCluster().getStripes().get(0).getServers().get(0), equalTo(server));
    }
  }

  @Test
  public void testReconfigureTopologyEntity_mismatch() throws Exception {
    EntityRef<TopologyEntity, ClusterConfiguration, Void> ref;
    List<Stripe> stripeConfigs = new ArrayList<>();
    List<Server> servers = new ArrayList<>();

    try (Connection connection = connect()) {
      ref = connection.getEntityRef(TopologyEntity.class, 1, "topology-entity");
      servers.add(new Server("myServer", "localhost", 9410));
      stripeConfigs.add(new Stripe(servers));
      ref.reconfigure(new ClusterConfiguration("NewCluster", stripeConfigs));
    } catch (EntityConfigurationException e) {
      assertThat(e.getMessage(), containsString("Mismatched config types"));
    }
  }
}
