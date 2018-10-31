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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;

import java.net.URI;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * ConfigurationSplitterTest
 */
public class ConfigurationSplitterTest {

  @Test
  public void testSplitsEnterpriseServerSideConfiguration() throws Exception {
    ClusteringServiceConfiguration clusteringServiceConfiguration = enterpriseCluster(URI.create("to://whatever")).autoCreate()
        .restartable("root")
        .resourcePool("defaultPool", 100, MemoryUnit.KB, "resource")
        .build();
    ServerSideConfiguration serverConfiguration = clusteringServiceConfiguration.getServerConfiguration();

    ConfigurationSplitter splitter = new ConfigurationSplitter();
    ServerSideConfiguration splitConfiguration = splitter.splitServerSideConfiguration(serverConfiguration, 2);

    assertThat(splitConfiguration, instanceOf(EnterpriseServerSideConfiguration.class));
    EnterpriseServerSideConfiguration eeSplitConfig = (EnterpriseServerSideConfiguration) splitConfiguration;
    assertThat(eeSplitConfig.getRestartConfiguration().getRestartableLogRoot(), is("root"));
    assertThat(eeSplitConfig.getRestartConfiguration().getOffHeapMode(), is(RestartableOffHeapMode.FULL));
    assertThat(eeSplitConfig.getResourcePools().get("defaultPool").getSize(), is(MemoryUnit.KB.toBytes(50)));
  }

  @Test
  public void testSplitsServerSideConfiguration() throws Exception {
    ClusteringServiceConfiguration clusteringServiceConfiguration = cluster(URI.create("to://whatever")).autoCreate()
        .defaultServerResource("default")
        .resourcePool("pool", 100, MemoryUnit.KB, "resource")
        .build();

    ConfigurationSplitter splitter = new ConfigurationSplitter();
    ServerSideConfiguration splitConfiguration = splitter.splitServerSideConfiguration(clusteringServiceConfiguration
        .getServerConfiguration(), 2);

    assertThat(splitConfiguration.getDefaultServerResource(), is("default"));
  }

  @Test
  public void testHandlesNullServerSideConfiguration() {
    ConfigurationSplitter splitter = new ConfigurationSplitter();
    ServerSideConfiguration splitConfiguration = splitter.splitServerSideConfiguration(null, 4);

    assertThat(splitConfiguration, nullValue());
  }

  @Test
  public void testSplitsServerStoreConfiguration() throws Exception {
    ServerStoreConfiguration configuration = new ServerStoreConfiguration(new PoolAllocation.Dedicated("resource", 10000),
        "keyType", "valueType", "keySerialzierType", "valueSerializerType", Consistency.EVENTUAL);

    ConfigurationSplitter splitter = new ConfigurationSplitter();
    ServerStoreConfiguration splitConfiguration = splitter.splitServerStoreConfiguration(configuration, 4);

    assertThat(splitConfiguration.getPoolAllocation(), instanceOf(PoolAllocation.Dedicated.class));
    PoolAllocation.Dedicated dedicated = (PoolAllocation.Dedicated) splitConfiguration.getPoolAllocation();
    assertThat(dedicated.getSize(), is(2500L));

  }

}