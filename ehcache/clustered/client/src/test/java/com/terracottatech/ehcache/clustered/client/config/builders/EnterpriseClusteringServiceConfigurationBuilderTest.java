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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.junit.Test;

import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;

import java.net.URI;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static com.terracottatech.ehcache.clustered.common.RestartConfiguration.DEFAULT_CONTAINER_NAME;
import static com.terracottatech.ehcache.clustered.common.RestartConfiguration.DEFAULT_DATA_LOG_NAME;
import static com.terracottatech.ehcache.clustered.common.RestartConfiguration.DEFAULT_DATA_LOG_NAME_HYBRID;
import static com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode.PARTIAL;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * tests enterprise config API builders {@link EnterpriseClusteringServiceConfigurationBuilder} and
 * {@link EnterpriseServerSideConfigurationBuilder}
 */
public class EnterpriseClusteringServiceConfigurationBuilderTest {
  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");

  @Test
  public void testWithRestartable() throws Exception {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).autoCreate().restartable("root1").build();
    assertThat(csc.getClusterUri(), is(CLUSTER_URI));
    assertThat(csc.getServerConfiguration(), instanceOf(EnterpriseServerSideConfiguration.class));
    final RestartConfiguration restartConfig = ((EnterpriseServerSideConfiguration)csc.getServerConfiguration()).getRestartConfiguration();
    assertNull(restartConfig.getFrsIdentifier());
    assertThat(restartConfig.getRestartableLogContainer(), is(DEFAULT_CONTAINER_NAME));
    assertThat(restartConfig.getRestartableLogName(), is(DEFAULT_DATA_LOG_NAME));
    assertThat(restartConfig.getRestartableLogRoot(), is("root1"));
    assertThat(restartConfig.isHybrid(), is(false));
  }

  @Test
  public void testWithRestartableAndFrsIdentifier() throws Exception {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).autoCreate().restartable("root1").withRestartIdentifier("frsmgr1").build();
    assertThat(csc.getClusterUri(), is(CLUSTER_URI));
    assertThat(csc.getServerConfiguration(), instanceOf(EnterpriseServerSideConfiguration.class));
    final RestartConfiguration restartConfig = ((EnterpriseServerSideConfiguration)csc.getServerConfiguration()).getRestartConfiguration();
    assertThat(restartConfig.getFrsIdentifier(), is("frsmgr1"));
    assertThat(restartConfig.getRestartableLogContainer(), is(DEFAULT_CONTAINER_NAME));
    assertThat(restartConfig.getRestartableLogName(), is(DEFAULT_DATA_LOG_NAME));
    assertThat(restartConfig.getRestartableLogRoot(), is("root1"));
    assertThat(restartConfig.isHybrid(), is(false));
  }

  @Test
  public void testWithHybridRestartable() throws Exception {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).expecting()
        .restartable("root1").withRestartableOffHeapMode(PARTIAL).build();
    assertThat(csc.getClusterUri(), is(CLUSTER_URI));
    assertThat(csc.getServerConfiguration(), instanceOf(EnterpriseServerSideConfiguration.class));
    final RestartConfiguration restartConfig = ((EnterpriseServerSideConfiguration)csc
        .getServerConfiguration())
        .getRestartConfiguration();
    assertNull(restartConfig.getFrsIdentifier());
    assertThat(restartConfig.getRestartableLogContainer(), is(DEFAULT_CONTAINER_NAME));
    assertThat(restartConfig.getRestartableLogName(), is(DEFAULT_DATA_LOG_NAME_HYBRID));
    assertThat(restartConfig.getRestartableLogRoot(), is("root1"));
    assertThat(restartConfig.isHybrid(), is(true));
  }

  @Test
  public void testWithNonRestartable() throws Exception {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).autoCreate().build();
    assertThat(csc.getClusterUri(), is(CLUSTER_URI));
    assertThat(csc.getServerConfiguration().getClass().getName(), is(ServerSideConfiguration.class.getName()));
  }
}