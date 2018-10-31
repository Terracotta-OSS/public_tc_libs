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

package com.terracottatech.ehcache.clustered.frs;

import org.junit.Before;
import org.junit.ClassRule;

import com.terracottatech.testing.rules.EnterpriseCluster;

import java.net.URI;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;

/**
 * Created by Saurabh Agarwal
 */
public abstract class BaseActivePassiveClusteredTest extends AbstractClusteredTest {
  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(2).withPlugins(RESOURCE_CONFIG).build();

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  void shutdownActiveAndWaitForPassiveToBecomeActive() throws Exception {
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
  }

  void restartServer() throws Exception {
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForActive();
  }

  @Override
  protected URI getURI() {
    return CLUSTER.getConnectionURI();
  }
}