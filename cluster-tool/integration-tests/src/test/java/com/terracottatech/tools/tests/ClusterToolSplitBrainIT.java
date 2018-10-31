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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.testing.rules.EnterpriseExternalCluster;
import com.terracottatech.testing.rules.EnterpriseCluster;
import com.terracottatech.tools.clustertool.ClusterTool;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.BAD_GATEWAY;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.PARTIAL_FAILURE;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertThat;

public class ClusterToolSplitBrainIT extends AbstractClusterToolTest {

  private static final int CONFIG_OFFHEAP_SIZE = 64;
  private static final String CONFIG_OFFHEAP_UNIT = "MB";
  private static final String PASSIVE_SUSPENDED = "PASSIVE_SUSPENDED";
  private static final String UNREACHABLE = "UNREACHABLE";
  private static final String ACTIVE_SUSPENDED = "ACTIVE_SUSPENDED";

  private static String resourceConfig;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final EnterpriseCluster CLUSTER = newCluster(2).withPlugins(resourceConfig)
      .withClientReconnectWindowTimeInSeconds(15).withFailoverPriorityVoterCount(1).build(false);

  @BeforeClass
  public static void beforeClass() {
    resourceConfig = "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>\n"
                     + "<ohr:offheap-resources>\n"
                     + "<ohr:resource name=\"" + RESOURCE_NAME + "\" unit=\"" + CONFIG_OFFHEAP_UNIT + "\">" + CONFIG_OFFHEAP_SIZE + "</ohr:resource>\n"
                     + "</ohr:offheap-resources>\n"
                     + "</config>\n"
                     + "<config>\n"
                     + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
                     + "<data:directory name=\"root\">../data</data:directory>\n"
                     + "</data:data-directories>\n"
                     + "</config>\n";
  }

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    // Open and directly close a connection to make sure cluster is really live - see TDB-2280
    CLUSTER.newConnection().close();
    System.setProperty("com.terracottatech.tools.clustertool.timeout", TIMEOUT);
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
    System.clearProperty("com.terracottatech.tools.clustertool.timeout");
  }

  @Test
  public void testStatusForActiveSuspendedState() throws Exception {
    terminatePassive();

    //Clear the log so that we don't get any false positives
    systemOutRule.clearLog();

    String[] servers = getServers();
    String[] args = { "-t", SOME_SERVERS_DEAD_TIMEOUT, "status", "-s", servers[0], "-s", servers[1] };

    exit.expectSystemExitWithStatus(PARTIAL_FAILURE.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), allOf(containsString(servers[0]), containsString(servers[1]),
            containsString(ACTIVE_SUSPENDED), containsString(UNREACHABLE))));
    ClusterTool.main(args);
  }

  @Test
  public void testStatusForPassiveSuspendedState() throws Exception {
    terminateActive();

    System.setProperty("com.terracottatech.tools.clustertool.timeout", SOME_SERVERS_DEAD_TIMEOUT);
    waitedAssert(
        showServerStatusAndGetLog(getServers()),
        allOf(
            containsString(getServers()[0]), containsString(getServers()[1]),
            containsString(UNREACHABLE), containsString(PASSIVE_SUSPENDED)
        )
    );
  }

  @Test
  public void testConnectionRejectionForClusterLevelStatusCommand() throws Exception {
    configureCluster();
    terminateActive();
    String[] servers = getServers();
    String[] args = { "-t", ALL_SERVERS_DEAD_TIMEOUT, "status", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1] };
    exit.expectSystemExitWithStatus(BAD_GATEWAY.getCode());
    ClusterTool.main(args);
  }

  @Test
  public void testClusterStop() {
    configureCluster();

    //See TDB-3782
    ((EnterpriseExternalCluster) CLUSTER).ignoreServerCrashes(true);

    String[] servers = getServers();
    String[] args = new String[]{ "stop", "-n", CLUSTER_NAME, "-s", servers[0] };
    ClusterTool.main(args);

    System.setProperty("com.terracottatech.tools.clustertool.timeout", ALL_SERVERS_DEAD_TIMEOUT);
    waitedAssert(
        showServerStatusAndGetLog(getServers()),
        allOf(
            containsString(getServers()[0]), containsString(getServers()[1]),
            not(containsString("ACTIVE")), not(containsString("PASSIVE")),
            not(containsString(ACTIVE_SUSPENDED)), not(containsString(PASSIVE_SUSPENDED)),
            matchesPattern("([\\s\\S]*)" + UNREACHABLE + "([\\s\\S]*)" + UNREACHABLE + "([\\s\\S]*)")
        )
    );
  }

  private void terminateActive() throws Exception {
    //Just terminate the Active without waiting for Passive to become Active since we want to test the stuck states
    CLUSTER.getClusterControl().terminateActive();
  }

  private void terminatePassive() throws Exception {
    CLUSTER.getClusterControl().terminateOnePassive();
  }

  private void configureCluster() {
    String[] servers = getServers();
    String[] args = { "configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", servers[0], "-s", servers[1] };
    ClusterTool.main(args);
  }

  private String[] getServers() {
    return CLUSTER.getConnectionURI().getAuthority().split(",");
  }
}