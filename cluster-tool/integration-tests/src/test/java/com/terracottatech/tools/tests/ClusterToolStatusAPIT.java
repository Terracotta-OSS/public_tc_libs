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

import com.terracottatech.tools.clustertool.ClusterTool;
import org.junit.Test;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.BAD_GATEWAY;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.BAD_REQUEST;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.PARTIAL_FAILURE;
import static com.terracottatech.tools.tests.BaseClusterToolActiveTest.DATA_ROOT_DIR_NAME;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ClusterToolStatusAPIT extends BaseClusterToolActivePassiveTest {
  @Test
  public void testStatusForMultipleServers_allUp() {
    String[] servers = getServers();
    String[] args = {"status", "-s", servers[0], "-s", servers[1], "-o", "TABULAR"};

    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), allOf(containsString(servers[0]), containsString(servers[1]),
        containsString("ACTIVE"), containsString("PASSIVE"), containsString("Server is not part of a configured cluster")));
  }

  @Test
  public void testWarningForUnknownServerInStatus() {
    String[] servers = getServers();
    String serverPort = servers[1].split(":")[1];
    String[] args = {"status", "-s", "127.0.0.1:" + serverPort };

    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), containsString("No match found for '127.0.0.1:" + serverPort + "' in known configuration"));
  }

  @Test
  public void testStatusForMultipleServers_allUp_deprecatedStyle() {
    String[] servers = getServers();
    String[] args = {"status", servers[0], servers[1]};

    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), allOf(containsString(servers[0]), containsString(servers[1]),
        containsString("ACTIVE"), containsString("PASSIVE"), containsString("Server is not part of a configured cluster")));
  }

  @Test
  public void testStatusForMultipleServers_someDown() throws Exception {
    String[] servers = getServers();
    String[] args = {"-t", SOME_SERVERS_DEAD_TIMEOUT, "status", servers[0], servers[1]};

    terminatePassive();
    exit.expectSystemExitWithStatus(PARTIAL_FAILURE.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), allOf(containsString(servers[0]),
        containsString(servers[1]), containsString("ACTIVE"), containsString("UNREACHABLE"),
        containsString("Server is not part of a configured cluster"))
    ));
    ClusterTool.main(args);
  }

  @Test
  public void testStatusForCluster() {
    configureCluster();

    String[] servers = getServers();
    String[] args = {"status", "-n", CLUSTER_NAME, "-s", servers[0]};

    ClusterTool.main(args);
    String consoleLog = systemOutRule.getLog();
    assertThat(consoleLog, containsString("Cluster name: " + CLUSTER_NAME));
    assertThat(consoleLog, containsString("Number of stripes: 1"));
    assertThat(consoleLog, containsString("Total number of servers: 2"));
    assertThat(consoleLog, containsString("Total configured offheap: " + CONFIG_OFFHEAP_SIZE + CONFIG_OFFHEAP_UNIT.charAt(0)));
    assertThat(consoleLog, containsString("Backup configured: false"));
    assertThat(consoleLog, containsString("Security configured: white-list"));
    assertThat(consoleLog, containsString("Data directories configured: " + DATA_ROOT_DIR_NAME));
    assertThat(consoleLog, allOf(containsString(servers[0]), containsString(servers[1]),
        containsString("ACTIVE"), containsString("PASSIVE")));
  }

  @Test
  public void testStatusForCluster_withoutConfigure() {
    String[] servers = getServers();
    String[] args = {"status", "-n", CLUSTER_NAME, "-s", servers[0]};

    exit.expectSystemExitWithStatus(BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Cluster has not been configured")));
    ClusterTool.main(args);
  }

  @Test
  public void testStatusForCluster_serverTerminated() throws Exception {
    configureCluster();
    terminateActiveAndWaitForPassiveToBecomeActive();

    String[] servers = getServers();
    String[] args = {"status", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};

    exit.expectSystemExitWithStatus(PARTIAL_FAILURE.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), allOf(containsString(servers[0]),
        containsString(servers[1]), containsString("ACTIVE"), containsString("UNREACHABLE"))));
    ClusterTool.main(args);
  }

  @Test
  public void testStatusForCluster_allServersTerminated() throws Exception {
    configureCluster();
    terminateAllServers();

    String[] servers = getServers();
    String[] args = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "status", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};

    exit.expectSystemExitWithStatus(BAD_GATEWAY.getCode());
    exit.checkAssertionAfterwards(() ->
        assertThat(systemOutRule.getLog(), containsString("Connection refused from server(s)")));
    ClusterTool.main(args);
  }
}
