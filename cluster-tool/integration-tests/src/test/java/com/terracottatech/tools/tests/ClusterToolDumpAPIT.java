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

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_FAILURE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ClusterToolDumpAPIT extends BaseClusterToolActivePassiveTest {
  @Test
  public void testMultipleServerDump() {
    String[] servers = getServers();
    String[] dumpArgs = {"dump", "-s", servers[0], "-s", servers[1]};
    ClusterTool.main(dumpArgs);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testMultipleServerDump_allTerminated() throws Exception {
    String[] servers = getServers();
    terminateAllServers();
    String[] dumpArgs = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "dump", "-s", servers[0], "-s", servers[1]};

    exit.expectSystemExitWithStatus(StatusCode.FAILURE.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString(COMMAND_FAILURE_MESSAGE)));
    ClusterTool.main(dumpArgs);
  }

  @Test
  public void testClusterDump_activeTerminated() throws Exception {
    String[] servers = getServers();
    configureCluster();

    terminateActiveAndWaitForPassiveToBecomeActive();

    String[] dumpArgs = {"dump", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};
    ClusterTool.main(dumpArgs);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testClusterDump_passiveTerminated() throws Exception {
    String[] servers = getServers();
    configureCluster();

    terminatePassive();

    String[] dumpArgs = {"dump", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};
    ClusterTool.main(dumpArgs);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testClusterDump_allTerminated() throws Exception {
    String[] servers = getServers();
    configureCluster();

    terminateAllServers();

    String[] dumpArgs = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "dump", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};
    exit.expectSystemExitWithStatus(StatusCode.BAD_GATEWAY.getCode());
    exit.checkAssertionAfterwards(() ->
        assertThat(systemOutRule.getLog(), containsString("Connection refused from server(s)")));
    ClusterTool.main(dumpArgs);
  }

  @Test
  public void testClusterDumpWithoutConfigure() {
    String[] servers = getServers();

    String[] dumpArgs = {"dump", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};
    exit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() ->
        assertThat(systemOutRule.getLog(), containsString("Cluster has not been configured")));
    ClusterTool.main(dumpArgs);
  }
}
