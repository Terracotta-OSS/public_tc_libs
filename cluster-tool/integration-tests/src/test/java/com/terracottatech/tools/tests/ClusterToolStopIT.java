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

import com.terracottatech.testing.rules.EnterpriseExternalCluster;
import com.terracottatech.tools.clustertool.ClusterTool;
import com.terracottatech.tools.clustertool.managers.StatusManager;

import org.junit.Test;

import java.net.URI;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_FAILURE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClusterToolStopIT extends BaseClusterToolActiveTest {
  @Test
  public void testStopServer_serverOption_ok() {
    String[] args = {"stop", "-s", "localhost:" + getConnectionURI().getPort()};

    //See TDB-3782
    ((EnterpriseExternalCluster) CLUSTER).ignoreServerCrashes(true);

    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));

    //Verify that all the servers have stopped
    System.setProperty("com.terracottatech.tools.clustertool.timeout", ALL_SERVERS_DEAD_TIMEOUT);
    waitedAssert(() -> new StatusManager().getServerStatus(getServers()[0]), is("UNREACHABLE"));
  }

  @Test
  public void testStopServer_wrong_host() {
    exit.expectSystemExitWithStatus(StatusCode.FAILURE.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString(COMMAND_FAILURE_MESSAGE)));

    String[] args = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "stop", "wrong-host:" + getConnectionURI().getPort()};
    ClusterTool.main(args);
  }

  @Test
  public void testStopCluster_ok() throws Exception {
    URI connectionURI = getConnectionURI();
    configureCluster(connectionURI);

    //See TDB-3782
    ((EnterpriseExternalCluster) CLUSTER).ignoreServerCrashes(true);

    String[] args = {"stop", "-n", CLUSTER_NAME, "localhost:" + connectionURI.getPort()};
    ClusterTool.main(args);

    //Verify that all the servers have stopped
    System.setProperty("com.terracottatech.tools.clustertool.timeout", ALL_SERVERS_DEAD_TIMEOUT);
    waitedAssert(() -> new StatusManager().getServerStatus(getServers()[0]), is("UNREACHABLE"));
  }

  @Test
  public void testStopCluster_without_configure() {
    exit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Cluster has not been configured")));

    String[] args = {"stop", "-n", CLUSTER_NAME, "localhost:" + getConnectionURI().getPort()};
    ClusterTool.main(args);
  }

  @Test
  public void testStopCluster_wrong_host() throws Exception {
    URI connectionURI = getConnectionURI();
    configureCluster(connectionURI);

    String[] args = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "stop", "-n", CLUSTER_NAME, "wrong-host:" + connectionURI.getPort()};
    exit.expectSystemExitWithStatus(StatusCode.BAD_GATEWAY.getCode());
    ClusterTool.main(args);
  }

  private String[] getServers() {
    return CLUSTER.getConnectionURI().getAuthority().split(",");
  }
}
