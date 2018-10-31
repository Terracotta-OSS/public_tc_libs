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
import com.terracottatech.tools.clustertool.result.ClusterToolCommandResults;
import org.junit.After;
import org.junit.Test;
import org.terracotta.testing.common.PortChooser;

import java.net.URI;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_FAILURE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ClusterToolDumpIT extends BaseClusterToolActiveTest {

  @After
  public void tearDown() {
    System.clearProperty("com.terracottatech.tools.clustertool.timeout");
  }

  @Test
  public void testServerDump_ok() {
    String[] args = {"dump", "localhost:" + getConnectionURI().getPort()};
    ClusterTool.main(args);
    String consoleLog = systemOutRule.getLog();
    assertThat(consoleLog, containsString(COMMAND_SUCCESS_MESSAGE));

    // Assert that dump was taken exactly once
    String dumpMarker = "org.terracotta.dump - [dump]";
    assertThat(consoleLog, containsString(dumpMarker));
    int firstOccurrence = consoleLog.indexOf(dumpMarker);
    int lastOccurrence = consoleLog.lastIndexOf(dumpMarker);
    assertThat(firstOccurrence, equalTo(lastOccurrence));
  }

  @Test
  public void testServerDump_wrong_host() {
    exit.expectSystemExitWithStatus(StatusCode.FAILURE.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString(COMMAND_FAILURE_MESSAGE)));

    String[] args = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "dump", "wrong-host:" + getConnectionURI().getPort()};
    ClusterTool.main(args);
  }

  @Test
  public void testServerDump_wrong_port() {
    exit.expectSystemExitWithStatus(StatusCode.FAILURE.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString(COMMAND_FAILURE_MESSAGE)));

    String[] args = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "dump", "localhost:" + new PortChooser().chooseRandomPort()};
    ClusterTool.main(args);
  }

  @Test
  public void testClusterDump_wrong_cluster_name() throws Exception {
    URI connectionURI = getConnectionURI();
    configureCluster(connectionURI);

    exit.expectSystemExitWithStatus(ClusterToolCommandResults.StatusCode.CONFLICT.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Mismatch found")));

    String[] args = new String[]{"dump", "-n", "wrong_cluster_name", "localhost:" + connectionURI.getPort()};
    ClusterTool.main(args);
  }

  @Test
  public void testClusterDump_ok() throws Exception {
    URI connectionURI = getConnectionURI();
    configureCluster(connectionURI);

    String[] args = new String[]{"dump", "-n", CLUSTER_NAME, "localhost:" + connectionURI.getPort()};
    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testClusterDump_without_configure() {
    exit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Cluster has not been configured")));

    String[] args = new String[]{"dump", "-n", CLUSTER_NAME, "localhost:" + getConnectionURI().getPort()};
    ClusterTool.main(args);
  }

  @Test
  public void testClusterDump_wrong_host() throws Exception {
    URI connectionURI = getConnectionURI();
    configureCluster(connectionURI);

    String[] args = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "dump", "-n", CLUSTER_NAME, "wrong-host:" + connectionURI.getPort()};
    exit.expectSystemExitWithStatus(StatusCode.BAD_GATEWAY.getCode());
    ClusterTool.main(args);
  }
}
