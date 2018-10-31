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
import com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_FAILURE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_PARTIAL_FAILURE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ClusterToolIPWhitelistReloadAPIT extends BaseClusterToolActivePassiveTest {
  @After
  public void revertWhiteListFile() throws IOException {
    Files.deleteIfExists(whitelistFile);
    whitelistFile = Files.createFile(whitelistFile);
  }

  @Test
  public void testReloadOnMultipleServers_allUp() throws Exception {
    addToWhiteListFile("::2");
    String[] servers = getServers();
    String[] args = {"ipwhitelist-reload", "-s", servers[0], "-s", servers[1]};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(servers[0] + ": IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString(servers[1] + ": IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReloadOnMultipleServers_allUp_deprecatedStyle() throws Exception {
    addToWhiteListFile("2.3.4.5");
    String[] servers = getServers();
    String[] args = {"ipwhitelist-reload", servers[0], servers[1]};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(servers[0] + ": IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString(servers[1] + ": IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReloadOnMultipleServers_someDown() throws Exception {
    terminatePassive();
    addToWhiteListFile("::5");

    String[] servers = getServers();
    String[] args = {"-t", SOME_SERVERS_DEAD_TIMEOUT, "ipwhitelist-reload", "-s", servers[0], "-s", servers[1]};
    exit.expectSystemExitWithStatus(StatusCode.PARTIAL_FAILURE.getCode());
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString("Connection refused"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_PARTIAL_FAILURE_MESSAGE));
  }

  @Test
  public void testReloadOnCluster() throws Exception {
    configureCluster();
    addToWhiteListFile("def:d::ffff");

    String[] servers = getServers();
    String[] args = {"ipwhitelist-reload", "-n", CLUSTER_NAME, "-s", servers[0]};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(servers[0] + ": IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString(servers[1] + ": IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReloadOnCluster_withoutConfigure() throws Exception {
    addToWhiteListFile("15.16.17.18");
    String[] servers = getServers();
    String[] args = {"ipwhitelist-reload", "-n", CLUSTER_NAME, "-s", servers[0]};

    exit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Cluster has not been configured")));
    ClusterTool.main(args);
  }

  @Test
  public void testReloadOnCluster_serverTerminated() throws Exception {
    configureCluster();
    terminatePassive();
    addToWhiteListFile("abc::2");

    String[] servers = getServers();
    String[] args = {"ipwhitelist-reload", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};
    exit.expectSystemExitWithStatus(StatusCode.PARTIAL_FAILURE.getCode());
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("IP whitelist reload request successful"));
    assertThat(systemOutRule.getLog(), containsString("Connection refused"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_PARTIAL_FAILURE_MESSAGE));
  }

  @Test
  public void testReloadOnCluster_allServersTerminated() throws Exception {
    configureCluster();
    terminateAllServers();
    addToWhiteListFile("20.20.20.20");

    String[] servers = getServers();
    String[] args = {"-t", ALL_SERVERS_DEAD_TIMEOUT, "ipwhitelist-reload", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};
    exit.expectSystemExitWithStatus(StatusCode.BAD_GATEWAY.getCode());
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("Connection refused"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_FAILURE_MESSAGE));
  }

  @Test
  public void testReloadOnCluster_deletedWhitelistFile() throws Exception {
    configureCluster();
    deleteWhiteListFile();

    String[] servers = getServers();
    String[] args = {"ipwhitelist-reload", "-n", CLUSTER_NAME, "-s", servers[0], "-s", servers[1]};
    exit.expectSystemExitWithStatus(StatusCode.FAILURE.getCode());
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(servers[0] + ": IP whitelist reload request failed"));
    assertThat(systemOutRule.getLog(), containsString(servers[1] + ": IP whitelist reload request failed"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_FAILURE_MESSAGE));
  }

  @Test
  public void testReloadOnServers_deletedWhitelistFile() throws Exception {
    deleteWhiteListFile();

    String[] servers = getServers();
    String[] args = {"ipwhitelist-reload", "-s", servers[0]};
    exit.expectSystemExitWithStatus(StatusCode.FAILURE.getCode());
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(servers[0] + ": IP whitelist reload request failed"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_FAILURE_MESSAGE));
  }

  private void addToWhiteListFile(String ip) throws IOException {
    Files.write(whitelistFile, ip.concat("\n").getBytes(), StandardOpenOption.APPEND);
  }

  private void deleteWhiteListFile() throws IOException {
    Files.delete(whitelistFile);
  }
}
