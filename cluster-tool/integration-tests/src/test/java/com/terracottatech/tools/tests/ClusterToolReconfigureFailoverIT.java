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

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.PARTIAL_FAILURE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ClusterToolReconfigureFailoverIT extends BaseClusterToolActivePassiveTest {
  @Test
  public void testReconfigureFollowedByFailover() throws Exception {
    configureCluster();
    String[] args = {"reconfigure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generateTCConfig(CONFIG_OFFHEAP_SIZE * 2).getAbsolutePath()};
    new ClusterTool().start(args);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));

    terminateActiveAndWaitForPassiveToBecomeActive();
    systemOutRule.clearLog();

    args = new String[]{"-t", SOME_SERVERS_DEAD_TIMEOUT, "status", getServers()[0], getServers()[1]};
    exit.expectSystemExitWithStatus(PARTIAL_FAILURE.getCode());//PARTIAL_FAILURE owing to the UNREACHABLE status of Passive
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("ACTIVE")));
    ClusterTool.main(args);
  }
}
