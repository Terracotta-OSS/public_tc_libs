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
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.CONFIGURATION_UPDATE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.LICENSE_INSTALLATION_MESSAGE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class ClusterToolConfigureAPIT extends BaseClusterToolActivePassiveTest {
  @Test
  public void testConfigureByServer() {
    String[] servers = getServers();
    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", servers[0]};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(LICENSE_INSTALLATION_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(CONFIGURATION_UPDATE_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testConfigureByServers() {
    String[] servers = getServers();
    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", servers[0], "-s", servers[1]};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(LICENSE_INSTALLATION_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(CONFIGURATION_UPDATE_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testConfigureByConfig_activeTerminated() throws Exception {
    terminateActiveAndWaitForPassiveToBecomeActive();

    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generateTCConfig().getAbsolutePath()};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString(LICENSE_INSTALLATION_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(CONFIGURATION_UPDATE_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testConfigureByConfig_passiveTerminated() throws Exception {
    terminatePassive();

    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generateTCConfig().getAbsolutePath()};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("WARNING: Found \"localhost\" as the hostname in Server"));
    assertThat(systemOutRule.getLog(), containsString(LICENSE_INSTALLATION_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(CONFIGURATION_UPDATE_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testConfigureByServers_terminatedServer() throws Exception {
    terminatePassive();
    String[] servers = getServers();
    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", servers[0], "-s", servers[1]};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("WARNING: Found \"localhost\" as the hostname in Server"));
    assertThat(systemOutRule.getLog(), containsString(LICENSE_INSTALLATION_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(CONFIGURATION_UPDATE_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }
}
