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

import java.io.File;
import java.net.URI;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ClusterToolConfigureIT extends BaseClusterToolActiveTest {
  @Test
  public void testConfigureTwice() throws Exception {
    exit.expectSystemExitWithStatus(StatusCode.ALREADY_CONFIGURED.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("configured already")));

    String[] args = configureCluster(getConnectionURI());
    ClusterTool.main(args);
  }

  @Test
  public void testConfigure_ConfigMismatch() throws Exception {
    File generatedTcConfig = generateTCConfig(getConnectionURI().getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT));
    exit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Mismatched off-heap")));

    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
  }

  @Test
  public void testConfigure_DuplicateStripes() throws Exception {
    URI connectionURI = getConnectionURI();
    File stripeConfig_1 = generateTCConfig(connectionURI.getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT));
    File stripeConfig_2 = generateTCConfig(connectionURI.getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT));

    exit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Duplicate stripe when creating cluster")));

    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), stripeConfig_1.getAbsolutePath(), stripeConfig_2.getAbsolutePath()};
    ClusterTool.main(args);
  }

  @Test
  public void testConfigureByServers() {
    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", "localhost:" + getConnectionURI().getPort()};
    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testConfigure_configTypesMismatch() throws Exception {
    File generatedTcConfig = generateTCConfig(this.getClass().getResource("/tc-config-added-config-type.xml").toURI(),
        getConnectionURI().getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT));
    exit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Mismatched config types")));

    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
  }
}
