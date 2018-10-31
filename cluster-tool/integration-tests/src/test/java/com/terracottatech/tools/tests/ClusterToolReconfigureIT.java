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
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.CONFIGURATION_UPDATE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.LICENSE_NOT_UPDATED_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.BAD_REQUEST;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.CONFLICT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class ClusterToolReconfigureIT extends BaseClusterToolActiveTest {
  @Test
  public void testReconfigure_without_configure() throws Exception {
    File generatedTcConfig = generateTCConfig(getConnectionURI().getPort(), CONFIG_OFFHEAP_UNIT, Integer.toString(CONFIG_OFFHEAP_SIZE));

    exit.expectSystemExitWithStatus(BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("No license found")));

    String[] args = {"reconfigure", "-n", CLUSTER_NAME, generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_wrong_clusterName() throws Exception {
    String[] returnedArgs = configureCluster(getConnectionURI());
    exit.expectSystemExitWithStatus(CONFLICT.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Mismatch found in cluster names")));

    String[] args = {"reconfigure", "-n", "wrong_cluster", returnedArgs[returnedArgs.length - 1]};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_offHeapMismatch() throws Exception {
    configureCluster(getConnectionURI());
    exit.expectSystemExitWithStatus(CONFLICT.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Mismatched off-heap")));

    File generatedTcConfig = generateTCConfig(getConnectionURI().getPort(), CONFIG_OFFHEAP_UNIT, Integer.toString(CONFIG_OFFHEAP_SIZE / 2));
    String[] args = {"reconfigure", "-n", "cluster", generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_licenseViolation() throws Exception {
    configureCluster(getConnectionURI());
    exit.expectSystemExitWithStatus(BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Cluster offheap resource is not within the limit of the license")));

    File generatedTcConfig = generateTCConfig(getConnectionURI().getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT * 2));
    String[] args = {"reconfigure", "-n", "cluster", generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_ok_largerOffHeapResource() throws Exception {
    configureCluster(getConnectionURI());

    File generatedTcConfig = generateTCConfig(getConnectionURI().getPort(), CONFIG_OFFHEAP_UNIT, Integer.toString(CONFIG_OFFHEAP_SIZE * 2));
    String[] args = {"reconfigure", "-n", "cluster", generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("WARNING: Found \"localhost\" as the hostname in Server"));
    assertThat(systemOutRule.getLog(), containsString(CONFIGURATION_UPDATE_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReconfigure_updateLicenseWithoutConfigure() {
    exit.expectSystemExitWithStatus(BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Cluster has not been configured")));

    String[] args = new String[]{"reconfigure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", "localhost:" + getConnectionURI().getPort()};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_updateLicenseAlone() throws Exception {
    URI connectionURI = getConnectionURI();
    configureCluster(connectionURI);

    String[] args = new String[]{"reconfigure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", "localhost:" + connectionURI.getPort()};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), not(containsString("WARNING: Found \"localhost\" as the hostname in server")));
    assertThat(systemOutRule.getLog(), containsString(LICENSE_NOT_UPDATED_MESSAGE));
    assertThat(systemOutRule.getLog(), not(containsString(CONFIGURATION_UPDATE_MESSAGE)));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReconfigure_updateLicenseAndConfigurationTogether() throws Exception {
    URI connectionURI = getConnectionURI();
    configureCluster(connectionURI);

    File generatedTcConfig = generateTCConfig(connectionURI.getPort(), CONFIG_OFFHEAP_UNIT, Integer.toString(CONFIG_OFFHEAP_SIZE * 2));
    String[] args = new String[]{"reconfigure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("WARNING: Found \"localhost\" as the hostname in Server"));
    assertThat(systemOutRule.getLog(), containsString(LICENSE_NOT_UPDATED_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(CONFIGURATION_UPDATE_MESSAGE));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReconfigure_moreConfigTypes() throws Exception {
    configureCluster(getConnectionURI());
    File generatedTcConfig = generateTCConfig(this.getClass().getResource("/tc-config-added-config-type.xml").toURI(),
        getConnectionURI().getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT));

    String[] args = {"reconfigure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);

    assertThat(systemOutRule.getLog(), containsString("WARNING: Found \"localhost\" as the hostname in Server"));
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReconfigure_moreConfigTypes_2() throws Exception {
    configureCluster(getConnectionURI());
    File generatedTcConfig = generateTCConfig(this.getClass().getResource("/tc-config-added-security-type.xml").toURI(),
        getConnectionURI().getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT));

    String[] args = {"reconfigure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReconfigure_moreInvalidConfigTypes() throws Exception {
    configureCluster(getConnectionURI());
    File generatedTcConfig = generateTCConfig(this.getClass().getResource("/tc-config-added-invalid-config-type.xml").toURI(),
        getConnectionURI().getPort(), LICENSE_OFFHEAP_UNIT, Integer.toString(LICENSE_OFFHEAP_LIMIT));

    exit.expectSystemExitWithStatus(BAD_REQUEST.getCode());
    String[] args = {"reconfigure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
  }
}
