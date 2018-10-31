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

import com.terracottatech.testing.rules.EnterpriseCluster;
import com.terracottatech.tools.clustertool.ClusterTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.statistics.Time;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.CONFLICT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ClusterToolReconfigureMultistripeIT extends AbstractClusterToolTest {
  private static final int CONFIG_OFFHEAP_SIZE = 64;
  private static final String CONFIG_OFFHEAP_UNIT = "MB";
  private static final String RESOURCE_CONFIG = "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>\n"
      + "<ohr:offheap-resources>\n"
      + "<ohr:resource name=\"" + RESOURCE_NAME + "\" unit=\"" + CONFIG_OFFHEAP_UNIT + "\">" + CONFIG_OFFHEAP_SIZE + "</ohr:resource>\n"
      + "</ohr:offheap-resources>\n"
      + "</config>\n"
      + "<config>\n"
      + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
      + "<data:directory name=\"root\">../data</data:directory>\n"
      + "</data:data-directories>\n"
      + "</config>\n";

  @Rule
  public final EnterpriseCluster CLUSTER = newCluster(1, 1).withPlugins(RESOURCE_CONFIG).build(false);

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    System.setProperty("com.terracottatech.tools.clustertool.timeout", TIMEOUT);
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
    System.clearProperty("com.terracottatech.tools.clustertool.timeout");
  }

  @Test
  public void testReconfigureStripeOrderMatch() throws Exception {
    String[] configureArgs = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generateTCConfig1(CONFIG_OFFHEAP_SIZE).getAbsolutePath(), generateTCConfig2(CONFIG_OFFHEAP_SIZE).getAbsolutePath()};
    ClusterTool.main(configureArgs);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));

    String[] reconfigureArgs = {"reconfigure", "-n", CLUSTER_NAME, generateTCConfig1(CONFIG_OFFHEAP_SIZE * 2).getAbsolutePath(), generateTCConfig2(CONFIG_OFFHEAP_SIZE * 2).getAbsolutePath()};
    ClusterTool.main(reconfigureArgs);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testReconfigureStripeOrderMismatch() throws Exception {
    String[] configureArgs = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generateTCConfig1(CONFIG_OFFHEAP_SIZE).getAbsolutePath(), generateTCConfig2(CONFIG_OFFHEAP_SIZE).getAbsolutePath()};
    ClusterTool.main(configureArgs);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));

    String[] reconfigureArgs = {"reconfigure", "-n", CLUSTER_NAME, generateTCConfig2(CONFIG_OFFHEAP_SIZE * 2).getAbsolutePath(), generateTCConfig1(CONFIG_OFFHEAP_SIZE * 2).getAbsolutePath()};
    exit.expectSystemExitWithStatus(CONFLICT.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Mismatched stripes")));
    ClusterTool.main(reconfigureArgs);
  }

  private File generateTCConfig1(int offheapSize) throws IOException, URISyntaxException {
    URI uri = this.getClass().getResource("/tc-config.xml").toURI();
    Path path = Paths.get(uri);
    String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    String server = getServer(0);
    content = content.replace("TSA_PORT_TO_BE_REPLACED", server.substring(server.indexOf(":") + 1));
    content = content.replaceAll("OFF_HEAP_UNIT", CONFIG_OFFHEAP_UNIT);
    content = content.replaceAll("OFF_HEAP_SIZE", Integer.toString(offheapSize));

    Path write = Files.write(Paths.get(path.toString() + Time.absoluteTime()), content.getBytes(StandardCharsets.UTF_8));
    return write.toFile();
  }

  private File generateTCConfig2(int offheapSize) throws IOException, URISyntaxException {
    URI uri = this.getClass().getResource("/tc-config-2.xml").toURI();
    Path path = Paths.get(uri);
    String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    String server = getServer(1);
    content = content.replace("TSA_PORT_TO_BE_REPLACED", server.substring(server.indexOf(":") + 1));
    content = content.replaceAll("OFF_HEAP_UNIT", CONFIG_OFFHEAP_UNIT);
    content = content.replaceAll("OFF_HEAP_SIZE", Integer.toString(offheapSize));

    Path write = Files.write(Paths.get(path.toString() + Time.absoluteTime()), content.getBytes(StandardCharsets.UTF_8));
    return write.toFile();
  }

  private String getServer(int stripeIndex) {
    return CLUSTER.getStripeConnectionURI(stripeIndex).getAuthority();
  }
}
