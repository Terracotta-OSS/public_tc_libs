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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
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

public class BaseClusterToolActivePassiveTest extends AbstractClusterToolTest {
  static final int CONFIG_OFFHEAP_SIZE = 64;
  static final String CONFIG_OFFHEAP_UNIT = "MB";

  static Path whitelistFile;
  private static String resourceConfig;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final EnterpriseCluster CLUSTER = newCluster(2).withPlugins(resourceConfig)
      .withClientReconnectWindowTimeInSeconds(15).build(false);

  @BeforeClass
  public static void beforeClass() throws Exception {
    whitelistFile = temporaryFolder.newFile("whitelist.txt").toPath();
    resourceConfig = "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>\n"
                     + "<ohr:offheap-resources>\n"
                     + "<ohr:resource name=\"" + RESOURCE_NAME + "\" unit=\"" + CONFIG_OFFHEAP_UNIT + "\">" + CONFIG_OFFHEAP_SIZE + "</ohr:resource>\n"
                     + "</ohr:offheap-resources>\n"
                     + "</config>\n"
                     + "<config>\n"
                     + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
                     + "<data:directory name=\"root\">../data</data:directory>\n"
                     + "</data:data-directories>\n"
                     + "</config>\n"
                     + "<service xmlns:security='http://www.terracottatech.com/config/security'>\n"
                     + "<security:security>\n"
                     + "<security:white-list path=\"" + whitelistFile + "\"/>\n"
                     + "</security:security>\n"
                     + "</service>\n";
  }

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    // Open and directly close a connection to make sure cluster is really live - see TDB-2280
    CLUSTER.newConnection().close();
    System.setProperty("com.terracottatech.tools.clustertool.timeout", TIMEOUT);
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
    System.clearProperty("com.terracottatech.tools.clustertool.timeout");
  }

  void terminateActiveAndWaitForPassiveToBecomeActive() throws Exception {
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
  }

  void terminatePassive() throws Exception {
    CLUSTER.getClusterControl().terminateOnePassive();
  }

  void terminateAllServers() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
  }

  void configureCluster() {
    String[] servers = getServers();
    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), "-s", servers[0], "-s", servers[1]};
    ClusterTool.main(args);
  }

  File generateTCConfig() throws IOException, URISyntaxException {
    return generateTCConfig(CONFIG_OFFHEAP_SIZE);
  }

  File generateTCConfig(int offHeapSize) throws IOException, URISyntaxException {
    URI uri = this.getClass().getResource("/tc-config-AP.xml").toURI();
    Path path = Paths.get(uri);
    String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    content = content.replace("WHITELIST_FILE_PATH", whitelistFile.toString());
    content = content.replace("TSA_PORT_TO_BE_REPLACED1", getServers()[0].substring(getServers()[0].indexOf(":") + 1));
    content = content.replace("TSA_PORT_TO_BE_REPLACED2", getServers()[1].substring(getServers()[1].indexOf(":") + 1));
    content = content.replaceAll("OFF_HEAP_UNIT", CONFIG_OFFHEAP_UNIT);
    content = content.replaceAll("OFF_HEAP_SIZE", offHeapSize + "");

    Path write = Files.write(Paths.get(path.toString() + Time.absoluteTime()), content.getBytes(StandardCharsets.UTF_8));
    return write.toFile();
  }

  String[] getServers() {
    return CLUSTER.getConnectionURI().getAuthority().split(",");
  }
}
