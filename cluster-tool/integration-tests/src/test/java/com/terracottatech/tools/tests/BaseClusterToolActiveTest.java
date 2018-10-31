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
import com.terracottatech.tools.client.TopologyEntityProvider;
import com.terracottatech.tools.clustertool.ClusterTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
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
import static com.terracottatech.utilities.InetSocketAddressConvertor.getInetSocketAddresses;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class BaseClusterToolActiveTest extends AbstractClusterToolTest {
  static final int CONFIG_OFFHEAP_SIZE = 64;
  static final String CONFIG_OFFHEAP_UNIT = "MB";
  static final String DATA_ROOT_DIR_NAME = "root";
  private final TopologyEntityProvider topologyEntityProvider = new TopologyEntityProvider();
  private static final String RESOURCE_CONFIG = "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>\n"
      + "<ohr:offheap-resources>\n"
      + "<ohr:resource name=\"" + RESOURCE_NAME + "\" unit=\"" + CONFIG_OFFHEAP_UNIT + "\">" + CONFIG_OFFHEAP_SIZE + "</ohr:resource>\n"
      + "</ohr:offheap-resources>\n"
      + "</config>\n"
      + "<config>"
      + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
      + "<data:directory name=\"" + DATA_ROOT_DIR_NAME + "\">../data</data:directory>\n"
      + "</data:data-directories>\n"
      + "</config>\n";

  @Rule
  public final EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build(false);

  @Before
  public void setUp() throws Exception {
    PROPERTIES.put(ConnectionPropertyNames.CONNECTION_NAME, "TOPOLOGY");
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    System.setProperty("com.terracottatech.tools.clustertool.timeout", TIMEOUT);
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
    System.clearProperty("com.terracottatech.tools.clustertool.timeout");
  }

  File generateTCConfig(int port, String offHeapUnit, String offHeapSize) throws IOException, URISyntaxException {
    return generateTCConfig(this.getClass().getResource("/tc-config.xml").toURI(), port, offHeapUnit, offHeapSize);
  }

  File generateTCConfig(URI configUri, int port, String offHeapUnit, String offHeapSize) throws IOException {
    Path path = Paths.get(configUri);
    String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    content = content.replaceAll("TSA_PORT_TO_BE_REPLACED", port + "");
    content = content.replaceAll("OFF_HEAP_UNIT", offHeapUnit);
    content = content.replaceAll("OFF_HEAP_SIZE", offHeapSize);

    Path write = Files.write(Paths.get(path.toString() + Time.absoluteTime()), content.getBytes(StandardCharsets.UTF_8));
    return write.toFile();
  }

  String[] configureCluster(URI connectionURI) throws IOException, URISyntaxException {
    File generatedTcConfig = generateTCConfig(connectionURI.getPort(), CONFIG_OFFHEAP_UNIT, Integer.toString(CONFIG_OFFHEAP_SIZE));
    String[] args = {"configure", "-n", CLUSTER_NAME, "-l", getLicensePath(), generatedTcConfig.getAbsolutePath()};
    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
    systemOutRule.clearLog();
    return args;
  }

  URI getConnectionURI() {
    return CLUSTER.getConnectionURI();
  }

  Connection connect() throws ConnectionException {
    // use a stripe:// URI as the topology entity isn't meant to be used from a multi-stripe connection
    return ConnectionFactory.connect(CLUSTER.getStripeConnectionURI(0), PROPERTIES);
  }

  TopologyEntityProvider.ConnectionCloseableTopologyEntity getCloseableTopologyEntity() {
    return topologyEntityProvider.getEntity(getInetSocketAddresses(CLUSTER.getClusterHostPorts()), null);
  }
}
