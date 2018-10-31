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

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.BAD_REQUEST;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ClusterToolStatusIT extends BaseClusterToolActiveTest {
  private static final String LINE_SEP = System.lineSeparator();

  @Test
  public void testStatusForServer_unknown() {
    String[] args = {"status", "-s", CLUSTER.getClusterHostPorts()[0], "-o", "blah"};

    exit.expectSystemExitWithStatus(BAD_REQUEST.getCode());
    exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Unknown output format")));
    ClusterTool.main(args);
  }

  @Test
  public void testStatusForServer_tabular() {
    String[] args = {"status", "-s", CLUSTER.getClusterHostPorts()[0], "-o", "tabular"};

    ClusterTool.main(args);
    assertThat(systemOutRule.getLog(), allOf(containsString(CLUSTER.getClusterHostPorts()[0]),
        containsString("ACTIVE"), containsString("Server is not part of a configured cluster")));
  }

  @Test
  public void testStatusForServer_json() {
    String[] args = {"status", "-s", CLUSTER.getClusterHostPorts()[0], "-o", "Json"};

    ClusterTool.main(args);
    String expectedOutput =
        "{" + LINE_SEP +
            "  \"" + CLUSTER.getClusterHostPorts()[0] + "\" : {" + LINE_SEP +
            "    \"status\" : \"ACTIVE\"," + LINE_SEP +
            "    \"member-of-cluster\" : \"-\"," + LINE_SEP +
            "    \"additional-information\" : \"Server is not part of a configured cluster\"" + LINE_SEP +
            "  }" + LINE_SEP +
        "}";
    assertThat(systemOutRule.getLog(), containsString(expectedOutput));
  }

  @Test
  public void testStatusForCluster() throws Exception {
    configureCluster(getConnectionURI());
    String[] args = {"status", "-n", CLUSTER_NAME, "-s", CLUSTER.getClusterHostPorts()[0]};

    ClusterTool.main(args);
    String consoleLog = systemOutRule.getLog();
    assertThat(consoleLog, containsString("Cluster name: " + CLUSTER_NAME));
    assertThat(consoleLog, containsString("Number of stripes: 1"));
    assertThat(consoleLog, containsString("Total number of servers: 1"));
    assertThat(consoleLog, containsString("Total configured offheap: 64M"));
    assertThat(consoleLog, containsString("Data directories configured: " + DATA_ROOT_DIR_NAME));
    assertThat(consoleLog, containsString("Backup configured: false"));
    assertThat(consoleLog, containsString("Security configured: false"));
    assertThat(consoleLog, allOf(containsString(CLUSTER.getClusterHostPorts()[0]), containsString("ACTIVE")));
  }

  @Test
  public void testStatusForCluster_json() throws Exception {
    configureCluster(getConnectionURI());
    String[] args = {"status", "-n", CLUSTER_NAME, "-s", CLUSTER.getClusterHostPorts()[0], "-o", "json"};

    ClusterTool.main(args);
    String expectedOutput =
        "{" + LINE_SEP +
            "  \"cluster-configuration\" : {" + LINE_SEP +
            "    \"cluster-name\" : \"" + CLUSTER_NAME + "\"," + LINE_SEP +
            "    \"number-of-stripes\" : 1," + LINE_SEP +
            "    \"total-number-of-servers\" : 1," + LINE_SEP +
            "    \"total-configured-offheap\" : \"64M\"," + LINE_SEP +
            "    \"data-directories-configured\" : \"" + DATA_ROOT_DIR_NAME + "\"," + LINE_SEP +
            "    \"backup-configured\" : false," + LINE_SEP +
            "    \"security-configured\" : \"false\"" + LINE_SEP +
            "  }," + LINE_SEP +
            "  \"stripes-information\" : [ {" + LINE_SEP +
            "    \"" + CLUSTER.getClusterHostPorts()[0] + "\" : {" + LINE_SEP +
            "      \"server-name\" : \"testServer0\"," + LINE_SEP +
            "      \"status\" : \"ACTIVE\"" + LINE_SEP +
            "    }" + LINE_SEP +
            "  } ]" + LINE_SEP +
        "}";
    assertThat(systemOutRule.getLog(), containsString(expectedOutput));
  }
}
