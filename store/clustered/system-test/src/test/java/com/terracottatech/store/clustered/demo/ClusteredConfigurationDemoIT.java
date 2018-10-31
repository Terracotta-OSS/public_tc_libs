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
package com.terracottatech.store.clustered.demo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.terracottatech.store.clustered.ConfigurationDemo;
import com.terracottatech.testing.rules.EnterpriseCluster;

import java.net.URI;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;

/**
 * Exercises {@link ConfigurationDemo}.
 */
public class ClusteredConfigurationDemoIT {

  private static final String OFFHEAP_RESOURCE = "primary-server-resource";
  private static final String DISK_RESOURCE = "root";

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"" + OFFHEAP_RESOURCE + "\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>"
          + "</config>\n"
          + "<config>"
          + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
          + "<data:directory name=\"" + DISK_RESOURCE + "\">../data</data:directory>\n"
          + "</data:data-directories>\n"
          + "</config>\n";

  @ClassRule
  public static final EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @AfterClass
  public static void stopServers() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
  }

  @Test
  public void testConfigurationDemoFull() throws Exception {
    URI uri = CLUSTER.getConnectionURI();
    ConfigurationDemo demo = new ConfigurationDemo(uri, OFFHEAP_RESOURCE, DISK_RESOURCE);
    demo.fullExample();
  }

  @Test
  public void testConfigurationDemoBroken() throws Exception {
    URI uri = CLUSTER.getConnectionURI();
    ConfigurationDemo demo = new ConfigurationDemo(uri, OFFHEAP_RESOURCE, DISK_RESOURCE);
    demo.brokenConfiguration();
  }

  @Test
  public void testConfigurationDemoNotBroken() throws Exception {
    URI uri = CLUSTER.getConnectionURI();
    ConfigurationDemo demo = new ConfigurationDemo(uri, OFFHEAP_RESOURCE, DISK_RESOURCE);
    demo.notBrokenConfiguration();
  }

  @Test
  public void testConfigurationDemoFluent() throws Exception {
    URI uri = CLUSTER.getConnectionURI();
    ConfigurationDemo demo = new ConfigurationDemo(uri, OFFHEAP_RESOURCE, DISK_RESOURCE);
    demo.fluentConfiguration();
  }

  @Test
  public void testConfigurationDemoDatasetWithIndex() throws Exception {
    URI uri = CLUSTER.getConnectionURI();
    ConfigurationDemo demo = new ConfigurationDemo(uri, OFFHEAP_RESOURCE, DISK_RESOURCE);
    demo.datasetWithIndex();
  }
}
