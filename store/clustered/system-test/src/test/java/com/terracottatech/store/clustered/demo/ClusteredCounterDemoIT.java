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

import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.terracottatech.store.embedded.demo.CounterDemo;
import com.terracottatech.testing.rules.EnterpriseCluster;
import com.terracottatech.testing.rules.EnterpriseExternalCluster;

import java.io.StringWriter;
import java.net.URI;
import java.util.List;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;

/**
 * Runs the {@link CounterDemo} in clustered mode.
 */
public class ClusteredCounterDemoIT {

  private static final String OFFHEAP_RESOURCE = "primary-server-resource";

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"" + OFFHEAP_RESOURCE + "\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>"
          + "</config>\n"
          + "<config>"
          + "<data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n"
          + "<data:directory name=\"root\">../data</data:directory>\n"
          + "</data:data-directories>\n"
          + "</config>\n";

  @ClassRule
  public static EnterpriseCluster CLUSTER = newCluster(1).withPlugins(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void driveCounterDemo() throws Exception {
    URI uri = CLUSTER.getConnectionURI();
    CounterDemo demo = new CounterDemo();

    StringWriter writer = new StringWriter();

    demo.runClustered(uri, OFFHEAP_RESOURCE, writer);

    List<String> lines = asList(writer.toString().split("\n"));

    @SuppressWarnings("unchecked")
    Matcher<Iterable<String>> hasItems = hasItems(
            containsString("'counter9' was stopped by: Albin"),
            containsString(" not stopped yet is: 4.5"),
            containsString(" DSL is: 4.5"),
            containsString("DSL mutation: 'counter8' was stopped by: Albin"),
            containsString(" key counter10 was added"));
    assertThat(lines, hasItems);

    lines.stream()
            .filter(s -> s.startsWith("[postStopping] "))
            .forEach(s -> {
              assertThat(s, stringContainsInOrder(asList("stopped", "true")));
              assertThat(s, stringContainsInOrder(asList("stoppedBy", "Albin")));
            });
  }
}
