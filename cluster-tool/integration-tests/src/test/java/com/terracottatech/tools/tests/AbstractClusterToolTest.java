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

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.tools.clustertool.managers.TopologyManager;
import com.terracottatech.tools.command.OutputFormat;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.awaitility.pollinterval.IterativePollInterval.iterative;

public abstract class AbstractClusterToolTest {
  static final String CLUSTER_NAME = "cluster";
  static final Properties PROPERTIES = new Properties();
  static final String LICENSE_OFFHEAP_UNIT = "GB";
  static final String RESOURCE_NAME = "primary-server-resource";
  static final int LICENSE_OFFHEAP_LIMIT = 1000;

  static final String TIMEOUT = "150000";
  static final String SOME_SERVERS_DEAD_TIMEOUT = "10000";
  static final String ALL_SERVERS_DEAD_TIMEOUT = "2000";

  private static final String LICENSE_FILE_NAME = "TerracottaDB101Linux.xml";

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();
  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog().muteForSuccessfulTests();
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  String getLicensePath() {
    try {
      return Paths.get(AbstractClusterToolTest.class.getResource("/" + LICENSE_FILE_NAME).toURI()).toString();
    } catch (URISyntaxException e) {
      throw new AssertionError("License file " + LICENSE_FILE_NAME + " URL isn't formatted correctly");
    }
  }

  Callable<String> showServerStatusAndGetLog(String[] servers) {
    return () -> {
      systemOutRule.clearLog();
      try {
        new TopologyManager().showServersStatus(OutputFormat.TABULAR, Arrays.asList(servers));
      } catch (Exception e) {
        //Don't care
      }
      return systemOutRule.getLog();
    };
  }

  <T> void waitedAssert(Callable<T> callable, Matcher<T> predicate) {
    Awaitility.await()
        .pollInterval(iterative(duration -> duration.multiply(2)).with().startDuration(Duration.TWO_HUNDRED_MILLISECONDS))
        .atMost(Duration.ONE_MINUTE)
        .until(callable, predicate);
  }
}
