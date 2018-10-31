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
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.TemporaryFolder;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.tests.BackupTestHelper.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class AbstractBackupIT {

  @ClassRule
  public static TemporaryFolder backupFolder = new TemporaryFolder();

  static {
    try {
      backupFolder.create();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @After
  public void cleanup() {
    backupFolder.delete();
  }

  @Test
  public void testBackupPlatform() throws Exception {
    backupViaClusterTool(getCluster());
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testBackupPlatformEhcache() throws Exception {
    populateEhCache(getCluster());
    backupViaClusterTool(getCluster());
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  @Test
  public void testBackupPlatformEhcacheTCStore() throws Exception {
    populateEhCache(getCluster());
    populateTCStore(getCluster());
    backupViaClusterTool(getCluster(), true);
    assertThat(systemOutRule.getLog(), containsString(COMMAND_SUCCESS_MESSAGE));
  }

  protected abstract EnterpriseCluster getCluster();
}