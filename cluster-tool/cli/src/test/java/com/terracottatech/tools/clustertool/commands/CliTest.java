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
package com.terracottatech.tools.clustertool.commands;

import com.terracottatech.tools.clustertool.ClusterTool;
import com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class CliTest extends BaseCommandTest {
  @Test
  public void testCli() {
    ClusterTool.main(new String[]{});
    assertThat(systemOutRule.getLog(), containsString("Usage: cluster-tool"));
  }

  @Test
  public void testCliHelp() {
    ClusterTool.main(new String[]{"--help"});
    assertThat(systemOutRule.getLog(), containsString("Usage: cluster-tool"));
    assertThat(systemOutRule.getLog(), containsString("Exit Codes"));
  }

  @Test
  public void testCliUnknownCommand() {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() ->
        assertThat(systemOutRule.getLog(), containsString("Expected a command, got blah")));
    ClusterTool.main(new String[]{"blah"});
  }

  @Test
  public void testCliStatusNoParams() {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() ->
        assertThat(systemOutRule.getLog(), containsString("Usage:")));
    ClusterTool.main(new String[]{"status"});
  }

  @Test
  public void testCliStatusWithHelp() {
    ClusterTool.main(new String[]{"status", "--help"});
    assertThat(systemOutRule.getLog(), containsString("Usage:"));
  }

  @Test
  public void testCliStatusWrongParams() {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() ->
        assertThat(systemOutRule.getLog(), containsString("Usage:")));

    ClusterTool.main(new String[]{"status", "-n", "clustername"});
  }
}