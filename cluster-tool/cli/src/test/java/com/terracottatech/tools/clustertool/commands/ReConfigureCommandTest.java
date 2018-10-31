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
import org.junit.Test;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ReConfigureCommandTest extends BaseCommandTest {
  @Test
  public void testReconfigure_wrong_args_1() throws Exception {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Invalid Parameter: Either")));

    String[] args = new String[]{"reconfigure", "-n", "cluster", getTempPath(), "-s", "localhost:12345"};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_wrong_args_2() throws Exception {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Invalid Parameter: Either")));

    String[] args = new String[]{"reconfigure", "-n", "cluster", getTempPath(), "-l", getTempPath(), "-s", "localhost:12345"};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_insufficient_args_1() throws Exception {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Invalid Parameter: At least")));

    String[] args = new String[]{"reconfigure", "-n", "cluster"};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_insufficient_args_2() throws Exception {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Invalid Parameter: At least")));

    String[] args = new String[]{"reconfigure", "-n", "cluster", "-l", getTempPath()};
    ClusterTool.main(args);
  }

  @Test
  public void testReconfigure_insufficient_args_3() throws Exception {
    systemExit.expectSystemExitWithStatus(StatusCode.BAD_REQUEST.getCode());
    systemExit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Invalid Parameter: At least")));

    String[] args = new String[]{"reconfigure", "-n", "cluster", "-s", "localhost:12345"};
    ClusterTool.main(args);
  }
}