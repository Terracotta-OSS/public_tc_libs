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
package com.terracottatech.tools.config;


import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandCodec;
import com.terracottatech.tools.command.CommandResult;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

public class CommandCodecTest {

  @Test
  public void testCommandCodec() throws Exception {
    Command configureCommand = new Command("hello", Command.COMMAND_TYPE.DUMP_STATE_CLUSTER, "1", "2", "3");
    Command decodedConfigureCommand = CommandCodec.MESSAGE_CODEC.decodeMessage(CommandCodec.MESSAGE_CODEC.encodeMessage(configureCommand));

    assertThat(configureCommand.getClusterName(), is(decodedConfigureCommand.getClusterName()));
    assertThat(configureCommand.getCommandType(), is(decodedConfigureCommand.getCommandType()));
    assertThat(configureCommand.getArgs(), is(decodedConfigureCommand.getArgs()));
  }

  @Test
  public void testCommandResultCodec() throws Exception {
    CommandResult commandResult = new CommandResult(CommandResult.COMMAND_RESULT_TYPE.CONFIG, "success");
    CommandResult decodedCommandResult = CommandCodec.MESSAGE_CODEC.decodeResponse(CommandCodec.MESSAGE_CODEC.encodeResponse(commandResult));

    assertThat(commandResult.getCommandResultType(), is(decodedCommandResult.getCommandResultType()));
    assertThat(commandResult.getMessage(), is(decodedCommandResult.getMessage()));
  }

  @Test
  public void testConfigResultCodec() throws Exception {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    Cluster cluster = configurationParser.parseConfigurations(readContent("config-types/tc-config1.xml"),
        readContent("config-types/tc-config2.xml"));
    CommandResult.ConfigResult configResult = new CommandResult.ConfigResult(new ClusterConfiguration("test", cluster));
    CommandResult.ConfigResult decodedConfigResult = (CommandResult.ConfigResult)CommandCodec.MESSAGE_CODEC.decodeResponse(CommandCodec.MESSAGE_CODEC.encodeResponse(configResult));

    assertThat(configResult.getCommandResultType(), is(decodedConfigResult.getCommandResultType()));
    assertThat(configResult.getClusterConfiguration().getClusterName(), is(decodedConfigResult.getClusterConfiguration().getClusterName()));
    assertThat(configResult.getClusterConfiguration().getCluster().getStripes().size(), is(decodedConfigResult.getClusterConfiguration().getCluster().getStripes().size()));

    // Asserting the decoded cluster configuration.
    for (int stripeIndex = 0; stripeIndex < configResult.getClusterConfiguration().getCluster().getStripes().size(); stripeIndex++) {
      for (int configIndex = 0; configIndex < configResult.getClusterConfiguration().getCluster().getStripes().get(stripeIndex).getConfigs().size(); configIndex++) {
        assertEquals(
            configResult.getClusterConfiguration().getCluster().getStripes().get(stripeIndex).getConfigs().get(configIndex),
            decodedConfigResult.getClusterConfiguration().getCluster().getStripes().get(stripeIndex).getConfigs().get(configIndex));
      }

      for (int serverIndex = 0; serverIndex < configResult.getClusterConfiguration().getCluster().getStripes().get(stripeIndex).getServers().size(); serverIndex++) {
        assertEquals(
            configResult.getClusterConfiguration().getCluster().getStripes().get(stripeIndex).getServers().get(serverIndex),
            decodedConfigResult.getClusterConfiguration().getCluster().getStripes().get(stripeIndex).getServers().get(serverIndex));
      }
    }

  }

  public static String readContent(String resourceConfigName) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      InputStream inputStream = DefaultConfigurationParserTest.class.getResourceAsStream("/" + resourceConfigName);
      byte[] buffer = new byte[1024];
      int length;
      while ((length = inputStream.read(buffer)) != -1) {
        baos.write(buffer, 0, length);
      }
      return baos.toString("UTF-8");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
