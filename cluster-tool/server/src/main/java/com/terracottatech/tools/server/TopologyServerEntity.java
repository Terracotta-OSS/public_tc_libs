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
package com.terracottatech.tools.server;

import org.terracotta.entity.CommonServerEntity;
import org.terracotta.entity.StateDumpCollector;

import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.command.CommandResult.COMMAND_RESULT_TYPE;
import com.terracottatech.tools.config.ClusterConfiguration;

public class TopologyServerEntity implements CommonServerEntity<Command, CommandResult> {
  private final ClusterConfiguration clusterConfiguration;

  TopologyServerEntity(ClusterConfiguration configuration) {
    this.clusterConfiguration = configuration;
  }

  @Override
  public void addStateTo(StateDumpCollector dump) {
    ConfigDump.dumpConfig(clusterConfiguration, dump);
  }

  @Override
  public void createNew() {
    //no op
  }

  @Override
  public void destroy() {
    //no op
  }

  ClusterConfiguration getClusterConfiguration() {
    return clusterConfiguration;
  }

  CommandResult checkClusterName(Command command) {
    if (command.getClusterName() != null && !command.getClusterName().equals(clusterConfiguration.getClusterName())) {
      return new CommandResult(COMMAND_RESULT_TYPE.FAILURE, "Command: {} " + command.getCommandType() +
                                                            " failed to execute due to wrong cluster name.");
    }
    return null;
  }
}
