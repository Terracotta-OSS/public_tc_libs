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
package com.terracottatech.tools.clustertool.managers;

import com.terracottatech.tools.clustertool.commands.BackupCommand;
import com.terracottatech.tools.clustertool.commands.Command;
import com.terracottatech.tools.clustertool.commands.ConfigureCommand;
import com.terracottatech.tools.clustertool.commands.DumpCommand;
import com.terracottatech.tools.clustertool.commands.IPWhitelistReloadCommand;
import com.terracottatech.tools.clustertool.commands.MainCommand;
import com.terracottatech.tools.clustertool.commands.ReConfigureCommand;
import com.terracottatech.tools.clustertool.commands.StatusCommand;
import com.terracottatech.tools.clustertool.commands.StopCommand;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultCommandManager implements CommandManager {

  private final Map<String, Command> commandMap;

  public DefaultCommandManager(ClusterManager clusterManager, LicenseManager licenseManager, BackupManager backupManager, TopologyManager topologyManager) {
    this.commandMap = Stream
        .of(new MainCommand(topologyManager),
            new ConfigureCommand(clusterManager, licenseManager),
            new ReConfigureCommand(clusterManager),
            new StopCommand(clusterManager),
            new StatusCommand(clusterManager),
            new DumpCommand(clusterManager),
            new BackupCommand(backupManager),
            new IPWhitelistReloadCommand(clusterManager))
        .collect(Collectors.toMap(Command::name, Function.identity()));
  }

  @Override
  public Collection<Command> getCommands() {
    return Collections.unmodifiableCollection(this.commandMap.values());
  }

  @Override
  public Command getCommand(String name) {
    if (!this.commandMap.containsKey(name)) {
      throw new IllegalArgumentException("Command not found: " + name);
    }
    return this.commandMap.get(name);
  }
}
