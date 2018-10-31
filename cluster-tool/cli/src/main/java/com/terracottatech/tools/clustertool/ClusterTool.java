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
package com.terracottatech.tools.clustertool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.tc.util.TCAssertionError;
import com.terracottatech.tools.clustertool.commands.MainCommand;
import com.terracottatech.tools.clustertool.exceptions.ClusterToolException;
import com.terracottatech.tools.clustertool.managers.BackupManager;
import com.terracottatech.tools.clustertool.managers.ClusterManager;
import com.terracottatech.tools.clustertool.managers.CommandManager;
import com.terracottatech.tools.clustertool.managers.DefaultBackupManager;
import com.terracottatech.tools.clustertool.managers.DefaultClusterManager;
import com.terracottatech.tools.clustertool.managers.DefaultCommandManager;
import com.terracottatech.tools.clustertool.managers.DefaultLicenseManager;
import com.terracottatech.tools.clustertool.managers.CommonEntityManager;
import com.terracottatech.tools.clustertool.managers.LicenseManager;
import com.terracottatech.tools.clustertool.managers.TopologyManager;
import com.terracottatech.tools.clustertool.parser.CustomJCommander;
import com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import com.terracottatech.tools.validation.ConfigurationValidator;
import com.terracottatech.tools.config.DefaultConfigurationParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Entry point for cluster tool.
 */
public class ClusterTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTool.class);

  public static void main(String[] args) {
    ClusterTool clusterTool = new ClusterTool();
    StatusCode exitCode = StatusCode.SUCCESS;
    Throwable t = null;
    String message = null;

    try {
      clusterTool.start(args);
    } catch (IllegalArgumentException | ParameterException ex) {
      exitCode = StatusCode.BAD_REQUEST;
      t = ex;
    } catch (ClusterToolException ex) {
      exitCode = ex.getStatusCode();
      //the wrapper isn't useful anymore:
      t = ex.getCause() == null ? ex : ex.getCause();
      message = ex.getMessage();
    } catch (Exception | TCAssertionError ex) {
      exitCode = StatusCode.FAILURE; //general unhandled error
      t = ex;
    }

    if (t != null) {
      if (message == null) {
        message = t.getMessage();
      }
      String errorMessage = String.format("Error (%s): %s", exitCode.name(), message);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.error(errorMessage, t);
      } else {
        LOGGER.error(errorMessage);
      }
      System.exit(exitCode.getCode());
    }
  }

  /**
   * Start the cluster tool.
   *
   * @param args command-line arguments
   */
  public void start(String[] args) {
    TopologyManager topologyManager = new TopologyManager();
    CommonEntityManager entityManager = new CommonEntityManager();
    LicenseManager licenseManager = new DefaultLicenseManager(topologyManager, entityManager);
    BackupManager backupManager = new DefaultBackupManager(topologyManager, entityManager);
    ClusterManager clusterManager = new DefaultClusterManager(new DefaultConfigurationParser(), new ConfigurationValidator(), topologyManager, licenseManager);
    CommandManager commandManager = new DefaultCommandManager(clusterManager, licenseManager, backupManager, topologyManager);
    JCommander jCommander = this.parseArguments(args, commandManager);

    // Process cluster tool arguments like '-v'
    commandManager.getCommand(MainCommand.NAME).process(jCommander);

    // If no command is provided, process help command
    if (jCommander.getParsedCommand() == null) {
      jCommander.usage(); //intentionally bypassing logger
      //show all exit codes, use same output channel for consistency
      JCommander.getConsole().println(generateStatusCodeHelp());
      return;
    }

    // Otherwise, process the real command
    try {
      commandManager.getCommand(jCommander.getParsedCommand()).process(jCommander);
    } catch (ParameterException iax) {
      jCommander.usage(jCommander.getParsedCommand());
      throw iax;
    }

  }

  private String generateStatusCodeHelp() {
    return "Exit Codes:\n    " + Stream
        .of(StatusCode.values())
        .map(StatusCode::toString)
        .collect(Collectors.joining("\n    "));
  }

  private JCommander parseArguments(String[] args, CommandManager commandManager) {
    JCommander jCommander = new CustomJCommander(commandManager.getCommand(MainCommand.NAME), commandManager);
    commandManager.getCommands().forEach(command -> {
      if (!MainCommand.NAME.equals(command.name())) {
        jCommander.addCommand(command.name(), command);
      }
    });
    try {
      jCommander.parse(args);
    } catch (ParameterException e) {
      String parsedCommand = jCommander.getParsedCommand();
      if (parsedCommand != null) {
        jCommander.usage(parsedCommand);
      }
      throw e;
    }
    return jCommander;
  }
}
