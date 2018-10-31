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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.terracottatech.tools.clustertool.managers.TopologyManager;
import com.terracottatech.tools.util.ConnectionPropertiesUtil;

import org.slf4j.LoggerFactory;


/**
 * MainCommand processes cluster-tool level commands, e.g. verbose mode.
 */
public class MainCommand extends AbstractServerCommand {
  public static final String NAME = "main";
  private final TopologyManager topologyManager;

  @Parameter(names = {"-h", "--help"}, description = "Help", help = true)
  private boolean help;

  @Parameter(names = {"-v", "--verbose"}, description = "Verbose mode")
  private boolean verbose = false;

  @Parameter(names = {"-t", "--timeout"}, description = "Server connection timeout in milliseconds")
  private String timeout;

  @Parameter(names = {"-srd", "--security-root-directory"}, description = "Security root directory")
  private String securityRootDirectory;

  public MainCommand(TopologyManager topologyManager) {
    this.topologyManager = topologyManager;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void process(JCommander jCommander) {
    if (securityRootDirectory != null) {
      topologyManager.setSecurityRootDirectory(securityRootDirectory);
    }

    if (verbose) {
      Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
      rootLogger.setLevel(Level.DEBUG);
      Appender<ILoggingEvent> detailAppender = rootLogger.getAppender("STDOUT-DETAIL");

      Logger clusterToolLogger = (Logger) LoggerFactory.getLogger("com.terracottatech.tools");
      clusterToolLogger.setLevel(Level.DEBUG);
      //Detach the STDOUT appender which logs in a minimal pattern and attached STDOUT-DETAIL appender
      clusterToolLogger.detachAppender("STDOUT");
      clusterToolLogger.addAppender(detailAppender);
    }

    if (timeout != null && !timeout.isEmpty()) {
      ConnectionPropertiesUtil.setConnectionTimeout(timeout);
    }
  }

  @Override
  public String usage() {
    return "";
  }
}
