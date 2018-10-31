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


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.terracottatech.tools.clustertool.managers.ClusterManager;

import java.util.ArrayList;
import java.util.List;


/**
 * IPWhitelistReloadCommand notifies the servers to reload the IPWhitelist file.
 */
@Parameters(commandDescription = "Reload IP whitelist file on individual server(s), or an entire cluster")
public class IPWhitelistReloadCommand extends AbstractServerCommand {
  private static final String NAME = "ipwhitelist-reload";

  private final ClusterManager clusterManager;

  @Parameter(names = "-n", description = "Cluster name")
  private String clusterName;

  @Parameter(names = {"-h", "--help"}, help = true, description = "Help")
  private boolean help;

  @Parameter(names = "-s", description = "List of server host:port(s), default port(s) being optional")
  private List<String> hostPortListWithParam = new ArrayList<>();

  @Parameter
  private List<String> hostPortListDefault = new ArrayList<>();

  public IPWhitelistReloadCommand(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void process(JCommander jCommander) {
    if (processHelp(help, jCommander)) return;

    List<String> hostPortList = validateAndGetHostPortList(hostPortListDefault, hostPortListWithParam);
    if (isClusterNameSpecified(clusterName)) {
      clusterManager.reloadClusterIPWhitelist(clusterName, hostPortList);
    } else {
      clusterManager.reloadIPWhitelist(hostPortList);
    }
  }

  @Override
  public String usage() {
    return "ipwhitelist-reload -n CLUSTER-NAME -s HOST[:PORT] [-s HOST[:PORT]]..." +
        "\nipwhitelist-reload -s HOST[:PORT] [-s HOST[:PORT]]...";
  }
}
