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
import com.terracottatech.tools.clustertool.license.LicenseHelper;
import com.terracottatech.tools.clustertool.managers.ClusterManager;
import com.terracottatech.tools.clustertool.managers.LicenseManager;

import java.util.ArrayList;
import java.util.List;

/**
 * ConfigureCommand configures the given cluster with provided tc-configs or servers.
 */
@Parameters(commandDescription = "Configure a cluster")
public class ConfigureCommand extends AbstractServerCommand {
  private static final String NAME = "configure";

  private final ClusterManager clusterManager;
  private final LicenseManager licenseManager;

  @Parameter(names = "-n", required = true, description = "Cluster name")
  private String clusterName;

  @Parameter(names = {"-h", "--help"}, help = true, description = "Help")
  private boolean help;

  @Parameter(names = "-l", description = "License file")
  private String licenseFilePath;

  @Parameter(description = "Terracotta configuration files")
  private List<String> tcConfigNames = new ArrayList<>();

  @Parameter(names = "-s", description = "List of server host:port(s), default port(s) being optional")
  private List<String> tcServerNames = new ArrayList<>();

  public ConfigureCommand(ClusterManager clusterManager, LicenseManager licenseManager) {
    this.clusterManager = clusterManager;
    this.licenseManager = licenseManager;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void process(JCommander jCommander) {
    if (processHelp(help, jCommander)) return;

    if (tcConfigNames.isEmpty() && tcServerNames.isEmpty()) {
      throw new ParameterException("Invalid Parameter: At least one tc-config or host:port should be provided along with cluster name.");
    } else if (!tcConfigNames.isEmpty() && !tcServerNames.isEmpty()) {
      throw new ParameterException("Invalid Parameter: Provide either a list of tc-configs or a list of hosts:ports, but not both.");
    }

    String licenseFilePath = LicenseHelper.getResolvedLicenseFilePath(this.licenseFilePath);
    if (licenseFilePath == null || licenseFilePath.isEmpty()) {
      throw new ParameterException("Invalid Parameter: License file should be provided," +
          " or named 'license.xml' and placed in 'tools/cluster-tool/conf' under the installation directory.");
    }

    if (!tcConfigNames.isEmpty()) {
      this.clusterManager.configureByConfigs(clusterName, tcConfigNames, licenseFilePath);
    } else {
      this.clusterManager.configureByServers(clusterName, validateHostPortList(tcServerNames), licenseFilePath);
    }
  }

  @Override
  public String usage() {
    return "configure -n CLUSTER-NAME [-l LICENSE-FILE] TC-CONFIG [TC-CONFIG...]" +
        "\nconfigure -n CLUSTER-NAME [-l LICENSE-FILE] -s HOST[:PORT] [-s HOST[:PORT]]...";
  }
}
