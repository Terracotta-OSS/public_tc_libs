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

import java.util.ArrayList;
import java.util.List;

/**
 * ReConfigureCommand updates the configuration of an already configured cluster.
 */
@Parameters(commandDescription = "Prepares the configuration update of a cluster - requires restart of server(s) with updated configuration(s)")
public class ReConfigureCommand extends AbstractServerCommand {
  private static final String NAME = "reconfigure";

  private final ClusterManager clusterManager;

  @Parameter(names = "-n", required = true, description = "Cluster name")
  private String clusterName;

  @Parameter(names = {"-h", "--help"}, help = true, description = "Help")
  private boolean help;

  @Parameter(names = "-l", description = "License file")
  private String licenseFilePath;

  @Parameter(names = "-s", description = "List of server host:port(s), default port(s) being optional")
  private List<String> hostPortList = new ArrayList<>();

  @Parameter(description = "Terracotta configuration files")
  private List<String> tcConfigNames = new ArrayList<>();

  public ReConfigureCommand(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void process(JCommander jCommander) {
    if (processHelp(help, jCommander)) return;

    String licenseFilePath = LicenseHelper.getResolvedLicenseFilePath(this.licenseFilePath);
    boolean isLicenseResolved = licenseFilePath != null && !licenseFilePath.isEmpty();

    if (tcConfigNames.isEmpty() && !isLicenseResolved) {
      throw new ParameterException("Invalid Parameter: At least one tc-config, or one license" +
          " file must be provided along with cluster name.");
    } else if (isLicenseResolved && (hostPortList.isEmpty() && tcConfigNames.isEmpty())) {
      throw new ParameterException("Invalid Parameter: At least one tc-config, or one server" +
          " <host> (or <host>:<port>) should be provided with license file.");
    } else if (!tcConfigNames.isEmpty() && !hostPortList.isEmpty()) {
      throw new ParameterException("Invalid Parameter: Either a list of tc-configs, or one server" +
          " <host> (or <host>:<port>) should be provided, but not both.");
    }

    if (!hostPortList.isEmpty()) {
      clusterManager.updateLicense(clusterName, validateHostPortList(hostPortList), licenseFilePath);
    } else {
      clusterManager.reConfigure(clusterName, tcConfigNames, licenseFilePath);
    }
  }

  @Override
  public String usage() {
    return "reconfigure -n CLUSTER-NAME TC-CONFIG [TC-CONFIG...]" +
        "\nreconfigure -n CLUSTER-NAME -l LICENSE-FILE -s HOST[:PORT] [-s HOST[:PORT]]..." +
        "\nreconfigure -n CLUSTER-NAME -l LICENSE-FILE TC-CONFIG [TC-CONFIG...]";
  }
}
