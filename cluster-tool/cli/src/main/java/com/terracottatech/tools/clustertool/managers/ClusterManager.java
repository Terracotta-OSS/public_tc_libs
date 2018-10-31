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


import com.terracottatech.tools.command.OutputFormat;

import java.util.List;

/**
 * ClusterManager interacts with the backend to execute the commands.
 */
public interface ClusterManager {

  /**
   * Configure a cluster with the provided cluster name, tc-configs files and license file path.
   *
   * @param clusterName
   * @param tcConfigs
   * @param licenseFilePath
   */
  void configureByConfigs(String clusterName, List<String> tcConfigs, String licenseFilePath);

  /**
   * Configure the given cluster name with the provided hosts:ports and licenseFilePath.
   *
   * @param clusterName
   * @param hostPortList
   * @param licenseFilePath
   */
  void configureByServers(String clusterName, List<String> hostPortList, String licenseFilePath);

  /**
   * Re-configure an already configured cluster with the provided tc-config files.
   *
   * @param clusterName
   * @param tcConfigs
   */
  void reConfigure(String clusterName, List<String> tcConfigs);

  /**
   * Re-configure an already configured cluster with the provided tc-config and license files.
   *
   * @param clusterName
   * @param tcConfigs
   * @param licenseFilePath
   */
  void reConfigure(String clusterName, List<String> tcConfigs, String licenseFilePath);

  /**
   * Update the cluster license with the provided license file.
   *
   * @param clusterName
   * @param hostPortList
   * @param licenseFilePath
   */
  void updateLicense(String clusterName, List<String> hostPortList, String licenseFilePath);

  /**
   * Stop the cluster with the provided cluster name.
   *
   * @param clusterName
   * @param hostPortList
   */
  void stopCluster(String clusterName, List<String> hostPortList);

  /**
   * Stop the server(s) running at the provided host(s) and port(s).
   *
   * @param hostPortList
   */
  void stopServers(List<String> hostPortList);

  /**
   * Dump the state of the cluster with the given cluster name.
   *
   * @param clusterName
   * @param hostPortList
   */
  void dumpClusterState(String clusterName, List<String> hostPortList);

  /**
   * Dump the state(s) of the server(s) running at the provided host(s) and port(s).
   *
   * @param hostPortList
   */
  void dumpServersState(List<String> hostPortList);

  /**
   * Show the status of the cluster with the provided cluster name.
   *
   * @param outputFormat
   * @param clusterName
   * @param hostPortList
   */
  void showClusterStatus(OutputFormat outputFormat, String clusterName, List<String> hostPortList);

  /**
   * Show the status(es) of the server(s) running at the provided host(s) and port(s).
   *
   * @param outputFormat
   * @param hostPortList
   */
  void showServersStatus(OutputFormat outputFormat, List<String> hostPortList);

  /**
   * Notify the servers running at the provided host(s) and port(s) to reload the IP white list.
   *
   * @param hostPortList
   */
  void reloadIPWhitelist(List<String> hostPortList);

  /**
   * Notify the servers in the cluster to reload the IP white list.
   *
   * @param clusterName
   * @param hostPortList
   */
  void reloadClusterIPWhitelist(String clusterName, List<String> hostPortList);
}
