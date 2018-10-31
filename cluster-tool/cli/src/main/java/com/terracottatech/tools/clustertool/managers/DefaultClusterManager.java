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


import com.terracottatech.License;
import com.terracottatech.tools.command.OutputFormat;
import com.terracottatech.tools.validation.Validator;
import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.ConfigurationParser;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.CONFIGURATION_UPDATE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.LICENSE_INSTALLATION_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.LICENSE_NOT_UPDATED_MESSAGE;

public class DefaultClusterManager implements ClusterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterManager.class);

  private final ConfigurationParser configurationParser;
  private final Validator validator;
  private final TopologyManager topologyManager;
  private final LicenseManager licenseManager;
  private static final String PROVIDED_FILES_MSG_FRAGMENT = "provided config files";
  private static final String KNOWN_FILES_MSG_FRAGMENT = "config files used to start the servers";

  public DefaultClusterManager(ConfigurationParser configurationParser, Validator validator, TopologyManager topologyManager, LicenseManager licenseManager) {
    this.configurationParser = configurationParser;
    this.validator = validator;
    this.topologyManager = topologyManager;
    this.licenseManager = licenseManager;
  }

  @Override
  public void configureByConfigs(String clusterName, List<String> tcConfigs, String licenseFilePath) {
    LOGGER.debug("Configure by configs called with: {}, {}, {}", clusterName, tcConfigs, licenseFilePath);

    List<String> tcConfigContents = tcConfigs.stream().map(this::fileContent).collect(Collectors.toList());
    Cluster providedCluster = configurationParser.parseConfigurations(tcConfigContents.toArray(new String[]{}));
    checkIfAnyHostIsLocalhost(providedCluster);

    // Validate that the provided configs are consistent with each other
    validator.validate(providedCluster, PROVIDED_FILES_MSG_FRAGMENT);

    // Create a Cluster by fetching the server configuration
    Cluster knownCluster = topologyManager.createClusterByStripes(providedCluster.getStripes());
    // Validate that the known cluster is consistent in its configuration
    validator.validate(knownCluster, KNOWN_FILES_MSG_FRAGMENT);

    // Verify that the provided tc-configs are equivalent to the tc-configs used to start the servers
    providedCluster.getStripes().forEach(stripe -> validator.validateStripeAgainstCluster(stripe, knownCluster));

    if (providedCluster.getStripes().get(0).shouldValidateStrictly()) {
      // All good, proceed - use knownCluster because it has parameters substituted by the servers
      configureInternal(clusterName, knownCluster, licenseFilePath);
    }else {
      //use provideCluster directly
      configureInternal(clusterName, providedCluster, licenseFilePath);
    }
  }

  @Override
  public void configureByServers(String clusterName, List<String> hostPortList, String licenseFilePath) {
    LOGGER.debug("Configure by servers called with: {}, {}, {}", clusterName, hostPortList, licenseFilePath);

    List<Server> servers = parseServers(hostPortList);
    Cluster cluster = topologyManager.createClusterByServers(servers);
    checkIfAnyHostIsLocalhost(cluster);
    validator.validate(cluster, KNOWN_FILES_MSG_FRAGMENT);

    configureInternal(clusterName, cluster, licenseFilePath);
  }

  @Override
  public void reConfigure(String clusterName, List<String> tcConfigs) {
    reConfigure(clusterName, tcConfigs, null);
  }

  @Override
  public void reConfigure(String clusterName, List<String> tcConfigs, String licenseFilePath) {
    LOGGER.debug("Re-configure called with: {}, {}, {}", clusterName, tcConfigs, licenseFilePath);

    List<String> tcConfigContents = tcConfigs.stream().map(this::fileContent).collect(Collectors.toList());
    Cluster cluster = configurationParser.parseConfigurations(tcConfigContents.toArray(new String[]{}));
    checkIfAnyHostIsLocalhost(cluster);

    // Validate that the provided tc-configs are consistent with each other here. Validation with
    // previous configuration will be done at the server side.
    validator.validate(cluster, PROVIDED_FILES_MSG_FRAGMENT);
    licenseManager.ensureCompatibleLicense(cluster);

    if (licenseFilePath != null && !licenseFilePath.isEmpty()) {
      //Install the new license prior to invoking reconfigure
      boolean installed = licenseManager.installLicense(clusterName, cluster, licenseManager.parse(licenseFilePath));
      if (installed) {
        LOGGER.info(LICENSE_INSTALLATION_MESSAGE);
      } else {
        LOGGER.info(LICENSE_NOT_UPDATED_MESSAGE);
      }
    }

    topologyManager.reConfigure(clusterName, cluster.getStripes());
    LOGGER.info(CONFIGURATION_UPDATE_MESSAGE);

    LOGGER.info("\n" + COMMAND_SUCCESS_MESSAGE);
  }

  @Override
  public void updateLicense(String clusterName, List<String> hostPortList, String licenseFilePath) {
    LOGGER.debug("Update license called with: {}, {}, {}", clusterName, hostPortList, licenseFilePath);

    Cluster cluster = getCluster(hostPortList);
    boolean success = licenseManager.installLicense(clusterName, cluster, licenseManager.parse(licenseFilePath));
    if (success) {
      LOGGER.info(LICENSE_INSTALLATION_MESSAGE);
    } else {
      LOGGER.info(LICENSE_NOT_UPDATED_MESSAGE);
    }

    LOGGER.info("\n" + COMMAND_SUCCESS_MESSAGE);
  }

  @Override
  public void stopCluster(String clusterName, List<String> hostPortList) {
    LOGGER.debug("Stop cluster called with: {}, {}", clusterName, hostPortList);
    topologyManager.stopCluster(clusterName, hostPortList);
  }

  @Override
  public void stopServers(List<String> hostPortList) {
    LOGGER.debug("Stop servers called with: {}", hostPortList);
    topologyManager.stopServers(hostPortList);
  }

  @Override
  public void dumpClusterState(String clusterName, List<String> hostPortList) {
    LOGGER.debug("Dump cluster state called with: {}, {}", clusterName, hostPortList);
    topologyManager.dumpClusterState(clusterName, hostPortList);
  }

  @Override
  public void dumpServersState(List<String> hostPortList) {
    LOGGER.debug("Dump servers state called with: {}", hostPortList);
    topologyManager.dumpServersState(hostPortList);
  }

  @Override
  public void showClusterStatus(OutputFormat outputFormat, String clusterName, List<String> hostPortList) {
    LOGGER.debug("Show cluster status called with: {}, {}, {}", outputFormat, clusterName, hostPortList);
    topologyManager.showClusterStatus(outputFormat, clusterName, hostPortList);
  }

  @Override
  public void showServersStatus(OutputFormat outputFormat, List<String> hostPortList) {
    LOGGER.debug("Show servers status called with: {}, {}", outputFormat, hostPortList);
    topologyManager.showServersStatus(outputFormat, hostPortList);
  }

  @Override
  public void reloadIPWhitelist(List<String> hostPortList) {
    LOGGER.debug("Reload IP whitelist called with: {}", hostPortList);
    topologyManager.reloadIPWhitelist(hostPortList);
  }

  @Override
  public void reloadClusterIPWhitelist(String clusterName, List<String> hostPortList) {
    LOGGER.debug("Reload IP whitelist called with: {}, {}", clusterName, hostPortList);
    topologyManager.reloadClusterIPWhitelist(clusterName, hostPortList);
  }

  private String fileContent(String fileName) {
    try {
      return new String(Files.readAllBytes(Paths.get(fileName)), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  static List<Server> parseServers(List<String> hostPortList) {
    List<Server> servers = new ArrayList<>(hostPortList.size());
    for (String serverName : hostPortList) {
      int lastIndexOfColon = serverName.lastIndexOf(":");
      if (lastIndexOfColon == -1 || (serverName.startsWith("[") && serverName.endsWith("]"))) {
        //IPv4/IPv6 address without port
        servers.add(new Server(serverName, serverName));
      } else {
        //IPv4/IPv6 address with port
        String hostOrIp = serverName.substring(0, lastIndexOfColon);
        String port = serverName.substring(lastIndexOfColon + 1);
        servers.add(new Server(hostOrIp, hostOrIp, Integer.parseInt(port)));
      }
    }
    return servers;
  }

  private Cluster getCluster(List<String> hostPortList) {
    return new Cluster(Collections.singletonList(new Stripe(parseServers(hostPortList))));
  }

  private void configureInternal(String clusterName, Cluster cluster, String licenseFilePath) {
    License license = licenseManager.parse(licenseFilePath);
    licenseManager.ensureCompatibleLicense(cluster, license);
    topologyManager.configureByStripes(clusterName, cluster.getStripes());
    LOGGER.info(CONFIGURATION_UPDATE_MESSAGE);

    licenseManager.installLicense(clusterName, cluster, license);
    LOGGER.info(LICENSE_INSTALLATION_MESSAGE);

    LOGGER.info("\n" + COMMAND_SUCCESS_MESSAGE);
  }

  private void checkIfAnyHostIsLocalhost(Cluster cluster) {
    cluster.getServers().forEach(server -> {
      if (server.getHost().equals("localhost")) {
        LOGGER.warn("WARNING: Found \"localhost\" as the hostname in " + server + ". " +
                    "This may not work correctly if clients and operator console are connecting from other hosts. " +
                    "Replace \"localhost\" with an appropriate hostname in the configuration.\n");
      }
    });
  }
}
