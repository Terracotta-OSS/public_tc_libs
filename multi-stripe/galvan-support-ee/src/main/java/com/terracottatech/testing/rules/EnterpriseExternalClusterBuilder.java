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
package com.terracottatech.testing.rules;

import org.terracotta.testing.master.ConfigBuilder;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

/**
 * EnterpriseExternalClusterBuilder
 */
public class EnterpriseExternalClusterBuilder {

  /**
   * Creates a new {@link EnterpriseExternalCluster} builder.
   *
   * The cluster will be sized according to the passed parameters. The first value is the number of servers in the first stripe.
   * Any additional number indicates that another stripe with the provided number of servers must be part of the cluster.
   *
   * @param serversFirstStripe amount of servers in first stripe
   * @param serversOtherStripes amount of servers in each additional stripe
   * @return a builder instance
   */
  public static EnterpriseExternalClusterBuilder newCluster(int serversFirstStripe, int... serversOtherStripes) {
    int[] servers = new int[serversOtherStripes.length + 1];
    servers[0] = serversFirstStripe;
    for (int i = 0; i < serversOtherStripes.length; i++) {
      servers[i + 1] = serversOtherStripes[i];
    }
    return newCluster(servers);
  }

  public static EnterpriseExternalClusterBuilder newCluster(int[] serversPerStripe) {
    if (serversPerStripe.length == 0) {
      throw new IllegalArgumentException("no server and no stripe defined");
    }
    return new EnterpriseExternalClusterBuilder(serversPerStripe);
  }

  private final int[] servers;
  private File clusterDirectory = new File(System.getProperty("kitTestDirectory"));
  private List<File> serverJars = emptyList();
  private String pluginsFragment = "";
  private String namespaceFragment = "";
  private Properties tcProperties = new Properties();
  private Supplier<Properties> serverPropertiesSupplier = Properties::new;
  private Path securityRootDirectory;
  private String clusterName = "primary";
  private String logConfigExtensionResourceName = "logback-ext.xml";
  private int clientReconnectWindowTime = ConfigBuilder.DEFAULT_CLIENT_RECONNECT_WINDOW_TIME;
  private int failoverPriorityVoterCount = ConfigBuilder.FAILOVER_PRIORITY_AVAILABILITY;

  private EnterpriseExternalClusterBuilder(int[] servers) {
    this.servers = servers;
  }

  public EnterpriseExternalClusterBuilder inDirectory(File clusterDirectory) {
    this.clusterDirectory = clusterDirectory;
    return this;
  }

  public EnterpriseExternalClusterBuilder withPlugins(String pluginsFragment) {
    this.pluginsFragment = pluginsFragment;
    return this;
  }

  public EnterpriseExternalClusterBuilder withClusterName(String clusterName) {
    this.clusterName = clusterName;
    return this;
  }

  public EnterpriseExternalClusterBuilder withNamespace(String namespaceFragment) {
    this.namespaceFragment = namespaceFragment;
    return this;
  }

  public EnterpriseExternalClusterBuilder withServerJars(File jar, File[] extraJars) {
    List<File> jars = new ArrayList<>();
    jars.add(jar);
    Collections.addAll(jars, extraJars);
    return withServerJars(jars);
  }

  public EnterpriseExternalClusterBuilder withServerJars(List<File> jars) {
    this.serverJars = new ArrayList<>(jars);
    return this;
  }

  public EnterpriseExternalClusterBuilder withTcProperties(Properties tcProperties) {
    this.tcProperties = tcProperties;
    return this;
  }

  public EnterpriseExternalClusterBuilder withServerProperties(Properties serverProperties) {
    this.serverPropertiesSupplier = () -> serverProperties;
    return this;
  }

  public EnterpriseExternalClusterBuilder withServerProperties(Supplier<Properties> serverPropertiesSupplier) {
    this.serverPropertiesSupplier = serverPropertiesSupplier;
    return this;
  }

  public EnterpriseExternalClusterBuilder withSecurityRootDirectory(Path securityRootDirectory) {
    this.securityRootDirectory = securityRootDirectory;
    return this;
  }

  public EnterpriseExternalClusterBuilder withLogConfigExtensionResourceName(String logConfigExtensionResourceName) {
    this.logConfigExtensionResourceName = logConfigExtensionResourceName;
    return this;
  }

  public EnterpriseExternalClusterBuilder withClientReconnectWindowTimeInSeconds(int clientReconnectWindowTime) {
    this.clientReconnectWindowTime = clientReconnectWindowTime;
    return this;
  }

  /**
   * Zero or any positive value will tune the cluster for consistency and set the respective voter count as provided.
   * A value of -1 will tune the cluster for availability. This is the default.
   */
  public EnterpriseExternalClusterBuilder withFailoverPriorityVoterCount(int failoverPriorityVoterCount) {
    this.failoverPriorityVoterCount = failoverPriorityVoterCount;
    return this;
  }

  /**
   * Creates a configured {@link EnterpriseExternalCluster}.
   *
   * @return a usable cluster
   */
  public EnterpriseExternalCluster build() {
    return build(true);
  }

  /**
   * Creates an {@link EnterpriseExternalCluster} which can be left un-configured.
   *
   * @param configureCluster whether or not the cluster tool should be run on the started cluster
   * @return a usable cluster
   */
  public EnterpriseExternalCluster build(boolean configureCluster) {
    return new EnterpriseExternalCluster(clusterDirectory, servers, serverJars, namespaceFragment, pluginsFragment,
        tcProperties, serverPropertiesSupplier, securityRootDirectory, configureCluster, clusterName,
        logConfigExtensionResourceName, clientReconnectWindowTime, failoverPriorityVoterCount);
  }

}
