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

package com.terracottatech.store.manager.config;

import com.terracottatech.store.management.ManageableDatasetManager;
import com.terracottatech.store.manager.DatasetManagerConfiguration.DatasetInfo;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.ServerBasedClientSideConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.UriBasedClientSideConfiguration;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.manager.ClusteredDatasetManagerBuilder.DEFAULT_CONNECTION_TIMEOUT_MS;
import static com.terracottatech.store.manager.ClusteredDatasetManagerBuilder.DEFAULT_RECONNECT_TIMEOUT_MS;

public class ClusteredDatasetManagerConfigurationBuilder {
  private final URI clusterUri;
  private final Iterable<InetSocketAddress> servers;
  private final Path securityRootDirectory;
  private final long connectTimeout;
  private final long reconnectTimeout;
  private final String alias;
  // Please keep a linked hash set here. Reason: https://github.com/Terracotta-OSS/terracotta-platform/issues/470
  private final Set<String> tags = new LinkedHashSet<>();
  private final Map<String, DatasetInfo<?>> datasets = new ConcurrentHashMap<>();

  public ClusteredDatasetManagerConfigurationBuilder(URI clusterUri) {
    this(clusterUri, null);
  }

  public ClusteredDatasetManagerConfigurationBuilder(URI clusterUri, Path securityRootDirectory) {
    this.clusterUri = clusterUri;
    this.servers = null;
    this.securityRootDirectory = securityRootDirectory;
    this.connectTimeout = TimeUnit.MILLISECONDS.toMillis(DEFAULT_CONNECTION_TIMEOUT_MS);
    this.reconnectTimeout = TimeUnit.MILLISECONDS.toMillis(DEFAULT_RECONNECT_TIMEOUT_MS);
    this.alias = ManageableDatasetManager.newShortUUID();
  }

  public ClusteredDatasetManagerConfigurationBuilder(Iterable<InetSocketAddress> servers) {
    this(servers, null);
  }

  public ClusteredDatasetManagerConfigurationBuilder(Iterable<InetSocketAddress> servers, Path securityRootDirectory) {
    this.clusterUri = null;
    List<InetSocketAddress> serverList = new ArrayList<>();
    servers.forEach(serverList::add);
    this.servers = serverList;
    this.securityRootDirectory = securityRootDirectory;
    this.connectTimeout = TimeUnit.MILLISECONDS.toMillis(DEFAULT_CONNECTION_TIMEOUT_MS);
    this.reconnectTimeout = TimeUnit.MILLISECONDS.toMillis(DEFAULT_RECONNECT_TIMEOUT_MS);
    this.alias = ManageableDatasetManager.newShortUUID();
  }

  private ClusteredDatasetManagerConfigurationBuilder(ClusteredDatasetManagerConfigurationBuilder original, long connectTimeout, long reconnectTimeout) {
    this.clusterUri = original.clusterUri;
    this.servers = original.servers;
    this.securityRootDirectory = original.securityRootDirectory;
    this.alias = original.alias;
    this.tags.addAll(original.tags);
    this.datasets.putAll(original.datasets);
    this.connectTimeout = connectTimeout;
    this.reconnectTimeout = reconnectTimeout;
  }

  private ClusteredDatasetManagerConfigurationBuilder(ClusteredDatasetManagerConfigurationBuilder original, String alias, Set<String> tags) {
    this.clusterUri = original.clusterUri;
    this.servers = original.servers;
    this.securityRootDirectory = original.securityRootDirectory;
    this.connectTimeout = original.connectTimeout;
    this.reconnectTimeout = original.reconnectTimeout;
    this.datasets.putAll(original.datasets);
    this.alias = Objects.requireNonNull(alias);
    this.tags.addAll(tags);
  }

  private ClusteredDatasetManagerConfigurationBuilder(ClusteredDatasetManagerConfigurationBuilder original, Map<String, DatasetInfo<?>> datasets) {
    this.clusterUri = original.clusterUri;
    this.servers = original.servers;
    this.securityRootDirectory = original.securityRootDirectory;
    this.connectTimeout = original.connectTimeout;
    this.reconnectTimeout = original.reconnectTimeout;
    this.alias = original.alias;
    this.tags.addAll(original.tags);
    this.datasets.putAll(datasets);
  }

  public ClusteredDatasetManagerConfigurationBuilder withConnectionTimeout(long timeout, TimeUnit unit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("connect timeout must be non-negative");
    }
    return new ClusteredDatasetManagerConfigurationBuilder(this, unit.toMillis(timeout), this.reconnectTimeout);
  }

  public ClusteredDatasetManagerConfigurationBuilder withReconnectTimeout(long timeout, TimeUnit unit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("reconnect timeout must be non-negative");
    }
    return new ClusteredDatasetManagerConfigurationBuilder(this, this.connectTimeout, unit.toMillis(timeout));
  }

  public ClusteredDatasetManagerConfigurationBuilder withClientAlias(String alias) {
    return new ClusteredDatasetManagerConfigurationBuilder(this, alias, tags);
  }

  public ClusteredDatasetManagerConfigurationBuilder withClientTags(String... tags) {
    return withClientTags(new LinkedHashSet<>(Arrays.asList(tags)));
  }

  public ClusteredDatasetManagerConfigurationBuilder withClientTags(Set<String> tags) {
    return new ClusteredDatasetManagerConfigurationBuilder(this, alias, tags);
  }

  public ClusteredDatasetManagerConfigurationBuilder withDatasetConfigurations(Map<String, DatasetInfo<?>> datasets) {
    return new ClusteredDatasetManagerConfigurationBuilder(this, datasets);
  }

  public ClusteredDatasetManagerConfiguration build() {
    if (clusterUri != null) {
      return new UriBasedClientSideConfiguration(clusterUri, alias, tags, connectTimeout, reconnectTimeout,
                                                                                      securityRootDirectory).withServerSideConfiguration(datasets);
    } else {
      return new ServerBasedClientSideConfiguration(servers, alias, tags, connectTimeout, reconnectTimeout,
                                                                                         securityRootDirectory).withServerSideConfiguration(datasets);
    }
  }
}