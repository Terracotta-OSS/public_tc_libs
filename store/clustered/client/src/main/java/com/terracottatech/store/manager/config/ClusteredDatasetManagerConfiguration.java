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

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class ClusteredDatasetManagerConfiguration extends AbstractDatasetManagerConfiguration {

  private final ClientSideConfiguration clientSideConfiguration;

  private ClusteredDatasetManagerConfiguration(ClientSideConfiguration clientSideConfiguration, Map<String, DatasetInfo<?>> datasets) {
    super(datasets);
    this.clientSideConfiguration = clientSideConfiguration;
  }

  public ClientSideConfiguration getClientSideConfiguration() {
    return clientSideConfiguration;
  }

  @Override
  public Type getType() {
    return Type.CLUSTERED;
  }

  public static class ClientSideConfiguration {
    private final String clientAlias;
    private final Set<String> clientTags;
    private final long connectTimeout;
    private final long reconnectTimeout;
    private final Path securityRootDirectory;

    ClientSideConfiguration(String clientAlias, Set<String> clientTags, long connectTimeout, long reconnectTimeout, Path securityRootDirectory) {
      this.clientAlias = clientAlias;
      this.clientTags = clientTags;
      this.connectTimeout = connectTimeout;
      this.reconnectTimeout = reconnectTimeout;
      this.securityRootDirectory = securityRootDirectory;
    }

    public String getClientAlias() {
      return clientAlias;
    }

    public Set<String> getClientTags() {
      return clientTags;
    }

    public long getConnectTimeout() {
      return connectTimeout;
    }

    public long getReconnectTimeout() {
      return reconnectTimeout;
    }

    public Path getSecurityRootDirectory() {
      return securityRootDirectory;
    }

    public ClusteredDatasetManagerConfiguration withServerSideConfiguration(Map<String, DatasetInfo<?>> datasets) {
      return new ClusteredDatasetManagerConfiguration(this, datasets);
    }
  }

  public static class ServerBasedClientSideConfiguration extends ClientSideConfiguration {
    private final Iterable<InetSocketAddress> servers;

    public ServerBasedClientSideConfiguration(Iterable<InetSocketAddress> servers, String clientAlias, Set<String> clientTags, long connectTimeout,
                                              long reconnectTimeout, Path securityRootDirectory) {
      super(clientAlias, clientTags, connectTimeout, reconnectTimeout, securityRootDirectory);
      this.servers = servers;
    }

    public Iterable<InetSocketAddress> getServers() {
      return servers;
    }
  }

  public static class UriBasedClientSideConfiguration extends ClientSideConfiguration {
    private final URI clusterUri;

    public UriBasedClientSideConfiguration(URI clusterUri, String clientAlias, Set<String> clientTags, long connectTimeout, long reconnectTimeout,
                                           Path securityRootDirectory) {
      super(clientAlias, clientTags, connectTimeout, reconnectTimeout, securityRootDirectory);
      this.clusterUri = clusterUri;
    }

    public URI getClusterUri() {
      return clusterUri;
    }
  }
}
