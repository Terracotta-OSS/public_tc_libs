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

package com.terracottatech.store.manager;

import com.terracottatech.store.management.ManageableDatasetManager;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.client.ClusteredDatasetManager;
import com.terracottatech.store.client.ConnectionProperties;
import com.terracottatech.store.client.builder.datasetmanager.clustered.ClusteredDatasetManagerBuilderImpl;
import com.terracottatech.store.client.management.ClusteredManageableDatasetManager;
import com.terracottatech.store.client.reconnectable.ReconnectableLeasedConnection;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.ServerBasedClientSideConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.UriBasedClientSideConfiguration;
import com.terracottatech.store.statistics.StatisticsDatasetManager;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class ClusteredDatasetManagerProvider extends AbstractDatasetManagerProvider {

  private static final String CONNECTION_PREFIX = "Store:";

  @Override
  public Set<Type> getSupportedTypes() {
    return Collections.singleton(Type.CLUSTERED);
  }

  @Override
  public ClusteredDatasetManagerBuilder clustered(URI uri) {
    return new ClusteredDatasetManagerBuilderImpl(this, uri);
  }

  @Override
  public ClusteredDatasetManagerBuilder clustered(Iterable<InetSocketAddress> servers) {
    return new ClusteredDatasetManagerBuilderImpl(this, servers);
  }

  @Override
  public ClusteredDatasetManagerBuilder secureClustered(URI uri, Path securityRootDirectory) {
    return new ClusteredDatasetManagerBuilderImpl(this, uri, securityRootDirectory);
  }

  @Override
  public ClusteredDatasetManagerBuilder secureClustered(Iterable<InetSocketAddress> servers, Path securityRootDirectory) {
    return new ClusteredDatasetManagerBuilderImpl(this, servers, securityRootDirectory);
  }

  @Override
  public EmbeddedDatasetManagerBuilder embedded() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetManager using(DatasetManagerConfiguration datasetManagerConfiguration,
                              ConfigurationMode configurationMode) throws StoreException  {
    return using(datasetManagerConfiguration, configurationMode, new Properties());
  }

  public DatasetManager using(DatasetManagerConfiguration datasetManagerConfiguration,
                              ConfigurationMode configurationMode,
                              Properties properties) throws StoreException  {
    if (!(datasetManagerConfiguration instanceof ClusteredDatasetManagerConfiguration)) {
      throw new IllegalArgumentException("Unexpected configuration type: " + datasetManagerConfiguration.getClass());
    }

    ClusteredDatasetManagerConfiguration.ClientSideConfiguration clientSideConfiguration =
        ((ClusteredDatasetManagerConfiguration)datasetManagerConfiguration).getClientSideConfiguration();

    try {
      Connection connection = createConnection(clientSideConfiguration, properties);
      try {
        DatasetManager datasetManager = new ClusteredManageableDatasetManager(
            clientSideConfiguration.getClientAlias(),
            ManageableDatasetManager.newShortUUID(), // at this time, instanceId is not configurable
            clientSideConfiguration.getClientTags(),
            new StatisticsDatasetManager(new ClusteredDatasetManager(connection, clientSideConfiguration)),
            connection);
        ensureDatasets(datasetManager, datasetManagerConfiguration, configurationMode);
        return datasetManager;
      } catch (Throwable t) {
        try {
          connection.close();
        } catch (Throwable tt) {
          t.addSuppressed(tt);
        }
        throw t;
      }
    } catch (ConnectionException e) {
      throw new StoreException(e);
    }
  }

  private Connection createConnection(ClusteredDatasetManagerConfiguration.ClientSideConfiguration clientSideConfiguration,
                                      Properties properties) throws ConnectionException {

    Properties connectionProperties = new Properties();
    connectionProperties.putAll(properties);
    Path securityRootDirectory = clientSideConfiguration.getSecurityRootDirectory();
    if (securityRootDirectory != null) {
      connectionProperties.put(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY, securityRootDirectory.toString());
    }
    connectionProperties.setProperty(ConnectionPropertyNames.CONNECTION_NAME, CONNECTION_PREFIX + clientSideConfiguration.getClientAlias());
    connectionProperties.setProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT, Long.toString(clientSideConfiguration.getConnectTimeout()));

    ReconnectableLeasedConnection.ConnectionSupplier connectionSupplier = () -> {
      if (clientSideConfiguration instanceof UriBasedClientSideConfiguration) {
        return LeasedConnectionFactory.connect(((UriBasedClientSideConfiguration) clientSideConfiguration).getClusterUri(), connectionProperties);
      } else if (clientSideConfiguration instanceof ServerBasedClientSideConfiguration) {
        return LeasedConnectionFactory.connect(((ServerBasedClientSideConfiguration) clientSideConfiguration).getServers(),
            connectionProperties);
      } else {
        throw new IllegalArgumentException("Unknown ClientSideConfiguration: " + clientSideConfiguration.getClass());
      }
    };
    if (Boolean.valueOf(connectionProperties.getProperty(ConnectionProperties.DISABLE_RECONNECT, "false"))) {
      // Reconnect following connection failure is inhibited -- use the Connection directly
      return connectionSupplier.get();
    } else {
      return new ReconnectableLeasedConnection(connectionSupplier, clientSideConfiguration.getReconnectTimeout());
    }
  }
}
