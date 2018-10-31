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
package com.terracottatech.tools.client;

import com.terracottatech.connection.HandshakeOrSecurityException;
import com.terracottatech.tools.clustertool.exceptions.ClusterToolException;
import com.terracottatech.tools.config.ClusterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static com.terracottatech.tools.util.ConnectionPropertiesUtil.getConnectionProperties;

public class TopologyEntityProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyEntityProvider.class);
  public static final String ENTITY_NAME = "topology-entity";
  private static final Properties DEFAULT_PROPERTIES = new Properties();
  public static final long ENTITY_VERSION = 1L;

  static {
    DEFAULT_PROPERTIES.setProperty(ConnectionPropertyNames.CONNECTION_NAME, "TOPOLOGY");
    DEFAULT_PROPERTIES.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, "stripe");
  }

  public ConnectionCloseableTopologyEntity getEntity(Iterable<InetSocketAddress> servers, String securityRootDirectory) {
    return getEntity(servers, securityRootDirectory, null);
  }

  public ConnectionCloseableTopologyEntity getEntity(Iterable<InetSocketAddress> servers, String securityRootDirectory,
                                                     Properties connectionProperties) {
    LOGGER.debug("getEntity called with: {}, {}, {}", servers, securityRootDirectory, connectionProperties);
    if (connectionProperties != null) {
      DEFAULT_PROPERTIES.putAll(connectionProperties);
    }

    Connection connection;
    try {
      connection = ConnectionFactory.connect(servers, getConnectionProperties(DEFAULT_PROPERTIES, securityRootDirectory));
    } catch (ConnectionException e) {
      String serversList = StreamSupport.stream(servers.spliterator(), false)
          .map(InetSocketAddress::toString)
          .collect(Collectors.joining(","));
      String message = "Connection refused from server(s) at: " + serversList;

      if (e.getCause() instanceof HandshakeOrSecurityException) {
        throw new ClusterToolException(StatusCode.HANDSHAKE_OR_SECURITY_ERROR, message + ". " + e.getCause().getMessage(), e);
      } else {
        throw new ClusterToolException(StatusCode.BAD_GATEWAY, message, e);
      }
    }
    EntityRef<TopologyEntity, ClusterConfiguration, Void> entityRef;
    try {
      entityRef = connection.getEntityRef(TopologyEntity.class, ENTITY_VERSION, ENTITY_NAME);
    } catch (EntityNotProvidedException e) {
      throw new ClusterToolException(StatusCode.NOT_IMPLEMENTED, e);
    }
    try {
      TopologyEntity topologyEntity = entityRef.fetchEntity(null);
      return new ConnectionCloseableTopologyEntity(topologyEntity, connection);
    } catch (EntityNotFoundException | EntityVersionMismatchException e) {
      throw new ClusterToolException(StatusCode.NOT_FOUND, e);
    }
  }

  public static final class ConnectionCloseableTopologyEntity implements Closeable {
    private final TopologyEntity topologyEntity;
    private final Connection connection;

    public ConnectionCloseableTopologyEntity(TopologyEntity topologyEntity, Connection connection) {
      this.topologyEntity = topologyEntity;
      this.connection = connection;
    }

    public TopologyEntity getTopologyEntity() {
      return topologyEntity;
    }

    public Connection getConnection() {
      return connection;
    }

    @Override
    public void close() throws IOException {
      LOGGER.debug("close called");
      try {
        topologyEntity.close();
      } finally {
        connection.close();
      }
    }

    public void release() throws IOException {
      LOGGER.debug("release called");
      try {
        topologyEntity.release();
      } finally {
        connection.close();
      }
    }
  }
}
