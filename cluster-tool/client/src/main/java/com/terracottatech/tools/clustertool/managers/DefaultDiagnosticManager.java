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

import com.terracottatech.connection.HandshakeOrSecurityException;
import com.terracotta.diagnostic.Diagnostics;
import com.terracottatech.tools.clustertool.exceptions.ClusterToolException;
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
import java.util.Collections;
import java.util.Properties;

import static com.terracottatech.tools.client.TopologyEntityProvider.ENTITY_VERSION;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static com.terracottatech.tools.util.ConnectionPropertiesUtil.getConnectionProperties;


public class DefaultDiagnosticManager implements DiagnosticManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDiagnosticManager.class);
  private static final String CONNECTION_TYPE = "diagnostic";

  @Override
  public ConnectionCloseableDiagnosticsEntity getEntity(InetSocketAddress server, String securityRootDirectory) {
    LOGGER.debug("getEntity called with: {}, {}", server, securityRootDirectory);
    Diagnostics diagnostics;
    Connection connection;
    EntityRef<Diagnostics, Object, Properties> ref;

    try {
      Properties properties = getConnectionProperties(null, securityRootDirectory);
      properties.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, CONNECTION_TYPE);
      LOGGER.debug("Calling connect on ConnectionService with: {}, {}", server, properties);
      connection = ConnectionFactory.connect(Collections.singletonList(server), properties);
    } catch (ConnectionException e) {
      String message = "Connection refused from server(s) at: " + server;
      if (e.getCause() instanceof HandshakeOrSecurityException) {
        throw new ClusterToolException(StatusCode.HANDSHAKE_OR_SECURITY_ERROR, message + ". " + e.getCause().getMessage(), e);
      } else {
        throw new ClusterToolException(StatusCode.BAD_GATEWAY, message, e);
      }
    }

    try {
      ref = connection.getEntityRef(Diagnostics.class, ENTITY_VERSION, "root");
    } catch (EntityNotProvidedException e) {
      throw new ClusterToolException(StatusCode.NOT_IMPLEMENTED, e);
    }
    try {
      diagnostics = ref.fetchEntity(null);
      return new ConnectionCloseableDiagnosticsEntity(diagnostics, connection);
    } catch (EntityNotFoundException | EntityVersionMismatchException e) {
      throw new ClusterToolException(StatusCode.NOT_FOUND, e);
    }
  }

  public static class ConnectionCloseableDiagnosticsEntity implements Closeable {
    private final Diagnostics diagnostics;
    private final Connection connection;

    public ConnectionCloseableDiagnosticsEntity(Diagnostics diagnostics, Connection connection) {
      this.diagnostics = diagnostics;
      this.connection = connection;
    }

    public Diagnostics getDiagnostics() {
      return diagnostics;
    }

    @Override
    public void close() throws IOException {
      LOGGER.debug("close called");
      diagnostics.close();
      connection.close();
    }
  }
}
