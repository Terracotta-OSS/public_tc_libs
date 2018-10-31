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
package com.terracottatech.connection;

import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.ConnectionService;

import com.terracotta.connection.api.DetailedConnectionException;
import com.terracotta.connection.api.DiagnosticConnectionService;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Properties;

/**
 * Enterprise version of {@link DiagnosticConnectionService} which can throw the enterprise-only {@link HandshakeOrSecurityException}
 */
public class EnterpriseDiagnosticConnectionService implements ConnectionService {
  private static final String CONNECTION_TYPE = "diagnostic";

  @Override
  public boolean handlesURI(URI uri) {
    return handlesConnectionType(uri.getScheme());
  }

  @Override
  public boolean handlesConnectionType(String connectionType) {
    return connectionType.equals(CONNECTION_TYPE);
  }

  @Override
  public final Connection connect(URI uri, Properties properties) throws ConnectionException {
    ConnectionService connectionService = new DiagnosticConnectionService();

    try {
      return connectionService.connect(uri, properties);
    } catch (DetailedConnectionException e) {
      throw ExceptionUtils.getConnectionException(e, properties);
    }
  }

  @Override
  public final Connection connect(Iterable<InetSocketAddress> servers, Properties properties) throws ConnectionException {
    ConnectionService connectionService = new DiagnosticConnectionService();
    properties.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, CONNECTION_TYPE);
    try {
      return connectionService.connect(servers, properties);
    } catch (DetailedConnectionException e) {
      throw ExceptionUtils.getConnectionException(e, properties);
    }
  }
}
