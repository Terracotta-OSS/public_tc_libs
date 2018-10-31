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
package com.terracottatech.testing;

import com.terracottatech.connection.BasicMultiConnection;
import com.terracottatech.connection.disconnect.RelayDisconnectListener;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.ConnectionService;
import org.terracotta.passthrough.PassthroughEndpointConnector;
import org.terracotta.passthrough.PassthroughServer;
import org.terracotta.passthrough.PassthroughServerRegistry;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class EnterprisePassthroughConnectionService implements ConnectionService {

  public static final String SS_SCHEME = "passthrough";
  public static final String MS_SCHEME = "mspassthrough";

  @Override
  public boolean handlesURI(URI uri) {
    return handlesConnectionType(uri.getScheme());
  }

  @Override
  public boolean handlesConnectionType(String connectionType) {
    return SS_SCHEME.equals(connectionType) || MS_SCHEME.equals(connectionType);
  }

  @Override
  public Connection connect(URI uri, Properties properties) throws ConnectionException {
    String scheme = uri.getScheme();
    List<String> servers = Arrays.stream(uri.getAuthority().split(",")).collect(Collectors.toList());
    if (scheme.equals(SS_SCHEME) && servers.size() != 1) {
      throw new AssertionError(SS_SCHEME + " URI scheme only supports a single stripe, i.e.: " + SS_SCHEME +
          "://stripe1,stripe2 format is forbidden. Use " + MS_SCHEME + ":// scheme instead");
    }
    return createMultiConnection(properties, servers);
  }

  @Override
  public Connection connect(Iterable<InetSocketAddress> servers, Properties properties) throws ConnectionException {
    List<String> addresses = new ArrayList<>();
    servers.forEach(server -> addresses.add(server.getHostString())); //Use only host component since passthrough uses only host names
    if (addresses.size() == 1) {
      properties.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, SS_SCHEME);
    } else {
      properties.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, MS_SCHEME);
    }
    return createMultiConnection(properties, addresses);
  }

  private Connection createMultiConnection(Properties properties, Collection<String> servers) throws ConnectionException {
    List<Connection> connections = new ArrayList<>();
    RelayDisconnectListener relayDisconnectListener = new RelayDisconnectListener();

    for (String server : servers) {
      Connection connection = doConnect(server, properties, relayDisconnectListener);
      connections.add(connection);
    }

    BasicMultiConnection multiConnection = new BasicMultiConnection(connections);
    relayDisconnectListener.setDisconnectListener(multiConnection);

    return multiConnection;
  }

  private Connection doConnect(String serverName, Properties properties, RelayDisconnectListener disconnectListener) throws ConnectionException {
    PassthroughEndpointConnector endpointConnector = new DisconnectPassthroughEndpointConnector(disconnectListener);
    PassthroughServer server = PassthroughServerRegistry.getSharedInstance().getServerForName(serverName);
    if (null != server) {
      String connectionName = properties.getProperty(ConnectionPropertyNames.CONNECTION_NAME);
      if (null == connectionName) {
        connectionName = "";
      }
      return server.connectNewClient(connectionName, endpointConnector);
    } else {
      throw new ConnectionException(new UnknownHostException(serverName));
    }
  }

}
