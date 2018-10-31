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

import com.tc.classloader.OverrideService;
import com.terracottatech.connection.BasicMultiConnection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.lease.connection.LeasedConnection;
import org.terracotta.lease.connection.LeasedConnectionService;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Properties;

@OverrideService("org.terracotta.passthrough.connection.PassthroughLeasedConnectionService")
public class EnterprisePassthroughLeasedConnnectionService implements LeasedConnectionService {
  private EnterprisePassthroughConnectionService connectionService = new EnterprisePassthroughConnectionService();
  @Override
  public boolean handlesURI(URI uri) {
    return connectionService.handlesURI(uri);
  }

  @Override
  public boolean handlesConnectionType(String connectionType) {
    return connectionService.handlesConnectionType(connectionType);
  }

  @Override
  public LeasedConnection connect(URI uri, Properties properties) throws ConnectionException {
    return (BasicMultiConnection)connectionService.connect(uri, properties);
  }

  @Override
  public LeasedConnection connect(Iterable<InetSocketAddress> servers, Properties properties) throws ConnectionException {
    return (BasicMultiConnection)connectionService.connect(servers, properties);
  }
}
