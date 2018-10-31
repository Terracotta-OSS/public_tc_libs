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
package com.terracottatech.testing.delay;

import com.terracotta.connection.EndpointConnector;
import com.terracotta.connection.api.TerracottaConnectionService;
import com.terracottatech.connection.StripeConnectionFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Properties;

public class DelayStripeConnectionFactory implements StripeConnectionFactory {
  private final ClusterDelay clusterDelay;

  public DelayStripeConnectionFactory(ClusterDelay clusterDelay) {
    this.clusterDelay = clusterDelay;
  }

  @Override
  public Connection createStripeConnection(int stripeIndex, EndpointConnector endpointConnector, URI uri, Properties properties) throws ConnectionException {
    return getConnectionService(stripeIndex, endpointConnector).connect(uri, properties);
  }

  @Override
  public Connection createStripeConnection(int stripeIndex, EndpointConnector endpointConnector, Iterable<InetSocketAddress> servers,
                                           Properties properties) throws ConnectionException {
    return getConnectionService(stripeIndex, endpointConnector).connect(servers, properties);
  }

  private TerracottaConnectionService getConnectionService(int stripeIndex, EndpointConnector endpointConnector) {
    StripeDelay stripeDelay = clusterDelay.getStripeDelay(stripeIndex);
    EndpointConnector delayedEndpointConnector = new DelayEndpointConnector(stripeDelay, endpointConnector);
    return new TerracottaConnectionService(delayedEndpointConnector);
  }
}
