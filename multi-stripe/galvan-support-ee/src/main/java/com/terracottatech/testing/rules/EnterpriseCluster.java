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

import org.junit.rules.ExternalResource;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.passthrough.IClusterControl;

import java.net.URI;

public abstract class EnterpriseCluster extends ExternalResource {

  /**
   * Returns the connection URI using the terracotta:// scheme.
   *
   * @return a URI to connect clients to the cluster
   */
  public abstract URI getConnectionURI();

  /**
   * Returns the connection URI to connect to a single stripe, using the stripe:// scheme.
   *
   * @param stripeIndex Index of the stripe to connect to
   *
   * @return a URI to connect to the stripe
   *
   */
  public abstract URI getStripeConnectionURI(int stripeIndex);

  /**
   * Returns the connection URI to connect to the cluster, using the cluster:// scheme.
   *
   * @return a URI to connect clients to the cluster
   */
  public abstract URI getClusterConnectionURI();

  /**
   * Returns the host:port definition of all servers in the cluster
   *
   * @return an array of host:port definitions
   */
  public abstract String[] getClusterHostPorts();

  public abstract Connection newConnection() throws ConnectionException;

  public abstract IClusterControl getClusterControl();

  public abstract IClusterControl getStripeControl(int stripeIndex);
}
