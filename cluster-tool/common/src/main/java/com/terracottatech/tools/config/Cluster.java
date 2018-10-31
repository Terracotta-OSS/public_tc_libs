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
package com.terracottatech.tools.config;


import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


// Terracotta Cluster Configuration.
public final class Cluster implements Serializable{

  public static final String TERRACOTTA_URI_PREFIX = "terracotta://";

  private static final long serialVersionUID = 23000000000231L;

  // Stripes within the cluster
  private final List<Stripe> stripes;

  public Cluster(List<Stripe> stripes) {
    if (stripes == null) {
      throw new NullPointerException("stripes cannot be null");
    }
    this.stripes = new ArrayList<Stripe>(stripes.size());
    Set<Stripe> stripeSet = new HashSet<Stripe>();
    for (Stripe stripe : stripes) {
      if (!stripeSet.contains(stripe)) {
        stripeSet.add(stripe);
        this.stripes.add(stripe);
      } else {
        throw new IllegalArgumentException("Duplicate stripe when creating cluster: " + stripe.toString());
      }
    }
  }

  public List<Stripe> getStripes() {
    return Collections.unmodifiableList(stripes);
  }

  public List<InetSocketAddress> serverInetAddresses() {
    List<InetSocketAddress> hostPorts = new ArrayList<>();
    for (Server server : getServers()) {
      hostPorts.add(InetSocketAddress.createUnresolved(server.getHost(), server.getPort()));
    }
    return hostPorts;
  }

  public List<String> hostPortList() {
    List<String> hostPorts = new ArrayList<String>();
    for (Server server : getServers()) {
      hostPorts.add(server.getHostPort());
    }
    return hostPorts;
  }

  public List<Server> getServers() {
    List<Server> servers = new ArrayList<Server>();
    for (Stripe stripe : this.getStripes()) {
      servers.addAll(stripe.getServers());
    }
    return servers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Cluster cluster = (Cluster) o;

    return stripes.equals(cluster.stripes);
  }

  @Override
  public int hashCode() {
    return stripes.hashCode();
  }

  @Override
  public String toString() {
    return "Cluster{" +
      "stripes=" + stripes +
      '}';
  }
}
