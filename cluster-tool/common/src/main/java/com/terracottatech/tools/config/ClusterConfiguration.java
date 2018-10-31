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
import java.util.Collections;
import java.util.List;

public class ClusterConfiguration implements Serializable {

  private static final long serialVersionUID = 23000000000231L;

  private final String clusterName;
  private final Cluster cluster;

  public ClusterConfiguration(Cluster cluster) {
    this(null, cluster);
  }

  public ClusterConfiguration(String clusterName, Cluster cluster) {
    this(clusterName, cluster.getStripes());
  }

  public ClusterConfiguration(String clusterName, List<Stripe> stripesConfigs) {
    if (stripesConfigs == null) {
      throw new NullPointerException("stripesConfigs cannot be null");
    }
    this.clusterName = clusterName;
    for (Stripe stripeConfig : stripesConfigs) {
      if (stripeConfig.getServers().isEmpty()) {
        throw new IllegalArgumentException("Stripe config must have at least 1 server !");
      }
    }
    this.cluster = new Cluster(Collections.unmodifiableList(stripesConfigs));
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public Cluster getCluster() {
    return this.cluster;
  }

  @Override
  public String toString() {
    return "ClusterConfiguration{" +
        "clusterName='" + clusterName + '\'' +
        ", cluster=" + cluster +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ClusterConfiguration that = (ClusterConfiguration) o;

    if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) return false;
    return cluster != null ? cluster.equals(that.cluster) : that.cluster == null;
  }

  @Override
  public int hashCode() {
    int result = clusterName != null ? clusterName.hashCode() : 0;
    result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
    return result;
  }
}
