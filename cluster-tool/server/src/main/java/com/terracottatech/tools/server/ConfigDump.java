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
package com.terracottatech.tools.server;

import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;
import org.terracotta.entity.StateDumpCollector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ConfigDump {
  static void dumpConfig(ClusterConfiguration clusterConfiguration, StateDumpCollector dump) {

    Cluster cluster = clusterConfiguration.getCluster();
    List<Stripe> stripes = cluster.getStripes();

    StateDumpCollector clusterDump = dump.subStateDumpCollector("cluster");
    clusterDump.addState("name", clusterConfiguration.getClusterName());
    clusterDump.addState("servers", cluster.serverInetAddresses());

    for(int i=0; i<stripes.size();i++) {
      StateDumpCollector stripeDump = clusterDump.subStateDumpCollector("stripe["+i+"]");

      Map serverMap = new HashMap();
      for(Server server : stripes.get(i).getServers()) {
        serverMap.put("name", server.getName());
        serverMap.put("host", server.getHost());
        serverMap.put("port", String.valueOf(server.getPort()));
      }
      stripeDump.addState("servers", serverMap);

      Map configMap = new HashMap();
      for(Stripe.Config config : stripes.get(i).getConfigs()) {
        configMap.put("name", config.getName());
        configMap.put("type", config.getType());
        configMap.put("value", String.valueOf(config.getValue()));
      }
      stripeDump.addState("config", configMap);

    }
  }
}
