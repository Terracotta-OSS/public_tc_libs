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

package com.terracottatech.store.systemtest;

import org.junit.rules.TemporaryFolder;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

public class XmlUtils {
  static URL createXmlConfigFile(String[] hostPorts,
                                 String datasetConfig,
                                 TemporaryFolder temporaryFolder) {
    try {
      StringBuilder clusterConnectionBuilder = new StringBuilder("<cluster-connection>\n");
      for (String hostPort : hostPorts) {
        String[] tokens = hostPort.split(":");
        String host = tokens[0];
        String port = tokens.length == 1 ? "9410" : tokens[1];
        clusterConnectionBuilder.append("<server host=\"");
        clusterConnectionBuilder.append(host);
        clusterConnectionBuilder.append("\" port=\"");
        clusterConnectionBuilder.append(port);
        clusterConnectionBuilder.append("\"/>\n");
      }
      clusterConnectionBuilder.append("</cluster-connection>\n");
      String xmlConfig = "<clustered xmlns=\"http://www.terracottatech.com/v1/terracotta/store/clustered\" " +
                         "xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\" >\n" +
                         clusterConnectionBuilder.toString() +
                         datasetConfig +
                         "</clustered>";

      Path xmlConfigFile = temporaryFolder.newFolder().toPath().resolve("test.xml");
      Files.write(xmlConfigFile, xmlConfig.getBytes());
      return xmlConfigFile.toUri().toURL();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static URL createXmlConfigFileWithComponents(String[] hostPorts,
                                               String connectionTimeout,
                                               String connUnit,
                                               String reconnectionTimeout,
                                               String reConnUnit,
                                               String clientAlias,
                                               String clientTags,
                                               String datasetConfig,
                                               TemporaryFolder temporaryFolder) {
    try {
      StringBuilder clusterConnectionBuilder = new StringBuilder("<cluster-connection>\n");
      for (String hostPort : hostPorts) {
        String[] tokens = hostPort.split(":");
        String host = tokens[0];
        String port = tokens.length == 1 ? "9410" : tokens[1];
        clusterConnectionBuilder.append("<server host=\"");
        clusterConnectionBuilder.append(host);
        clusterConnectionBuilder.append("\" port=\"");
        clusterConnectionBuilder.append(port);
        clusterConnectionBuilder.append("\"/>\n");
      }
      clusterConnectionBuilder.append("<connection-timeout unit=\"" + connUnit + "\">");
      clusterConnectionBuilder.append(connectionTimeout);
      clusterConnectionBuilder.append("</connection-timeout>");
      clusterConnectionBuilder.append("<reconnection-timeout unit=\"" + reConnUnit + "\">");
      clusterConnectionBuilder.append(reconnectionTimeout);
      clusterConnectionBuilder.append("</reconnection-timeout>");
      clusterConnectionBuilder.append("<client-alias>" + clientAlias + "</client-alias>");
      clusterConnectionBuilder.append("<client-tags>" + clientTags + "</client-tags>");
      clusterConnectionBuilder.append("</cluster-connection>\n");
      String xmlConfig = "<clustered xmlns=\"http://www.terracottatech.com/v1/terracotta/store/clustered\" " +
                         "xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\" >\n" +
                         clusterConnectionBuilder.toString() +
                         datasetConfig +
                         "</clustered>";

      Path xmlConfigFile = temporaryFolder.newFolder().toPath().resolve("test.xml");
      Files.write(xmlConfigFile, xmlConfig.getBytes());
      return xmlConfigFile.toUri().toURL();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
