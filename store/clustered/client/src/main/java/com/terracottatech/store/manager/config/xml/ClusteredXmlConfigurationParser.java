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
package com.terracottatech.store.manager.config.xml;

import com.terracottatech.store.client.builder.datasetconfiguration.ClusteredDatasetConfigurationBuilder;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.ServerBasedClientSideConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.UriBasedClientSideConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfigurationBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static com.terracottatech.store.manager.xml.util.Misc.getTimeUnit;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getAttribute;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getNodeValue;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getOptionalAttribute;

public class ClusteredXmlConfigurationParser extends AbstractXmlConfigurationParser {

  private static final URL SCHEMA = ClusteredXmlConfigurationParser.class.getResource("/clustered.xsd");
  private static final String NAMESPACE = "http://www.terracottatech.com/v1/terracotta/store/clustered";

  private static final String CLUSTER_CONNECTION_TAG = "cluster-connection";
  private static final String SERVER_TAG = "server";

  private static final String CONNECTION_TIMEOUT_TAG = "connection-timeout";
  private static final String RECONNECTION_TIMEOUT_TAG = "reconnection-timeout";
  private static final String SECURITY_ROOT_DIRECTORY_TAG = "security-root-directory";
  private static final String SERVER_TAG_HOST_ATTRIBUTE = "host";
  private static final String SERVER_TAG_POST_ATTRIBUTE = "port";
  private static final String TIME_UNIT_ATTRIBUTE = "unit";
  private static final String CLIENT_ALIAS = "client-alias";
  private static final String CLIENT_TAGS = "client-tags";
  private ClusteredDatasetManagerConfigurationBuilder clusteredDatasetManagerConfigurationBuilder = null;

  public ClusteredXmlConfigurationParser() {
    super(ClusteredDatasetConfigurationBuilder::new);
  }

  @Override
  protected void parseInternal(Node childNode) {
    switch (childNode.getLocalName()) {
      case CLUSTER_CONNECTION_TAG: {
        clusteredDatasetManagerConfigurationBuilder = parseClusterConnection(childNode);
        break;
      }

      default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                        "validated");
    }
  }

  @Override
  protected DatasetManagerConfiguration build(Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets) {
    if (clusteredDatasetManagerConfigurationBuilder == null) {
      throw new AssertionError(CLUSTER_CONNECTION_TAG + " tag must exists as XML configuration was already " +
                               "validated");
    }
    return clusteredDatasetManagerConfigurationBuilder.withDatasetConfigurations(datasets)
                                                      .build();
  }

  @Override
  protected Document unparseInternal(DatasetManagerConfiguration datasetManagerConfiguration) {
    try {
      if (!(datasetManagerConfiguration instanceof ClusteredDatasetManagerConfiguration)) {
        throw new IllegalArgumentException("Unexpected configuration type: " + datasetManagerConfiguration.getClass());
      }
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document document = builder.newDocument();
      Element documentElement = document.createElementNS(NAMESPACE, Type.CLUSTERED.name().toLowerCase());
      documentElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + DATASET_NAMESPACE_PREFIX, DATASET_NAMESPACE);
      document.appendChild(documentElement);
      ClusteredDatasetManagerConfiguration configuration = (ClusteredDatasetManagerConfiguration)datasetManagerConfiguration;
      ClusteredDatasetManagerConfiguration.ClientSideConfiguration clientSideConfiguration = configuration.getClientSideConfiguration();
      Iterable<InetSocketAddress> servers;
      if (clientSideConfiguration instanceof UriBasedClientSideConfiguration) {
        URI clusterUri = ((UriBasedClientSideConfiguration) clientSideConfiguration).getClusterUri();
        List<InetSocketAddress> inetSocketAddresses = new ArrayList<>();
        String authority = clusterUri.getAuthority();
        for(String hostPort : authority.split(",")) {
          String[] tokens = hostPort.split(":");
          if (tokens.length == 1) {
            inetSocketAddresses.add(InetSocketAddress.createUnresolved(tokens[0], 0));
          } else {
            inetSocketAddresses.add(InetSocketAddress.createUnresolved(tokens[0], Integer.parseInt(tokens[1])));
          }
        }
        servers = inetSocketAddresses;
      } else if (clientSideConfiguration instanceof ServerBasedClientSideConfiguration) {
        servers = ((ServerBasedClientSideConfiguration) clientSideConfiguration).getServers();
      } else {
        throw new IllegalArgumentException("Unknown ClusteredDatasetManagerConfiguration type: " + configuration.getClass());
      }

      Element clusterConnectionElement = document.createElement(CLUSTER_CONNECTION_TAG);
      servers.forEach((inetSocketAddress -> {
        Element serverElement = document.createElement(SERVER_TAG);
        serverElement.setAttribute(SERVER_TAG_HOST_ATTRIBUTE, inetSocketAddress.getHostName());
        serverElement.setAttribute(SERVER_TAG_POST_ATTRIBUTE, String.valueOf(inetSocketAddress.getPort()));
        clusterConnectionElement.appendChild(serverElement);
      }));

      if (clientSideConfiguration.getConnectTimeout() != ClusteredDatasetManagerBuilder.DEFAULT_CONNECTION_TIMEOUT_MS) {
        Element connectionElement = document.createElement(CONNECTION_TIMEOUT_TAG);
        connectionElement.setAttribute(TIME_UNIT_ATTRIBUTE, "MILLIS");
        connectionElement.appendChild(document.createTextNode(String.valueOf(clientSideConfiguration.getConnectTimeout())));
        clusterConnectionElement.appendChild(connectionElement);
      }

      if (clientSideConfiguration.getReconnectTimeout() != ClusteredDatasetManagerBuilder.DEFAULT_RECONNECT_TIMEOUT_MS) {
        Element reconnectionElement = document.createElement(RECONNECTION_TIMEOUT_TAG);
        reconnectionElement.setAttribute(TIME_UNIT_ATTRIBUTE, "MILLIS");
        reconnectionElement.appendChild(document.createTextNode(String.valueOf(clientSideConfiguration.getReconnectTimeout())));
        clusterConnectionElement.appendChild(reconnectionElement);
      }

      Path securityRootDirectory = clientSideConfiguration.getSecurityRootDirectory();
      if (securityRootDirectory != null) {
        Element securityElement = document.createElement(SECURITY_ROOT_DIRECTORY_TAG);
        securityElement.appendChild(document.createTextNode(securityRootDirectory.toString()));
        clusterConnectionElement.appendChild(securityElement);
      }

      if (clientSideConfiguration.getClientAlias() != null) {
        Element clientAliasElement = document.createElement(CLIENT_ALIAS);
        clientAliasElement.appendChild(document.createTextNode(clientSideConfiguration.getClientAlias()));
        clusterConnectionElement.appendChild(clientAliasElement);
      }

      if (clientSideConfiguration.getClientTags() != null && !clientSideConfiguration.getClientTags().isEmpty()) {
        Element clientAliasElement = document.createElement(CLIENT_TAGS);
        clientAliasElement.appendChild(document.createTextNode(clientSideConfiguration.getClientTags().stream().collect(Collectors.joining(","))));
        clusterConnectionElement.appendChild(clientAliasElement);
      }

      documentElement.appendChild(clusterConnectionElement);

      return document;
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<Type> getSupportedTypes() {
    return Collections.singleton(Type.CLUSTERED);
  }

  @Override
  public URL getSchema() {
    return SCHEMA;
  }

  private static ClusteredDatasetManagerConfigurationBuilder parseClusterConnection(Node parentNode) {
    List<InetSocketAddress> servers = new ArrayList<>();
    Path securityRootDirectory = null;
    String connectTimeout = null;
    TimeUnit connectTimeoutUnit = null;
    String reconnectTimeout = null;
    TimeUnit reconnectTimeoutUnit = null;
    String clientAlias = null;
    String clientTags = null;

    NodeList childNodes = parentNode.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node childNode = childNodes.item(i);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case SERVER_TAG: {
            String hostname = getAttribute(childNode, SERVER_TAG_HOST_ATTRIBUTE);
            String port = getOptionalAttribute(childNode, SERVER_TAG_POST_ATTRIBUTE).orElse("0");

            servers.add(InetSocketAddress.createUnresolved(hostname, Integer.parseInt(port)));
            break;
          }

          case CONNECTION_TIMEOUT_TAG: {
            connectTimeout = getNodeValue(childNode);
            connectTimeoutUnit = getTimeUnit(getAttribute(childNode, TIME_UNIT_ATTRIBUTE));
            break;
          }

          case RECONNECTION_TIMEOUT_TAG: {
            reconnectTimeout = getNodeValue(childNode);
            reconnectTimeoutUnit = getTimeUnit(getAttribute(childNode, TIME_UNIT_ATTRIBUTE));
            break;
          }

          case SECURITY_ROOT_DIRECTORY_TAG: {
            securityRootDirectory = Paths.get(getNodeValue(childNode));
            break;
          }

          case CLIENT_ALIAS: {
            clientAlias = getNodeValue(childNode);
            break;
          }

          case CLIENT_TAGS: {
            clientTags = getNodeValue(childNode);
            break;
          }

          default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                            "validated");
        }
      }
    }

    ClusteredDatasetManagerConfigurationBuilder clusteredDatasetManagerConfigurationBuilder =
        new ClusteredDatasetManagerConfigurationBuilder(servers, securityRootDirectory);

    if (connectTimeout != null) {
      clusteredDatasetManagerConfigurationBuilder =
          clusteredDatasetManagerConfigurationBuilder.withConnectionTimeout(Long.parseLong(connectTimeout), connectTimeoutUnit);
    }

    if (reconnectTimeout != null) {
      clusteredDatasetManagerConfigurationBuilder =
          clusteredDatasetManagerConfigurationBuilder.withReconnectTimeout(Long.parseLong(reconnectTimeout), reconnectTimeoutUnit);
    }

    if (clientAlias != null) {
      clusteredDatasetManagerConfigurationBuilder = clusteredDatasetManagerConfigurationBuilder.withClientAlias(clientAlias);
    }

    if (clientTags != null) {
      clusteredDatasetManagerConfigurationBuilder = clusteredDatasetManagerConfigurationBuilder.withClientTags(clientTags.split(","));
    }

    return clusteredDatasetManagerConfigurationBuilder;
  }
}
