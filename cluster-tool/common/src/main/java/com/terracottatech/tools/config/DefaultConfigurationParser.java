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


import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.terracottatech.tools.config.extractors.BackupRestoreConfigExtractor;
import com.terracottatech.tools.config.extractors.ConfigExtractor;
import com.terracottatech.tools.config.extractors.DataRootConfigExtractor;
import com.terracottatech.tools.config.extractors.FailoverPriorityConfigExtractor;
import com.terracottatech.tools.config.extractors.OffheapConfigExtractor;
import com.terracottatech.tools.config.extractors.PlatformPersistenceConfigExtractor;
import com.terracottatech.tools.config.extractors.SecurityConfigExtractor;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.tools.config.Server.DEFAULT_PORT;
import static com.terracottatech.utilities.HostAndIpValidator.isValidIPv6;

public class DefaultConfigurationParser implements ConfigurationParser {
  private static final String SERVER_EXP = "/tc-config/servers/server";
  private static final String PROPS_EXP = "/tc-config/tc-properties/property";

  private static final String TSA_PORT = "tsa-port";
  private static final String NAME_ATTR = "name";
  private static final String VALUE_ATTR = "value";
  private static final String HOST_ATTR = "host";

  private static final ConfigExtractor[] CONFIG_EXTRACTORS = new ConfigExtractor[]{
      new OffheapConfigExtractor(), new DataRootConfigExtractor(), new PlatformPersistenceConfigExtractor(),
      new SecurityConfigExtractor(), new BackupRestoreConfigExtractor(), new FailoverPriorityConfigExtractor()
  };

  @Override
  public Cluster parseConfigurations(String... tcConfigContents) {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder;
    try {
      builder = factory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    }

    List<Stripe> stripes = new ArrayList<>();
    for (String tcConfigContent : tcConfigContents) {
      boolean validateStrictly = true;
      Document document;
      try {
        document = builder.parse(new ByteArrayInputStream(tcConfigContent.getBytes("UTF-8")));
      } catch (SAXException | IOException e) {
        throw new RuntimeException(e);
      }
      XPath xPath = XPathFactory.newInstance().newXPath();
      List<Stripe.Config<?>> configs = new ArrayList<>();
      List<Server> servers = new ArrayList<>();
      try {
        for (ConfigExtractor configExtractor : CONFIG_EXTRACTORS) {
          NodeList configNodeList = (NodeList) xPath.compile(configExtractor.getXPathExpression()).evaluate(document, XPathConstants.NODESET);
          configs.addAll(configExtractor.extractConfigFromNodes(configNodeList));
        }

        NodeList serverList = (NodeList) xPath.compile(SERVER_EXP).evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < serverList.getLength(); i++) {
          Node serverNode = serverList.item(i);
          Server server = new Server(getName(serverNode), getHost(serverNode), getPort(serverNode));
          String hostname = server.getHost();
          if (hostname == null) {
            throw new IllegalArgumentException("Invalid 'host' attribute value in " + server + ". Reason: 'host' is null");
          } else if (hostname.contains("%")) {
            throw new IllegalArgumentException("Invalid 'host' attribute value in " + server +
                                               ". Reason: 'host' contain substitution parameters");
          }
          servers.add(server);
        }

        if (servers.size() == 0) {
          throw new IllegalArgumentException("No <server> elements found in tc-config:\n" + tcConfigContent);
        }

        NodeList props = (NodeList) xPath.compile(PROPS_EXP).evaluate(document, XPathConstants.NODESET);
        String val = getPropValue(props, "topology.validate");
        if (val != null) {
            validateStrictly = Boolean.valueOf(val);
        }

        stripes.add(new Stripe(servers, validateStrictly, configs.toArray(new Stripe.Config<?>[configs.size()])));

      } catch (XPathExpressionException e) {
        throw new RuntimeException(e);
      }
    }
    return new Cluster(stripes);
  }

  private String getHost(Node server) {
    Node hostNode = server.getAttributes().getNamedItem(HOST_ATTR);
    String hostname = hostNode == null ? null : hostNode.getNodeValue().trim();
    if (hostname != null && isValidIPv6(hostname, false)) {
      //IPv6 address was found
      hostname = "[" + hostname + "]";
    }
    return hostname;
  }

  private String getName(Node server) {
    Node nameNode = server.getAttributes().getNamedItem(NAME_ATTR);
    String hostName = getHost(server);
    String name;
    if (nameNode == null) {
      if (hostName == null) {
        name = null;
      } else {
        name = hostName + ":" + DEFAULT_PORT;
      }
    } else {
      name = nameNode.getNodeValue().trim();
    }
    return name;
  }

  private int getPort(Node serverNode) {
    int port = -1;
    NodeList childNodes = serverNode.getChildNodes();
    for (int j = 0; j < childNodes.getLength(); j++) {
      if (childNodes.item(j).getNodeName().trim().equals(TSA_PORT)) {
        String portString = childNodes.item(j).getTextContent().trim();
        try {
          port = Integer.parseInt(portString);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid tsa-port '" + portString + "' in Server" +
                  "{name='" + getName(serverNode) + "', host='" + getHost(serverNode) + "', port=" + portString + "}");
        }
      }
    }
    if (port == -1) {
      port = DEFAULT_PORT;
    }
    return port;
  }

  private String getPropValue(NodeList tcprop, String target) {
    if (tcprop != null) {
        int len = tcprop.getLength();
        for (int x=0;x<len;x++) {
            Node prop = tcprop.item(x);
            Node name = prop.getAttributes().getNamedItem(NAME_ATTR);
            if (name != null) {
                if (name.getNodeValue().trim().equals(target)) {
                    Node value = prop.getAttributes().getNamedItem(VALUE_ATTR);
                    if (value != null) {
                        return value.getNodeValue().trim();
                    }
                }
            }
        }
    }
    return null;
  }

}
