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
package com.terracottatech.ehcache.internal.config.xml;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder;
import com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseServerSideConfigurationBuilder;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.config.xml.ClusteringCacheManagerServiceConfigurationParser;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.TimeType;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseSecureCluster;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.NAMESPACE_URI;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.XML_SCHEMA_URI;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.TERRACOTTA_NAMESPACE_PREFIX;
import static org.ehcache.xml.XmlModel.convertToJavaTimeUnit;

public class EnterpriseClusteringCacheManagerServiceConfigurationParser extends ClusteringCacheManagerServiceConfigurationParser {

  private static final String CLUSTER = "cluster";
  private static final String CONNECTION = "connection";
  private static final String CLUSTER_CONNECTION = "cluster-connection";
  private static final String SECURITY_ROOT_DIRECTORY = "security-root-directory";
  private static final String URL = "url";
  private static final String READ_TIMEOUT = "read-timeout";
  private static final String WRITE_TIMEOUT = "write-timeout";
  private static final String CONNECTION_TIMEOUT = "connection-timeout";
  private static final String SERVER_SIDE_CONFIG = "server-side-config";
  private static final String DEFAULT_RESOURCE = "default-resource";
  private static final String SHARED_POOL = "shared-pool";
  private static final String NAME = "name";
  private static final String FROM = "from";
  private static final String UNIT = "unit";
  private static final String RESTARTABLE = "restartable";
  private static final String OFFHEAP_MODE = "offheap-mode";
  private static final String RESTART_IDENTIFIER = "restart-identifier";

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA_URI.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE_URI;
  }

  @Override
  public ServiceCreationConfiguration<ClusteringService> parseServiceCreationConfiguration(final Element fragment) {

    EnterpriseClusteringServiceConfigurationBuilder builder = null;
    EnterpriseServerSideConfigurationBuilder serverConfigBuilder = null;
    if (CLUSTER.equals(fragment.getLocalName())) {
      Duration getTimeout = null, putTimeout = null, connectionTimeout = null;
      final NodeList childNodes = fragment.getChildNodes();
      for (int i = 0; i < childNodes.getLength(); i++) {
        final Node item = childNodes.item(i);
        if (Node.ELEMENT_NODE == item.getNodeType()) {
          if (CONNECTION.equals(item.getLocalName())) {
            URI connectionUri = null;
            final Attr urlAttribute = ((Element)item).getAttributeNode(URL);
            final Attr securityRootDirectoryAttr = ((Element)item).getAttributeNode(SECURITY_ROOT_DIRECTORY);
            final String urlValue = urlAttribute.getValue();
            try {
              connectionUri = new URI(urlValue);
              if (securityRootDirectoryAttr != null) {
                builder = enterpriseSecureCluster(connectionUri, Paths.get(securityRootDirectoryAttr.getValue()));
              } else {
                builder = enterpriseCluster(connectionUri);
              }
            } catch (URISyntaxException e) {
              throw new XmlConfigurationException(
                  String.format("Value of %s attribute on XML configuration element <%s> in <%s> is not a valid URI - '%s'",
                      urlAttribute.getName(), item.getNodeName(), fragment.getTagName(), connectionUri), e);
            }

          } else if (CLUSTER_CONNECTION.equals(item.getLocalName())) {
            List<InetSocketAddress> serverAddresses = new ArrayList<>();
            String clusterTierManager = ((Element) item).getAttribute("cluster-tier-manager");
            final Attr securityRootDirectoryAttr = ((Element)item).getAttributeNode(SECURITY_ROOT_DIRECTORY);

            final NodeList serverNodes = item.getChildNodes();
            for (int j = 0; j < serverNodes.getLength(); j++) {
              final Node serverNode = serverNodes.item(j);
              final String host = ((Element) serverNode).getAttributeNode("host").getValue();
              final Attr port = ((Element) serverNode).getAttributeNode("port");
              InetSocketAddress address;
              if (port == null) {
                address = InetSocketAddress.createUnresolved(host, 0);
              } else {
                String portString = port.getValue();
                address = InetSocketAddress.createUnresolved(host, Integer.parseInt(portString));
              }
              serverAddresses.add(address);
            }

            if (securityRootDirectoryAttr != null) {
              builder = enterpriseSecureCluster(serverAddresses, clusterTierManager, Paths.get(securityRootDirectoryAttr.getValue()));
            } else {
              builder = enterpriseCluster(serverAddresses, clusterTierManager);
            }
          } else if (READ_TIMEOUT.equals(item.getLocalName())) {
            getTimeout = processTimeout(fragment, item, builder);
          } else if (WRITE_TIMEOUT.equals(item.getLocalName())) {
            putTimeout = processTimeout(fragment, item, builder);
          } else if (CONNECTION_TIMEOUT.equals(item.getLocalName())) {
            connectionTimeout = processTimeout(fragment, item, builder);
          } else if (SERVER_SIDE_CONFIG.equals(item.getLocalName())) {
            serverConfigBuilder = processServerSideConfig(item, builder);
          }
        }
      }

      if (serverConfigBuilder == null) {
        if (builder != null) {
          Timeouts timeouts = getTimeouts(getTimeout, putTimeout, connectionTimeout);
          return builder.timeouts(timeouts).build();
        }
      } else {
        return serverConfigBuilder.build();
      }
    }
    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }

  protected Element createUrlElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element urlElement = super.createUrlElement(doc, clusteringServiceConfiguration);
    setSecurityAttribute(urlElement, clusteringServiceConfiguration);
    return urlElement;
  }

  protected Element createConnectionElementWrapper(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element connectionElement = super.createConnectionElementWrapper(doc, clusteringServiceConfiguration);
    setSecurityAttribute(connectionElement, clusteringServiceConfiguration);
    return connectionElement;
  }

  private void setSecurityAttribute(Element element, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Properties clusterConfigProperties = clusteringServiceConfiguration.getProperties();

    if (clusterConfigProperties != null) {
      String securityRootDirectory = (String)clusterConfigProperties.get(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY);
      if (securityRootDirectory != null) {
        element.setAttribute(SECURITY_ROOT_DIRECTORY, securityRootDirectory);
      }
    }
  }

  protected Element processServerSideElements(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element serverSideConfigurationElem = super.processServerSideElements(doc, clusteringServiceConfiguration);
    ServerSideConfiguration serverSideConfiguration = clusteringServiceConfiguration.getServerConfiguration();
    if (serverSideConfiguration != null && serverSideConfiguration instanceof EnterpriseServerSideConfiguration) {
      Element restartableElem = doc.createElement(TERRACOTTA_NAMESPACE_PREFIX + RESTARTABLE);
      EnterpriseServerSideConfiguration enterpriseServerSideConfiguration = (EnterpriseServerSideConfiguration)serverSideConfiguration;
      RestartConfiguration restartConfiguration = enterpriseServerSideConfiguration.getRestartConfiguration();
      String restartableLogRoot = restartConfiguration.getRestartableLogRoot();
      restartableElem.setTextContent(restartableLogRoot);
      String frsIdentifier = restartConfiguration.getFrsIdentifier();
      RestartableOffHeapMode offHeapMode = restartConfiguration.getOffHeapMode();
      if (offHeapMode != null) {
        restartableElem.setAttribute(OFFHEAP_MODE, offHeapMode.name());
      }
      if (frsIdentifier != null) {
        restartableElem.setAttribute(RESTART_IDENTIFIER, frsIdentifier);
      }
      serverSideConfigurationElem.appendChild(restartableElem);
    }
    return serverSideConfigurationElem;
  }

  private Timeouts getTimeouts(Duration getTimeout, Duration putTimeout, Duration connectionTimeout) {
    TimeoutsBuilder builder = TimeoutsBuilder.timeouts();
    if (getTimeout != null) {
      builder.read(getTimeout);
    }
    if(putTimeout != null) {
      builder.write(putTimeout);
    }
    if(connectionTimeout != null) {
      builder.connection(connectionTimeout);
    }
    return builder.build();
  }

  private Duration processTimeout(Element parentElement, Node timeoutNode,
                                                                         EnterpriseClusteringServiceConfigurationBuilder builder) {
    try {
      // <xxx-timeout> is a direct subtype of ehcache:time-type; use JAXB to interpret it
      JAXBContext context = JAXBContext.newInstance(TimeType.class.getPackage().getName());
      Unmarshaller unmarshaller = context.createUnmarshaller();
      JAXBElement<TimeType> jaxbElement = unmarshaller.unmarshal(timeoutNode, TimeType.class);

      TimeType timeType = jaxbElement.getValue();
      BigInteger amount = timeType.getValue();
      if (amount.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
        throw new XmlConfigurationException(
            String.format("Value of XML configuration element <%s> in <%s> exceeds allowed value - %s",
                timeoutNode.getNodeName(), parentElement.getTagName(), amount));
      }
      return Duration.of(amount.longValue(), convertToJavaTimeUnit(timeType.getUnit()));

    } catch (JAXBException e) {
      throw new XmlConfigurationException(e);
    }
  }

  private EnterpriseServerSideConfigurationBuilder processServerSideConfig(Node serverSideConfigElement,
                                                                                  EnterpriseClusteringServiceConfigurationBuilder builder) {
    boolean autoCreate = Boolean.parseBoolean(((Element)serverSideConfigElement).getAttribute("auto-create"));
    EnterpriseServerSideConfigurationBuilder eeServerSideConfigBuilder;
    if (autoCreate) {
      eeServerSideConfigBuilder = builder.autoCreate();
    } else {
      eeServerSideConfigBuilder = builder.expecting();
    }

    final NodeList serverSideNodes = serverSideConfigElement.getChildNodes();
    for (int i = 0; i < serverSideNodes.getLength(); i++) {
      final Node item = serverSideNodes.item(i);
      if (Node.ELEMENT_NODE == item.getNodeType()) {
        String nodeLocalName = item.getLocalName();
        if (DEFAULT_RESOURCE.equals(nodeLocalName)) {
          String defaultServerResource = ((Element)item).getAttribute("from");
          eeServerSideConfigBuilder = eeServerSideConfigBuilder.defaultServerResource(defaultServerResource);
        } else if (SHARED_POOL.equals(nodeLocalName)) {
          Element sharedPoolElement = (Element)item;
          String poolName = sharedPoolElement.getAttribute(NAME);     // required
          Attr fromAttr = sharedPoolElement.getAttributeNode(FROM);   // optional
          String fromResource = (fromAttr == null ? null : fromAttr.getValue());
          Attr unitAttr = sharedPoolElement.getAttributeNode(UNIT);   // optional - default 'B'
          String unit = (unitAttr == null ? "B" : unitAttr.getValue());
          MemoryUnit memoryUnit = MemoryUnit.valueOf(unit.toUpperCase(Locale.ENGLISH));

          String quantityValue = sharedPoolElement.getFirstChild().getNodeValue();
          long quantity;
          try {
            quantity = Long.parseLong(quantityValue);
          } catch (NumberFormatException e) {
            throw new XmlConfigurationException("Magnitude of value specified for <shared-pool name=\""
                                                + poolName + "\"> is too large");
          }

          ServerSideConfiguration.Pool poolDefinition;
          if (fromResource == null) {
            poolDefinition = new ServerSideConfiguration.Pool(memoryUnit.toBytes(quantity));
          } else {
            poolDefinition = new ServerSideConfiguration.Pool(memoryUnit.toBytes(quantity), fromResource);
          }
          eeServerSideConfigBuilder = eeServerSideConfigBuilder.resourcePool(poolName, poolDefinition);
        } else if (RESTARTABLE.equals(nodeLocalName)) {
          Element restartableElement = (Element) item;
          String logRoot = restartableElement.getFirstChild().getNodeValue();
          EnterpriseServerSideConfigurationBuilder.RestartableServerSideConfigurationBuilder restartableConfigBuilder =
              eeServerSideConfigBuilder.restartable(logRoot);
          if (!restartableElement.getAttribute(OFFHEAP_MODE).isEmpty()) {
            restartableConfigBuilder = restartableConfigBuilder.withRestartableOffHeapMode(RestartableOffHeapMode.valueOf(restartableElement.getAttribute(OFFHEAP_MODE)));
          }

          if (!restartableElement.getAttribute(RESTART_IDENTIFIER).isEmpty()) {
            restartableConfigBuilder = restartableConfigBuilder.withRestartIdentifier(restartableElement.getAttribute(RESTART_IDENTIFIER));
          }

          eeServerSideConfigBuilder = restartableConfigBuilder;
        }
      }
    }
    return eeServerSideConfigBuilder;
  }
}
