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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.config.Configuration;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;

import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseCluster;
import static com.terracottatech.ehcache.clustered.client.config.builders.EnterpriseClusteringServiceConfigurationBuilder.enterpriseSecureCluster;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EnterpriseClusteringCacheManagerServiceConfigurationParserTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void testServiceLocatable() {
    String expectedParser = EnterpriseClusteringCacheManagerServiceConfigurationParser.class.getName();
    @SuppressWarnings("rawtypes")
    final ServiceLoader<CacheManagerServiceConfigurationParser> parsers =
        ClassLoading.libraryServiceLoaderFor(CacheManagerServiceConfigurationParser.class);
    boolean found = false;
    for (final CacheManagerServiceConfigurationParser<?> parser : parsers) {
      if (parser.getClass().getName().equals(expectedParser)) {
        found = true;
        break;
      }
    }
    if (!found) {
      fail("Expected Parser Not Found");
    }
  }

  @Test
  public void testTargetNameSpace() throws Exception {
    EnterpriseClusteringCacheManagerServiceConfigurationParser parserUnderTest = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    StreamSource schemaSource = (StreamSource) parserUnderTest.getXmlSchema();

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder domBuilder = factory.newDocumentBuilder();
    Element schema = domBuilder.parse(schemaSource.getInputStream()).getDocumentElement();
    Attr targetNamespaceAttr = schema.getAttributeNode("targetNamespace");

    assertThat(targetNamespaceAttr, is(not(nullValue())));
    assertThat(targetNamespaceAttr.getValue(), is(parserUnderTest.getNamespace().toString()));
  }

  @Test(expected = XmlConfigurationException.class)
  public void testUrlAndServers() throws Exception {
    final String[] config = new String[]
        {
            "<ehcache:config",
            "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
            "    xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">",
            "",
            "  <ehcache:service>",
            "    <tc:cluster>",
            "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" />",
            "      <tc:cluster-connection cluster-tier-manager=\"cM\">",
            "        <tc:server host=\"blah\" port=\"1234\" />",
            "      </tc:cluster-connection>",
            "    </tc:cluster>",
            "  </ehcache:service>",
            "",
            "</ehcache:config>"
        };

    new XmlConfiguration(makeConfig(config));
  }

  @Test(expected = XmlConfigurationException.class)
  public void testServersOnly() throws Exception {
    final String[] config = new String[]
        {
            "<ehcache:config",
            "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
            "    xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">",
            "",
            "  <ehcache:service>",
            "    <tc:cluster>",
            "      <tc:cluster-connection>",
            "        <tc:server host=\"blah\" port=\"1234\" />",
            "      </tc:cluster-connection>",
            "    </tc:cluster>",
            "  </ehcache:service>",
            "",
            "</ehcache:config>"
        };

    new XmlConfiguration(makeConfig(config));
  }

  @Test
  public void testServersWithClusterTierManagerAndSecurityRootDirectory() throws Exception {
    final String[] config = new String[]
        {
            "<ehcache:config",
            "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
            "    xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">",
            "",
            "  <ehcache:service>",
            "    <tc:cluster>",
            "      <tc:cluster-connection cluster-tier-manager=\"cM\" security-root-directory=\"secure-root\">",
            "        <tc:server host=\"server-1\" port=\"9510\" />",
            "        <tc:server host=\"server-2\" port=\"9540\" />",
            "      </tc:cluster-connection>",
            "    </tc:cluster>",
            "  </ehcache:service>",
            "",
            "</ehcache:config>"
        };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));
    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations = configuration.getServiceCreationConfigurations();
    ClusteringServiceConfiguration clusteringServiceConfiguration =
        ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    ConnectionSource.ServerList connectionSource = (ConnectionSource.ServerList) clusteringServiceConfiguration.getConnectionSource();
    Iterable<InetSocketAddress> servers = connectionSource.getServers();

    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("server-1", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 9540);
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer);

    assertThat(connectionSource.getClusterTierManager(), is("cM"));
    assertThat(servers, is(expectedServers));
  }

  @Test
  public void testServersWithClusterTierManagerAndOptionalPorts() throws Exception {
    final String[] config = new String[]
        {
            "<ehcache:config",
            "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
            "    xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">",
            "",
            "  <ehcache:service>",
            "    <tc:cluster>",
            "      <tc:cluster-connection cluster-tier-manager=\"cM\">",
            "        <tc:server host=\"100.100.100.100\" port=\"9510\" />",
            "        <tc:server host=\"server-2\" />",
            "        <tc:server host=\"[::1]\" />",
            "        <tc:server host=\"[fe80::1453:846e:7be4:15fe]\" port=\"9710\" />",
            "      </tc:cluster-connection>",
            "    </tc:cluster>",
            "  </ehcache:service>",
            "",
            "</ehcache:config>"
        };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));
    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations = configuration.getServiceCreationConfigurations();
    ClusteringServiceConfiguration clusteringServiceConfiguration =
        ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    ConnectionSource.ServerList connectionSource = (ConnectionSource.ServerList) clusteringServiceConfiguration.getConnectionSource();
    Iterable<InetSocketAddress> servers = connectionSource.getServers();

    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("100.100.100.100", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 0);
    InetSocketAddress thirdServer = InetSocketAddress.createUnresolved("[::1]", 0);
    InetSocketAddress fourthServer = InetSocketAddress.createUnresolved("[fe80::1453:846e:7be4:15fe]", 9710);
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer, thirdServer, fourthServer);

    assertThat(connectionSource.getClusterTierManager(), is("cM"));
    assertThat(servers, is(expectedServers));
  }

  @Test
  public void testSecurityElementWithIpv6Address() {
    String securityRootDirectory = TEMPORARY_FOLDER.getRoot().getAbsolutePath();

    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("100.100.100.100", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 0);
    InetSocketAddress thirdServer = InetSocketAddress.createUnresolved("[::1]", 0);
    InetSocketAddress fourthServer = InetSocketAddress.createUnresolved("[fe80::1453:846e:7be4:15fe]", 9710);
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer, thirdServer, fourthServer);

    ClusteringServiceConfiguration csc = enterpriseSecureCluster(expectedServers, "my-application",
        Paths.get(TEMPORARY_FOLDER.getRoot().getAbsolutePath())).build();
    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    NamedNodeMap attributes = element.getFirstChild().getAttributes();
    assertThat(attributes.getLength(), is(2));
    assertThat(extractSecurityRootDirectory(attributes), is(securityRootDirectory));
  }

  @Test
  public void testSecurityElement() {
    String securityRootDirectory = TEMPORARY_FOLDER.getRoot().getAbsolutePath();
    ClusteringServiceConfiguration csc = enterpriseSecureCluster(CLUSTER_URI, Paths.get(securityRootDirectory)).build();

    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    NamedNodeMap attributes = element.getFirstChild().getAttributes();
    assertThat(attributes.getLength(), is(2));
    assertThat(extractSecurityRootDirectory(attributes), is(securityRootDirectory));
  }

  @Test
  public void testNoSecurityElement() {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).build();
    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    NamedNodeMap attributes = element.getFirstChild().getAttributes();
    assertThat(attributes.getLength(), is(1));
    assertThat(extractSecurityRootDirectory(attributes), is(nullValue()));
  }

  @Test
  public void testNoSecurityElementWithIpv6Address() {
    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("100.100.100.100", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 0);
    InetSocketAddress thirdServer = InetSocketAddress.createUnresolved("[::1]", 0);
    InetSocketAddress fourthServer = InetSocketAddress.createUnresolved("[fe80::1453:846e:7be4:15fe]", 9710);
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer, thirdServer, fourthServer);

    ClusteringServiceConfiguration csc = enterpriseCluster(expectedServers, "my-application").build();
    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    NamedNodeMap attributes = element.getFirstChild().getAttributes();
    assertThat(attributes.getLength(), is(1));
    assertThat(extractSecurityRootDirectory(attributes), is(nullValue()));
  }

  @Test
  public void testRestartableElementGenerationWithRestartIdentifier() {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).autoCreate().restartable("root1").
        withRestartIdentifier("frsmgr1").build();
    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    assertThat(element, is(notNullValue()));
    assertThat(element.getFirstChild(), is(notNullValue()));

    Node readTimeoutElement = element.getFirstChild().getNextSibling();
    assertThat(readTimeoutElement, is(notNullValue()));
    Node writeTimeout = readTimeoutElement.getNextSibling();
    assertThat(writeTimeout, is(notNullValue()));
    Node connTimeoutElement = writeTimeout.getNextSibling();
    assertThat(connTimeoutElement, is(notNullValue()));

    Node serverSideElement = connTimeoutElement.getNextSibling();
    assertThat(serverSideElement, is(notNullValue()));
    Node restartable = serverSideElement.getFirstChild();
    assertThat(restartable, is(notNullValue()));
    String nodeText = restartable.getTextContent();
    assertThat(nodeText, is(notNullValue()));
    assertThat(nodeText, is(equalTo("root1")));

    NamedNodeMap attributes = restartable.getAttributes();
    assertThat(attributes, is(notNullValue()));
    assertThat(attributes.getLength(), is(equalTo(2)));
    assertOnRestartableAttributes(attributes, RestartableOffHeapMode.FULL);
  }

  @Test
  public void testRestartableElementGeneration() {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).
        autoCreate().
        restartable("root1").
        withRestartableOffHeapMode(RestartableOffHeapMode.PARTIAL).
        withRestartIdentifier("frsmgr1").build();

    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    assertThat(element, is(notNullValue()));
    assertThat(element.getFirstChild(), is(notNullValue()));

    Node readTimeoutElement = element.getFirstChild().getNextSibling();
    assertThat(readTimeoutElement, is(notNullValue()));
    Node writeTimeout = readTimeoutElement.getNextSibling();
    assertThat(writeTimeout, is(notNullValue()));
    Node connTimeoutElement = writeTimeout.getNextSibling();
    assertThat(connTimeoutElement, is(notNullValue()));

    Node serverSideElement = connTimeoutElement.getNextSibling();
    assertThat(serverSideElement, is(notNullValue()));
    Node restartable = serverSideElement.getFirstChild();
    assertThat(restartable, is(notNullValue()));
    String nodeText = restartable.getTextContent();
    assertThat(nodeText, is(notNullValue()));
    assertThat(nodeText, is(equalTo("root1")));

    NamedNodeMap attributes = restartable.getAttributes();
    assertThat(attributes, is(notNullValue()));
    assertThat(attributes.getLength(), is(equalTo(2)));
    assertOnRestartableAttributes(attributes, RestartableOffHeapMode.PARTIAL);
  }

  @Test
  public void testRestartableElementGenerationWithExplicitOffHeapModeAndRestartIdentifier() {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).
        autoCreate().
        defaultServerResource("defaultResource").
        restartable("root1").
        withRestartableOffHeapMode(RestartableOffHeapMode.PARTIAL).
        withRestartIdentifier("frsmgr1").build();

    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    assertThat(element, is(notNullValue()));
    assertThat(element.getFirstChild(), is(notNullValue()));

    Node readTimeoutElement = element.getFirstChild().getNextSibling();
    assertThat(readTimeoutElement, is(notNullValue()));
    Node writeTimeout = readTimeoutElement.getNextSibling();
    assertThat(writeTimeout, is(notNullValue()));
    Node connTimeoutElement = writeTimeout.getNextSibling();
    assertThat(connTimeoutElement, is(notNullValue()));

    Node serverSideElement = connTimeoutElement.getNextSibling();
    assertThat(serverSideElement, is(notNullValue()));
    Node defaultResource = serverSideElement.getFirstChild();
    assertThat(defaultResource, is(notNullValue()));
    Node restartable = defaultResource.getNextSibling();
    assertThat(restartable, is(notNullValue()));
    String nodeText = restartable.getTextContent();
    assertThat(nodeText, is(notNullValue()));
    assertThat(nodeText, is(equalTo("root1")));

    NamedNodeMap attributes = restartable.getAttributes();
    assertThat(attributes, is(notNullValue()));
    assertThat(attributes.getLength(), is(equalTo(2)));
    assertOnRestartableAttributes(attributes, RestartableOffHeapMode.PARTIAL);
  }

  @Test
  public void testNoRestartableElement() {
    ClusteringServiceConfiguration csc = enterpriseCluster(CLUSTER_URI).
        autoCreate().
        defaultServerResource("defaultResource").
        build();

    EnterpriseClusteringCacheManagerServiceConfigurationParser parser = new EnterpriseClusteringCacheManagerServiceConfigurationParser();
    Element element = parser.unparseServiceCreationConfiguration(csc);
    assertThat(element, is(notNullValue()));
    assertThat(element.getFirstChild(), is(notNullValue()));

    Node readTimeoutElement = element.getFirstChild().getNextSibling();
    assertThat(readTimeoutElement, is(notNullValue()));
    Node writeTimeout = readTimeoutElement.getNextSibling();
    assertThat(writeTimeout, is(notNullValue()));
    Node connTimeoutElement = writeTimeout.getNextSibling();
    assertThat(connTimeoutElement, is(notNullValue()));

    Node serverSideElement = connTimeoutElement.getNextSibling();
    assertThat(serverSideElement, is(notNullValue()));
    Node defaultResource = serverSideElement.getFirstChild();
    assertThat(defaultResource, is(notNullValue()));
    Node restartable = defaultResource.getNextSibling();
    assertThat(restartable, is(nullValue()));
  }

  private URL makeConfig(final String[] lines) throws IOException {
    final File configFile = TEMPORARY_FOLDER.newFile(testName.getMethodName() + "_config.xml");
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(configFile), "UTF-8")) {
      for (final String line : lines) {
        out.write(line);
      }
    }

    return configFile.toURI().toURL();
  }

  private String extractSecurityRootDirectory(NamedNodeMap attributes) {
    for (int i = 0; i < attributes.getLength(); i++) {
      Node attributeNode = attributes.item(i);
      if ("security-root-directory".equals(attributeNode.getNodeName())) {
        return attributeNode.getNodeValue();
      }
    }
    return null;
  }

  private void assertOnRestartableAttributes(NamedNodeMap attributes, RestartableOffHeapMode restartableOffHeapMode) {
    for (int i = 0; i < attributes.getLength(); i++) {
      Node attributeNode = attributes.item(i);
      switch (attributeNode.getNodeName()) {
        case "offheap-mode":
          assertThat(attributeNode.getNodeValue(), is(equalTo(restartableOffHeapMode.name())));
          break;
        case "restart-identifier":
          assertThat(attributeNode.getNodeValue(), is(equalTo("frsmgr1")));
          break;
        default:
          fail("Unexpected attribute:" + attributeNode.getNodeName());
          break;
      }
    }
  }
}
