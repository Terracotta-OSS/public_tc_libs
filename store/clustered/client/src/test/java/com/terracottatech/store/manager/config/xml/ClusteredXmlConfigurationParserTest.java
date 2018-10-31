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

import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import org.junit.Test;
import org.w3c.dom.Document;

import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.ServerBasedClientSideConfiguration;
import com.terracottatech.store.manager.xml.exception.XmlConfigurationException;
import com.terracottatech.store.manager.xml.util.XmlUtils;

import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.Diff;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static com.terracottatech.store.manager.xml.util.XmlUtils.getValidatedDocument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ClusteredXmlConfigurationParserTest {

  private static final String TEST_HOST_NAME1 = "localhost";
  private static final int TEST_HOST_PORT1 = 1990;
  private static final String TEST_HOST_NAME2 = "localhost";
  private static final int TEST_HOST_PORT2 = 1440;
  private static final long TEST_CONNECTION_TIME = 10;
  private static final long TEST_RECONNECTION_TIME = 20;
  private static final Path TEST_SRD_PATH = Paths.get("/path/to/srd");
  private static final String TEST_CLIENT_ALIAS = "test-alias";
  private static final String TEST_CLIENT_TAGS = "test-tag-1,test-tag-2";

  private static final String TEST_DATASET_NAME = "test-dataset";
  private static final String TEST_DATASET_KEY_TYPE = "STRING";
  private static final String TEST_DATASET_OFFHEAP_RESOURCE = "test-offheap";

  @Test
  public void testParseWithFullConfig() throws Exception {
    String xmlConfig = "<clustered xmlns=\"http://www.terracottatech.com/v1/terracotta/store/clustered\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <cluster-connection>\n" +
                       "    <server host=\"" + TEST_HOST_NAME1 + "\" port=\"" + TEST_HOST_PORT1 + "\"/>\n" +
                       "    <server host=\"" + TEST_HOST_NAME2 + "\" port=\"" + TEST_HOST_PORT2 + "\"/>\n" +
                       "    <connection-timeout unit=\"MILLIS\">" + TEST_CONNECTION_TIME + "</connection-timeout>\n" +
                       "    <reconnection-timeout unit=\"MILLIS\">" + TEST_RECONNECTION_TIME + "</reconnection-timeout>\n" +
                       "    <security-root-directory>" + TEST_SRD_PATH + "</security-root-directory>\n" +
                       "    <client-alias>" + TEST_CLIENT_ALIAS + "</client-alias>\n" +
                       "    <client-tags>" + TEST_CLIENT_TAGS + "</client-tags>\n" +
                       "  </cluster-connection>\n" +
                       "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                       "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                       "  </dataset>\n" +
                       "</clustered>";

    ClusteredXmlConfigurationParser parser = new ClusteredXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    ClusteredDatasetManagerConfiguration datasetManagerConfiguration = (ClusteredDatasetManagerConfiguration)parser.parse(document);
    ClusteredDatasetManagerConfiguration.ClientSideConfiguration clientSideConfiguration = datasetManagerConfiguration.getClientSideConfiguration();
    assertThat(clientSideConfiguration).isInstanceOf(ServerBasedClientSideConfiguration.class);
    ServerBasedClientSideConfiguration configuration = (ServerBasedClientSideConfiguration) clientSideConfiguration;
    assertThat(configuration.getServers()).containsExactly(InetSocketAddress.createUnresolved(TEST_HOST_NAME1, TEST_HOST_PORT1),
                                                           InetSocketAddress.createUnresolved(TEST_HOST_NAME2, TEST_HOST_PORT2));
    assertThat(configuration.getConnectTimeout()).isEqualTo(TEST_CONNECTION_TIME);
    assertThat(configuration.getReconnectTimeout()).isEqualTo(TEST_RECONNECTION_TIME);
    assertThat(configuration.getSecurityRootDirectory()).isEqualTo(TEST_SRD_PATH);
    assertThat(configuration.getClientAlias()).isEqualTo(TEST_CLIENT_ALIAS);
    assertThat(configuration.getClientTags()).isEqualTo(new HashSet<>(Arrays.asList(TEST_CLIENT_TAGS.split(","))));

    Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
    assertThat(datasets.size()).isEqualTo(1);
    assertThat(datasets.keySet().iterator().next()).isEqualTo(TEST_DATASET_NAME);
  }

  @Test
  public void testParseWithOptionalConfig() throws Exception {
    String xmlConfig = "<clustered xmlns=\"http://www.terracottatech.com/v1/terracotta/store/clustered\">\n" +
                       "  <cluster-connection>\n" +
                       "    <server host=\"" + TEST_HOST_NAME1 + "\" port=\"" + TEST_HOST_PORT1 + "\"/>\n" +
                       "  </cluster-connection>\n" +
                       "</clustered>";
    ClusteredXmlConfigurationParser parser = new ClusteredXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    ClusteredDatasetManagerConfiguration datasetManagerConfiguration = (ClusteredDatasetManagerConfiguration)parser.parse(document);
    ClusteredDatasetManagerConfiguration.ClientSideConfiguration clientSideConfiguration = datasetManagerConfiguration.getClientSideConfiguration();
    assertThat(clientSideConfiguration).isInstanceOf(ServerBasedClientSideConfiguration.class);
    ServerBasedClientSideConfiguration configuration = (ServerBasedClientSideConfiguration) clientSideConfiguration;
    assertThat(configuration.getServers()).containsExactly(InetSocketAddress.createUnresolved(TEST_HOST_NAME1, TEST_HOST_PORT1));
    assertThat(configuration.getConnectTimeout()).isEqualTo(ClusteredDatasetManagerBuilder.DEFAULT_CONNECTION_TIMEOUT_MS);
    assertThat(configuration.getReconnectTimeout()).isEqualTo(ClusteredDatasetManagerBuilder.DEFAULT_RECONNECT_TIMEOUT_MS);
    assertThat(configuration.getSecurityRootDirectory()).isNull();
    assertThat(datasetManagerConfiguration.getDatasets()).isEmpty();
  }

  @Test
  public void testDatasetConfigsWithSameName() throws Exception {
    String xmlConfig = "<clustered xmlns=\"http://www.terracottatech.com/v1/terracotta/store/clustered\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <cluster-connection>\n" +
                       "    <server host=\"" + TEST_HOST_NAME1 + "\" port=\"" + TEST_HOST_PORT1 + "\"/>\n" +
                       "  </cluster-connection>\n" +
                       "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                       "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                       "  </dataset>\n" +
                       "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                       "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                       "  </dataset>\n" +
                       "</clustered>";
    ClusteredXmlConfigurationParser parser = new ClusteredXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    assertThatExceptionOfType(XmlConfigurationException.class)
        .isThrownBy(() -> parser.parse(document))
        .withMessageContaining("Two datasets configured with the same name");
  }

  @Test
  public void testUnparseWithFullConfig() throws Exception {
    String originalXmlConfig = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<clustered xmlns=\"http://www.terracottatech.com/v1/terracotta/store/clustered\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
      "  <cluster-connection>\n" +
      "    <server host=\"" + TEST_HOST_NAME1 + "\" port=\"" + TEST_HOST_PORT1 + "\"/>\n" +
      "    <server host=\"" + TEST_HOST_NAME2 + "\" port=\"" + TEST_HOST_PORT2 + "\"/>\n" +
      "    <connection-timeout unit=\"MILLIS\">" + TEST_CONNECTION_TIME + "</connection-timeout>\n" +
      "    <reconnection-timeout unit=\"MILLIS\">" + TEST_RECONNECTION_TIME + "</reconnection-timeout>\n" +
      "    <security-root-directory>" + TEST_SRD_PATH + "</security-root-directory>\n" +
      "    <client-alias>" + TEST_CLIENT_ALIAS + "</client-alias>\n" +
      "    <client-tags>" + TEST_CLIENT_TAGS + "</client-tags>\n" +
      "  </cluster-connection>\n" +
      "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
      "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
      "  </dataset>\n" +
      "</clustered>";
    ClusteredXmlConfigurationParser parser = new ClusteredXmlConfigurationParser();
    Document originalDocument = getValidatedDocument(new ByteArrayInputStream(originalXmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    ClusteredDatasetManagerConfiguration datasetManagerConfiguration = (ClusteredDatasetManagerConfiguration)parser.parse(originalDocument);
    Document translatedDocument = parser.unparse(datasetManagerConfiguration);
    String translatedXmlConfig = XmlUtils.convertToString(translatedDocument);
    System.out.println(translatedXmlConfig);
    Diff diff = DiffBuilder.compare(Input.fromString(originalXmlConfig))
      .withTest(Input.from(translatedXmlConfig))
      .ignoreWhitespace()
      .build();
    System.out.println(diff.toString());
    assertThat(diff.hasDifferences()).isFalse().withFailMessage(diff.toString());
  }

  @Test
  public void testUnparseWithOptionalConfig() throws Exception {
    String originalXmlConfig = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<clustered xmlns=\"http://www.terracottatech.com/v1/terracotta/store/clustered\">\n" +
      "  <cluster-connection>\n" +
      "    <server host=\"" + TEST_HOST_NAME1 + "\" port=\"" + TEST_HOST_PORT1 + "\"/>\n" +
      "    <client-alias>" + TEST_CLIENT_ALIAS + "</client-alias>\n" +
      "  </cluster-connection>\n" +
      "</clustered>";
    ClusteredXmlConfigurationParser parser = new ClusteredXmlConfigurationParser();
    Document originalDocument = getValidatedDocument(new ByteArrayInputStream(originalXmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    ClusteredDatasetManagerConfiguration datasetManagerConfiguration = (ClusteredDatasetManagerConfiguration)parser.parse(originalDocument);
    Document translatedDocument = parser.unparse(datasetManagerConfiguration);
    String translatedXmlConfig = XmlUtils.convertToString(translatedDocument);
    Diff diff = DiffBuilder.compare(Input.fromString(originalXmlConfig))
        .withTest(Input.from(translatedXmlConfig))
        .ignoreWhitespace()
        .build();
    assertThat(diff.hasDifferences()).isFalse().withFailMessage(diff.toString());
  }
}