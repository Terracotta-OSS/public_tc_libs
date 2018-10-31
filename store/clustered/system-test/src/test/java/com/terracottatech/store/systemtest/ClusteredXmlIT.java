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

import org.junit.Test;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.Diff;

import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.AdvancedDatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.ConfigurationMode;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.XmlConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.ServerBasedClientSideConfiguration;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.systemtest.TranslateTestHelper.assertElementWithFilter;
import static com.terracottatech.store.systemtest.TranslateTestHelper.assertElementXml;
import static org.assertj.core.api.Assertions.assertThat;

public class ClusteredXmlIT extends BaseClusterWithOffheapAndFRSTest {
  @Test
  public void testParse() {
    URL xmlConfigFile = ClusteredXmlIT.class.getResource("/clustered.xml");
    DatasetManagerConfiguration datasetManagerConfiguration = XmlConfiguration.parseDatasetManagerConfig(xmlConfigFile);
    assertThat(datasetManagerConfiguration).isInstanceOf(ClusteredDatasetManagerConfiguration.class);
    ClusteredDatasetManagerConfiguration.ClientSideConfiguration clientSideConfiguration =
        ((ClusteredDatasetManagerConfiguration)datasetManagerConfiguration).getClientSideConfiguration();
    assertThat(clientSideConfiguration).isInstanceOf(ServerBasedClientSideConfiguration.class);
    ServerBasedClientSideConfiguration serverBasedClusteredDatasetManagerConfiguration =
        (ServerBasedClientSideConfiguration)clientSideConfiguration;
    Iterable<InetSocketAddress> servers = serverBasedClusteredDatasetManagerConfiguration.getServers();
    Iterator<InetSocketAddress> serversIterator = servers.iterator();
    assertThat(serversIterator.hasNext()).isTrue();
    InetSocketAddress firstServer = serversIterator.next();
    assertThat(firstServer.getHostName()).isEqualTo("localhost");
    assertThat(firstServer.getPort()).isEqualTo(9410);
    assertThat(clientSideConfiguration.getSecurityRootDirectory()).isEqualTo(Paths.get("srd"));
    assertThat(clientSideConfiguration.getClientAlias()).isEqualTo("client-alias");
    assertThat(clientSideConfiguration.getClientTags()).isEqualTo(Collections.singleton("client-tags"));
    assertThat(clientSideConfiguration.getConnectTimeout()).isEqualTo(10);
    assertThat(clientSideConfiguration.getReconnectTimeout()).isEqualTo(20);

    Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
    assertThat(datasets.size()).isEqualTo(1);
    assertThat(datasets.containsKey("dataset")).isEqualTo(true);

    DatasetManagerConfiguration.DatasetInfo<?> datasetInfo = datasets.get("dataset");
    assertThat(datasetInfo.getType()).isEqualTo(Type.STRING);

    DatasetConfiguration datasetConfiguration = datasetInfo.getDatasetConfiguration();
    assertThat(datasetConfiguration.getDiskDurability().isPresent()).isTrue();
    assertThat(datasetConfiguration.getDiskDurability().get().getDurabilityEnum()).isEqualTo(DiskDurability.DiskDurabilityEnum.EVENTUAL);
    assertThat(datasetConfiguration.getDiskResource().isPresent()).isTrue();
    assertThat(datasetConfiguration.getDiskResource().get()).isEqualTo("disk");
    assertThat(datasetConfiguration.getOffheapResource()).isEqualTo("offheap");

    Map<CellDefinition<?>, IndexSettings> indexes = datasetConfiguration.getIndexes();
    assertThat(indexes.size()).isEqualTo(1);
    assertThat(indexes.containsKey(CellDefinition.define("cell", Type.BOOL))).isTrue();
    assertThat(indexes.get(CellDefinition.define("cell", Type.BOOL))).isEqualTo(IndexSettings.BTREE);

    assertThat(((AdvancedDatasetConfiguration)datasetConfiguration).getConcurrencyHint().get()).isEqualTo(16);
  }

  @Test
  public void testDatasetManagerCreationUsingXml() throws Exception {
    final String datasetName = "xml-dataset";

    String datasetConfig = "  <dataset name=\"" + datasetName + "\" key-type=\"STRING\">\n" +
                           "    <tcs:offheap-resource>" + CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "  </dataset>\n";

    URL xmlConfigFile = XmlUtils.createXmlConfigFile(CLUSTER.getClusterHostPorts(), datasetConfig, temporaryFolder);

    try (DatasetManager datasetManager = DatasetManager.using(XmlConfiguration.parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE)) {
      datasetManager.destroyDataset(datasetName);
    }
  }

  @Test
  public void testGetDatasetManagerConfiguration() throws Exception {
    final String datasetName = "xml-dataset";

    String datasetConfig = "  <dataset name=\"" + datasetName + "\" key-type=\"STRING\">\n" +
                           "    <tcs:offheap-resource>" + CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "  </dataset>\n";

    URL xmlConfigFile = XmlUtils.createXmlConfigFile(CLUSTER.getClusterHostPorts(), datasetConfig, temporaryFolder);

    try (DatasetManager datasetManager = DatasetManager.using(XmlConfiguration.parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE)) {
      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
      assertThat(datasetManagerConfiguration).isInstanceOf(ClusteredDatasetManagerConfiguration.class);
      ClusteredDatasetManagerConfiguration.ClientSideConfiguration clientSideConfiguration =
          ((ClusteredDatasetManagerConfiguration)datasetManagerConfiguration).getClientSideConfiguration();
      assertThat(clientSideConfiguration.getConnectTimeout())
          .isEqualTo(ClusteredDatasetManagerBuilder.DEFAULT_CONNECTION_TIMEOUT_MS);
      assertThat(clientSideConfiguration.getReconnectTimeout())
          .isEqualTo(ClusteredDatasetManagerBuilder.DEFAULT_RECONNECT_TIMEOUT_MS);
      Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
      assertThat(datasets.size()).isEqualTo(1);
      assertThat(datasets.containsKey(datasetName)).isEqualTo(true);
      datasetManager.destroyDataset(datasetName);
    }
  }

  @Test
  public void testGetDatasetManagerConfigurationWithExistingDataset() throws Exception {
    final String datasetName = "test-dataset";
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()){
      DatasetConfiguration datasetConfiguration =
          datasetManager.datasetConfiguration().offheap(CLUSTER_OFFHEAP_RESOURCE).build();
      assertThat(datasetManager.newDataset(datasetName, Type.STRING, datasetConfiguration)).isTrue();
    }

    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()){
      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
      Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
      assertThat(datasets.size()).isEqualTo(1);
      assertThat(datasets.containsKey(datasetName)).isTrue();
      datasetManager.destroyDataset(datasetName);
    }
  }

  @Test
  public void testUnparse() throws Exception {
    URL xmlConfigFile = ClusteredXmlIT.class.getResource("/clustered.xml");
    DatasetManagerConfiguration datasetManagerConfiguration = XmlConfiguration.parseDatasetManagerConfig(xmlConfigFile);
    String translatedXmlConfig = XmlConfiguration.toXml(datasetManagerConfiguration);
    Diff diff = DiffBuilder.compare(Input.fromStream(xmlConfigFile.openStream()))
                           .withTest(Input.from(translatedXmlConfig))
                           .ignoreComments()
                           .ignoreWhitespace()
                           .build();
    assertThat(diff.hasDifferences()).isFalse().withFailMessage(diff.toString());
  }

  @Test
  public void testUnparsingInputProgrammaticConfigToXmlForMinimalConfig() throws StoreException {
    String res[] = CLUSTER.getClusterHostPorts();
    String[] tokens = res[0].split(":");
    URL xmlConfigFile = XmlUtils.createXmlConfigFile(res, "", temporaryFolder);
    try (DatasetManager datasetManager = DatasetManager.clustered(URI.create("terracotta://localhost:" + tokens[1]))
        .build()) {
      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
      String translatedXmlConfig = XmlConfiguration.toXml(datasetManagerConfiguration);
      assertElementWithFilter(xmlConfigFile, translatedXmlConfig);
    }
  }

  @Test
  public void testUnparsingInputProgrammaticConfigToXmlForMinimalDatasetConfig() throws StoreException {
    String res[] = CLUSTER.getClusterHostPorts();
    String[] tokens = res[0].split(":");
    String datasetConfig = "  <dataset name=\"" + "dataset1" + "\" key-type=\"STRING\">\n" +
                           "    <tcs:offheap-resource>" + CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "  </dataset>\n";
    URL xmlConfigFile = XmlUtils.createXmlConfigFile(res, datasetConfig, temporaryFolder);
    try (DatasetManager datasetManager = DatasetManager.clustered(URI.create("terracotta://localhost:" + tokens[1]))
        .build()) {
      datasetManager.newDataset("dataset1", Type.STRING, datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE).build());
      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
      String translatedXmlConfig = XmlConfiguration.toXml(datasetManagerConfiguration);
      assertElementWithFilter(xmlConfigFile, translatedXmlConfig);
      datasetManager.destroyDataset("dataset1");
    }
  }

  @Test
  public void testUnparsingInputProgrammaticConfigToXmlForMultipleDatasetsWithFullConfiguration() throws StoreException {
    String res[] = CLUSTER.getClusterHostPorts();
    String[] tokens = res[0].split(":");
    String datasetConfig = "<dataset name=\"" + "dataset1" + "\" key-type=\"STRING\">\n" +
                           "<tcs:offheap-resource>" + CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "<tcs:disk-resource>" + CLUSTER_DISK_RESOURCE + "</tcs:disk-resource>\n" +
                           "<tcs:indexes>\n" + "<tcs:index>\n" +
                           "<tcs:cell-definition name=\"cell1\" type=\"BOOL\"/>\n" +
                           "<tcs:type>BTREE</tcs:type>\n" + "</tcs:index>\n" + "</tcs:indexes>\n" +
                           "<tcs:durability-eventual/>\n" + "<tcs:advanced\n>" +
                           "<tcs:concurrency-hint>" + 16 + "</tcs:concurrency-hint>\n" + "</tcs:advanced>\n" +
                           "</dataset>\n" +
                           "<dataset name=\"" + "dataset2" + "\" key-type=\"BOOL\">\n" +
                           "<tcs:offheap-resource>" + CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "<tcs:disk-resource storage-type=\"FRS\">" + CLUSTER_DISK_RESOURCE + "</tcs:disk-resource>\n" +
                           "<tcs:indexes>\n" + "<tcs:index>\n" +
                           "<tcs:cell-definition name=\"cell2\" type=\"CHAR\"/>\n" +
                           "<tcs:type>BTREE</tcs:type>\n" + "</tcs:index>\n" + "</tcs:indexes>\n" +
                           "<tcs:durability-every-mutation/>\n" +
                           "</dataset>\n" +
                           "<dataset name=\"" + "dataset3" + "\" key-type=\"INT\">\n" +
                           "<tcs:offheap-resource>" + CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "<tcs:disk-resource storage-type=\"HYBRID\">" + SECONDARY_DISK_RESOURCE + "</tcs:disk-resource>\n" +
                           "<tcs:indexes>\n" + "<tcs:index>\n" +
                           "<tcs:cell-definition name=\"cell3\" type=\"DOUBLE\"/>\n" +
                           "<tcs:type>BTREE</tcs:type>\n" + "</tcs:index>\n" + "</tcs:indexes>\n" +
                           "<tcs:durability-timed unit=\"MILLIS\">5000</tcs:durability-timed>\n" +
                           "</dataset>\n" +
                           "<dataset name=\"" + "dataset4" + "\" key-type=\"DOUBLE\">\n" +
                           "<tcs:offheap-resource>" + CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "<tcs:disk-resource storage-type=\"HYBRID\">" + SECONDARY_DISK_RESOURCE + "</tcs:disk-resource>\n" +
                           "<tcs:indexes>\n" + "<tcs:index>\n" +
                           "<tcs:cell-definition name=\"cell4\" type=\"LONG\"/>\n" +
                           "<tcs:type>BTREE</tcs:type>\n" + "</tcs:index>\n" + "</tcs:indexes>\n" +
                           "<tcs:durability-timed unit=\"MILLIS\">6000</tcs:durability-timed>\n" +
                           "<tcs:advanced\n>" +
                           "<tcs:concurrency-hint>" + 16 + "</tcs:concurrency-hint>\n" + "</tcs:advanced>\n" +
                           "</dataset>\n";
    URL xmlConfigFile = XmlUtils.createXmlConfigFileWithComponents(res,
        "10000", "MILLIS", "20000", "MILLIS",
        "client-alias", "client-tags", datasetConfig, temporaryFolder);
    try (DatasetManager datasetManager = DatasetManager.clustered(URI.create("terracotta://localhost:" + tokens[1]))
        .withClientAlias("client-alias").withClientTags("client-tags")
        .withConnectionTimeout(10000L, TimeUnit.MILLISECONDS)
        .withReconnectTimeout(20000L, TimeUnit.MILLISECONDS).build()) {
      CellDefinition<Boolean> booleanCellDefinition = CellDefinition.defineBool("cell1");
      CellDefinition<Character> characterCellDefinition = CellDefinition.defineChar("cell2");
      CellDefinition<Double> doubleCellDefinition = CellDefinition.defineDouble("cell3");
      CellDefinition<Long> longCellDefinition = CellDefinition.defineLong("cell4");
      IndexSettings indexSettings = IndexSettings.BTREE;

      datasetManager.newDataset("dataset1", Type.STRING, datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE).disk(CLUSTER_DISK_RESOURCE)
          .index(booleanCellDefinition, indexSettings)
          .durabilityEventual().advanced().concurrencyHint(16).build());
      datasetManager.newDataset("dataset2", Type.BOOL, datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE, PersistentStorageType.permanentIdToStorageType(1))
          .index(characterCellDefinition, indexSettings)
          .durabilityEveryMutation()
          .build());
      datasetManager.newDataset("dataset3", Type.INT, datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(SECONDARY_DISK_RESOURCE, PersistentStorageType.permanentIdToStorageType(2))
          .index(doubleCellDefinition, indexSettings)
          .durabilityTimed(5000L, TimeUnit.MILLISECONDS)
          .build());
      datasetManager.newDataset("dataset4", Type.DOUBLE, datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(SECONDARY_DISK_RESOURCE, PersistentStorageType.permanentIdToStorageType(2))
          .index(longCellDefinition, indexSettings)
          .durabilityTimed(6000L, TimeUnit.MILLISECONDS)
          .advanced().concurrencyHint(16).build());
      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
      String translatedXmlConfig = XmlConfiguration.toXml(datasetManagerConfiguration);
      assertElementXml(xmlConfigFile, translatedXmlConfig);
      datasetManager.destroyDataset("dataset1");
      datasetManager.destroyDataset("dataset2");
      datasetManager.destroyDataset("dataset3");
      datasetManager.destroyDataset("dataset4");
    }
  }
}
