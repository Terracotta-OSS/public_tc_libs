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

import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import org.junit.Test;
import org.w3c.dom.Document;

import com.terracottatech.store.configuration.AbstractDatasetConfiguration;
import com.terracottatech.store.configuration.AdvancedDatasetConfiguration;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.BaseDiskDurability;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.w3c.dom.Element;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.Diff;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.manager.config.xml.AbstractXmlConfigurationParser.DATASET_NAMESPACE;
import static com.terracottatech.store.manager.config.xml.AbstractXmlConfigurationParser.DATASET_NAMESPACE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DatasetXmlConfigurationParserTest {
  private static final String TEST_DATASET_NAME = "test-dataset";
  private static final String TEST_DATASET_KEY_TYPE = "STRING";
  private static final String TEST_DATASET_OFFHEAP_RESOURCE = "test-offheap";
  private static final String TEST_DATASET_DISK_RESOURCE = "test-disk";
  private static final String TEST_CELL_NAME = "test-cell";
  private static final String TEST_CELL_TYPE = "STRING";
  private static final String TEST_INDEX_TYPE = "BTREE";
  private static final DiskDurability.DiskDurabilityEnum TEST_DISK_DURABILITY = DiskDurability.DiskDurabilityEnum.EVENTUAL;
  private static final String TEST_DISK_DURABILITY_TIMED_TIME = "100";
  private static final String TEST_DISK_DURABILITY_TIMED_UNIT = "MILLIS";
  private static final int TEST_CONCURRENCY_UNIT = 10;
  private static final String TEST_STORAGE_TYPE = PersistentStorageEngine.HYBRID.getShortName();

  private static final String FULL_XML_CONFIG =
      "<dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
      "  <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
      "  <tcs:disk-resource>" + TEST_DATASET_DISK_RESOURCE + "</tcs:disk-resource>\n" +
      "  <tcs:indexes>\n" +
      "    <tcs:index>\n" +
      "      <tcs:cell-definition name=\"test-cell\" type=\"STRING\"/>\n" +
      "      <tcs:type>" + TEST_INDEX_TYPE + "</tcs:type>\n" +
      "    </tcs:index>\n" +
      "  </tcs:indexes>\n" +
      getDiskDurabilityString(TEST_DISK_DURABILITY) + "\n" +
      "</dataset>";

  private static final String FULL_XML_CONFIG_WITH_STORAGE_TYPE = FULL_XML_CONFIG.replace("<tcs:disk-resource>",
      "<tcs:disk-resource storage-type=\"" + TEST_STORAGE_TYPE + "\">");

  private static final String FULL_XML_CONFIG_WITH_WRONG_STORAGE_TYPE = FULL_XML_CONFIG.replace("<tcs:disk-resource>",
      "<tcs:disk-resource storage-type=\"NONEXISTING\">");

  private static final String XML_ADVANCED_CONFIG =
      "<dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
      "  <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
      "  <tcs:advanced>\n" +
      "    <tcs:concurrency-hint>" + TEST_CONCURRENCY_UNIT + "</tcs:concurrency-hint>\n" +
      "  </tcs:advanced>\n" +
      "</dataset>";

  private static final String XML_MINIMAL_CONFIG =
      "<dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
      "  <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
      "</dataset>";

  @Test
  public void testParseWithFullConfig() {
    parseAndAssertFullConfig(FULL_XML_CONFIG);
  }

  @Test
  public void testParseWithFullConfigAndStorageType() {
    DatasetConfiguration config = parseAndAssertFullConfig(FULL_XML_CONFIG_WITH_STORAGE_TYPE);
    assertThat(config.getPersistentStorageType().isPresent()).isTrue();
    assertThat(config.getPersistentStorageType().get().getShortName()).isEqualTo(TEST_STORAGE_TYPE);
  }

  @Test
  public void testParseWithWrongStorageType() {
    try {
      parseAndAssertFullConfig(FULL_XML_CONFIG_WITH_WRONG_STORAGE_TYPE);
      fail("Unexpectedly parsed invalid storage type");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("Invalid Storage Type NONEXISTING");
    }
  }

  @Test
  public void testParseWithAdvancedConfig() {
    Document document = getDocument(new ByteArrayInputStream(XML_ADVANCED_CONFIG.getBytes()));
    DatasetConfiguration datasetConfiguration =
        DatasetXmlConfigurationParser.parse(document.getDocumentElement(), new TestDatasetConfigurationBuilder()).getDatasetInfo().getDatasetConfiguration();

    AdvancedDatasetConfiguration advancedDatasetConfiguration = (AdvancedDatasetConfiguration)datasetConfiguration;
    assertThat(advancedDatasetConfiguration.getConcurrencyHint()).isEqualTo(Optional.of(TEST_CONCURRENCY_UNIT));
  }

  @Test
  public void testParseWithOptionalConfig() {
    Document document = getDocument(new ByteArrayInputStream(XML_MINIMAL_CONFIG.getBytes()));
    DatasetXmlConfigurationParser.ConfigurationHolder configurationHolder =
        DatasetXmlConfigurationParser.parse(document.getDocumentElement(), new TestDatasetConfigurationBuilder());

    DatasetConfiguration datasetConfiguration = configurationHolder.getDatasetInfo().getDatasetConfiguration();

    assertThat(configurationHolder.getDatasetName()).isEqualTo(TEST_DATASET_NAME);
    assertThat(configurationHolder.getDatasetInfo().getType()).isEqualTo(DatasetXmlConfigurationParser.getKeyType(TEST_DATASET_KEY_TYPE));
    assertThat(datasetConfiguration.getOffheapResource()).isEqualTo(TEST_DATASET_OFFHEAP_RESOURCE);
    assertThat(datasetConfiguration.getDiskResource()).isEmpty();
    assertThat(datasetConfiguration.getIndexes()).isEmpty();
    assertThat(datasetConfiguration.getDiskDurability()).isEmpty();

    AdvancedDatasetConfiguration advancedDatasetConfiguration = (AdvancedDatasetConfiguration)datasetConfiguration;
    assertThat(advancedDatasetConfiguration.getConcurrencyHint()).isEqualTo(Optional.empty());
  }

  @Test
  public void testUnparseWithFullConfig() throws Exception {
    parseAndUnparse(FULL_XML_CONFIG);
  }

  @Test
  public void testUnparseWithFullConfigAndStorageType() throws Exception {
    parseAndUnparse(FULL_XML_CONFIG_WITH_STORAGE_TYPE);
  }

  @Test
  public void testUnparseWithOptionalConfig() throws Exception {
    parseAndUnparse(XML_MINIMAL_CONFIG);
  }

  @Test
  public void testUnparseWithAdvancedConfig() throws Exception {
    parseAndUnparse(XML_ADVANCED_CONFIG);
  }

  private DatasetConfiguration parseAndAssertFullConfig(String fullConfigXml) {
    Document document = getDocument(new ByteArrayInputStream(fullConfigXml.getBytes()));
    DatasetXmlConfigurationParser.ConfigurationHolder configurationHolder =
        DatasetXmlConfigurationParser.parse(document.getDocumentElement(), new TestDatasetConfigurationBuilder());

    DatasetConfiguration datasetConfiguration = configurationHolder.getDatasetInfo().getDatasetConfiguration();
    DiskDurability.DiskDurabilityEnum durabilityEnum = datasetConfiguration.getDiskDurability()
        .orElseThrow(RuntimeException::new)
        .getDurabilityEnum();
    CellDefinition<?> expectedCellDefinition = CellDefinition.define(TEST_CELL_NAME,
        DatasetXmlConfigurationParser.getCellType(TEST_CELL_TYPE));

    assertThat(configurationHolder.getDatasetName()).isEqualTo(TEST_DATASET_NAME);
    assertThat(configurationHolder.getDatasetInfo()
        .getType()).isEqualTo(DatasetXmlConfigurationParser.getKeyType(TEST_DATASET_KEY_TYPE));
    assertThat(datasetConfiguration.getOffheapResource()).isEqualTo(TEST_DATASET_OFFHEAP_RESOURCE);
    assertThat(datasetConfiguration.getDiskResource()
        .orElseThrow(RuntimeException::new)).isEqualTo(TEST_DATASET_DISK_RESOURCE);
    assertThat(datasetConfiguration.getIndexes()).containsOnlyKeys(expectedCellDefinition);
    assertThat(datasetConfiguration.getIndexes().get(expectedCellDefinition)).isEqualTo(DatasetXmlConfigurationParser
        .getIndexSettings(TEST_INDEX_TYPE));
    assertThat(durabilityEnum).isEqualTo(TEST_DISK_DURABILITY);
    return datasetConfiguration;
  }

  private void parseAndUnparse(String originalXmlConfig) throws TransformerException, ParserConfigurationException {
    Document document = getDocument(new ByteArrayInputStream(originalXmlConfig.getBytes()));
    DatasetXmlConfigurationParser.ConfigurationHolder configurationHolder =
        DatasetXmlConfigurationParser.parse(document.getDocumentElement(), new TestDatasetConfigurationBuilder());

    String datasetName = configurationHolder.getDatasetName();
    DatasetManagerConfiguration.DatasetInfo<?> datasetInfo = configurationHolder.getDatasetInfo();

    Document translatedDocument = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element element = translatedDocument.createElement("dataset");
    element.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + DATASET_NAMESPACE_PREFIX, DATASET_NAMESPACE);

    DatasetXmlConfigurationParser.unparse(translatedDocument, element, datasetName, datasetInfo.getType(), datasetInfo.getDatasetConfiguration());

    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    transformerFactory.newTransformer().transform(new DOMSource(element), new StreamResult(byteArrayOutputStream));

    String translatedXmlConfig = new String(byteArrayOutputStream.toByteArray());
    Diff diff = DiffBuilder.compare(Input.fromString(originalXmlConfig))
        .withTest(Input.from(translatedXmlConfig))
        .ignoreWhitespace()
        .build();
    assertThat(diff.hasDifferences()).withFailMessage(diff.toString()).isFalse();
  }

  private static String getDiskDurabilityString(DiskDurability.DiskDurabilityEnum diskDurabilityEnum) {
    switch (diskDurabilityEnum) {
      case TIMED: return "<tcs:durability-timed unit=\"" + TEST_DISK_DURABILITY_TIMED_UNIT + "\">" +
                         TEST_DISK_DURABILITY_TIMED_TIME +
                         "</tcs:durability-timed>";
      case EVENTUAL: return "<tcs:durability-eventual/>";
      case EVERY_MUTATION: return "<tcs:durability-every-mutation/>";
      default: throw new IllegalArgumentException("Unknown diskDurabilityEnum" + diskDurabilityEnum);
    }
  }

  private static class TestDatasetConfigurationBuilder implements AdvancedDatasetConfigurationBuilder {

    private String offheapResource;
    private String diskResource;
    private Map<CellDefinition<?>, IndexSettings> indexes = new HashMap<>();
    private Integer concurrencyHint;
    private DiskDurability durability;
    private PersistentStorageType storageType;

    @Override
    public DatasetConfiguration build() {
      return new AbstractDatasetConfiguration(offheapResource, diskResource, indexes, concurrencyHint, durability, storageType) {};
    }

    @Override
    public DatasetConfigurationBuilder offheap(String resourceName) {
      this.offheapResource = resourceName;
      return this;
    }

    @Override
    public DatasetConfigurationBuilder disk(String resourceName) {
      this.diskResource = resourceName;
      return this;
    }

    @Override
    public DatasetConfigurationBuilder disk(String resourceName, PersistentStorageType storageType) {
      this.diskResource = resourceName;
      this.storageType = storageType;
      return this;
    }

    @Override
    public DatasetConfigurationBuilder index(CellDefinition<?> cellDefinition, IndexSettings settings) {
      indexes.put(cellDefinition, settings);
      return this;
    }

    @Override
    public DatasetConfigurationBuilder durabilityEventual() {
      durability = BaseDiskDurability.OS_DETERMINED;
      return this;
    }

    @Override
    public DatasetConfigurationBuilder durabilityEveryMutation() {
      durability = BaseDiskDurability.ALWAYS;
      return this;
    }

    @Override
    public DatasetConfigurationBuilder durabilityTimed(long duration, TimeUnit units) {
      durability = BaseDiskDurability.timed(duration, units);
      return this;
    }

    @Override
    public AdvancedDatasetConfigurationBuilder advanced() {
      return this;
    }

    @Override
    public AdvancedDatasetConfigurationBuilder concurrencyHint(int hint) {
      concurrencyHint = hint;
      return this;
    }
  }

  private static Document getDocument(InputStream inputStream) {
    try {
      DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
      documentBuilderFactory.setNamespaceAware(true);
      documentBuilderFactory.setIgnoringComments(true);
      documentBuilderFactory.setIgnoringElementContentWhitespace(true);
      DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
      return documentBuilder.parse(inputStream);
    } catch (Exception e) {
      throw new RuntimeException("Error while parsing XML configuration", e);
    }
  }
}