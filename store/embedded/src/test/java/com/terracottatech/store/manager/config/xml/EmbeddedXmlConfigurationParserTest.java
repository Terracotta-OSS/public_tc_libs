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

import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration;
import com.terracottatech.store.manager.xml.exception.XmlConfigurationException;
import com.terracottatech.store.manager.xml.util.XmlUtils;

import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.Diff;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static com.terracottatech.store.manager.xml.util.XmlUtils.getValidatedDocument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

public class EmbeddedXmlConfigurationParserTest {

  private static final String TEST_OFFHEAP_RESOURCE_NAME = "test-offheap";
  private static final String TEST_OFFHEAP_RESOURCE_UNIT = "B";
  private static final long TEST_OFFHEAP_RESOURCE_VALUE = 100;

  private static final String TEST_DISK_RESOURCE_NAME = "test-disk";
  private static final FileMode TEST_DISK_RESOURCE_FILE_MODE = FileMode.NEW;
  private static final Path TEST_DISK_RESOURCE_PATH = Paths.get("/path/to/disk");

  private static final String TEST_DATASET_NAME = "test-dataset";
  private static final String TEST_DATASET_KEY_TYPE = "STRING";
  private static final String TEST_DATASET_OFFHEAP_RESOURCE = "test-offheap";
  private static final String TEST_HYBRID_STORAGE_TYPE = "HYBRID";
  private static final String TEST_FRS_STORAGE_TYPE = "FRS";
  private static final String TEST_WRONG_STORAGE_TYPE = "WRONG";

  @Test
  public void testParseWithFullConfig() throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "  <disk-resources>\n" +
                       "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
                       "  </disk-resources>\n" +
                       "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                       "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                       "  </dataset>\n" +
                       "</embedded>";
    EmbeddedXmlConfigurationParser parser = new EmbeddedXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    EmbeddedDatasetManagerConfiguration configuration = (EmbeddedDatasetManagerConfiguration)parser.parse(document);
    assertThat(configuration.getResourceConfiguration().getOffheapResources()).containsOnlyKeys(TEST_OFFHEAP_RESOURCE_NAME);
    assertThat(configuration.getResourceConfiguration().getOffheapResources()).containsValues(TEST_OFFHEAP_RESOURCE_VALUE);
    Map<String, DiskResource> diskResources = configuration.getResourceConfiguration().getDiskResources();
    assertThat(diskResources).containsOnlyKeys(TEST_DISK_RESOURCE_NAME);
    DiskResource diskResource = diskResources.get(TEST_DISK_RESOURCE_NAME);
    assertThat(diskResource.getDataRoot()).isEqualTo(TEST_DISK_RESOURCE_PATH);
    assertThat(diskResource.getFileMode()).isEqualTo(TEST_DISK_RESOURCE_FILE_MODE);
    assertThat(diskResource.getPersistenceMode()).isNull();

    Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = configuration.getDatasets();
    assertThat(datasets.size()).isEqualTo(1);
    assertThat(datasets.keySet().iterator().next()).isEqualTo(TEST_DATASET_NAME);
  }

  @Test
  public void testParseWithOptionalConfig() throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "</embedded>";
    EmbeddedXmlConfigurationParser parser = new EmbeddedXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    EmbeddedDatasetManagerConfiguration configuration = (EmbeddedDatasetManagerConfiguration)parser.parse(document);
    assertThat(configuration.getResourceConfiguration().getOffheapResources()).containsOnlyKeys(TEST_OFFHEAP_RESOURCE_NAME);
    assertThat(configuration.getResourceConfiguration().getOffheapResources()).containsValues(TEST_OFFHEAP_RESOURCE_VALUE);
    assertThat(configuration.getResourceConfiguration().getDiskResources()).isEmpty();

    assertThat(configuration.getDatasets()).isEmpty();
  }

  @Test
  public void testParseWithStorageTypes() throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "  <disk-resources>\n" +
                       "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
                       "  </disk-resources>\n" +
                       "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                       "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                       "    <tcs:disk-resource storage-type=\"" + TEST_HYBRID_STORAGE_TYPE + "\">" + TEST_DISK_RESOURCE_NAME + "</tcs:disk-resource>\n" +
                       "  </dataset>\n" +
                       "</embedded>";
    EmbeddedXmlConfigurationParser parser = new EmbeddedXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    EmbeddedDatasetManagerConfiguration configuration = (EmbeddedDatasetManagerConfiguration)parser.parse(document);
    assertThat(configuration.getResourceConfiguration().getOffheapResources()).containsOnlyKeys(TEST_OFFHEAP_RESOURCE_NAME);
    assertThat(configuration.getResourceConfiguration().getOffheapResources()).containsValues(TEST_OFFHEAP_RESOURCE_VALUE);

    Map<String, DiskResource> diskResources = configuration.getResourceConfiguration().getDiskResources();
    assertThat(diskResources).containsOnlyKeys(TEST_DISK_RESOURCE_NAME);
    DiskResource diskResource = diskResources.get(TEST_DISK_RESOURCE_NAME);
    assertThat(diskResource.getDataRoot()).isEqualTo(TEST_DISK_RESOURCE_PATH);
    assertThat(diskResource.getFileMode()).isEqualTo(TEST_DISK_RESOURCE_FILE_MODE);
    assertThat(diskResource.getPersistenceMode()).isNull();

    Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = configuration.getDatasets();
    assertThat(datasets.size()).isEqualTo(1);
    assertThat(datasets.keySet().iterator().next()).isEqualTo(TEST_DATASET_NAME);
    assertThat(datasets.get(TEST_DATASET_NAME).getDatasetConfiguration().getDiskResource().orElse("")).isEqualTo(TEST_DISK_RESOURCE_NAME);
    assertThat(datasets.get(TEST_DATASET_NAME).getDatasetConfiguration().getPersistentStorageType()
        .orElse(PersistentStorageEngine.FRS)).isEqualTo(PersistentStorageEngine.HYBRID);
  }

  @Test
  public void testParseWithWrongStorageTypes() throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "  <disk-resources>\n" +
                       "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
                       "  </disk-resources>\n" +
                       "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                       "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                       "    <tcs:disk-resource storage-type=\"" + TEST_WRONG_STORAGE_TYPE + "\">" + TEST_DISK_RESOURCE_NAME + "</tcs:disk-resource>\n" +
                       "  </dataset>\n" +
                       "</embedded>";
    EmbeddedXmlConfigurationParser parser = new EmbeddedXmlConfigurationParser();
    try {
      getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));
      fail("Wrong Storage Types should not be parsed correctly");
    } catch (Exception e) {
      assertThat(e.getMessage()).contains("FRS, HYBRID");
    }
  }

  @Test
  public void testOffheapResourcesWithSameName() throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                       "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"KB\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "</embedded>";
    EmbeddedXmlConfigurationParser parser = new EmbeddedXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    assertThatExceptionOfType(XmlConfigurationException.class)
        .isThrownBy(() -> parser.parse(document))
        .withMessageContaining("Two offheap-resources configured with the same name");
  }

  @Test
  public void testDiskResourcesWithSameName() throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "  <disk-resources>\n" +
                       "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
                       "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
                       "  </disk-resources>\n" +
                       "</embedded>";
    EmbeddedXmlConfigurationParser parser = new EmbeddedXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(xmlConfig.getBytes()), Collections.singletonList(parser.getSchema()));

    assertThatExceptionOfType(XmlConfigurationException.class)
        .isThrownBy(() -> parser.parse(document))
        .withMessageContaining("Two disk-resources configured with the same name");
  }

  @Test
  public void testUnparseWithFullConfig() throws Exception {
    String originalXmlConfig = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
      "  <offheap-resources>\n" +
      "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
      "  </offheap-resources>\n" +
      "  <disk-resources>\n" +
      "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
      "  </disk-resources>\n" +
      "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
      "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
      "  </dataset>\n" +
      "</embedded>";
    assertParseAndUnparse(originalXmlConfig);
  }

  @Test
  public void testUnparseWithOptionalConfig() throws Exception {
    String originalXmlConfig = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\">\n" +
      "  <offheap-resources>\n" +
      "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
      "  </offheap-resources>\n" +
      "</embedded>";
    assertParseAndUnparse(originalXmlConfig);
  }

  @Test
  public void testUnparseWithDiskResourceDefaultConfig() throws Exception {
    String originalXmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                               "  <offheap-resources>\n" +
                               "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                               "  </offheap-resources>\n" +
                               "  <disk-resources>\n" +
                               "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
                               "  </disk-resources>\n" +
                               "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                               "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                               "    <tcs:disk-resource>" + TEST_DISK_RESOURCE_NAME + "</tcs:disk-resource>\n" +
                               "  </dataset>\n" +
                               "</embedded>";
    assertParseAndUnparse(originalXmlConfig);
  }

  @Test
  public void testUnparseWithStorageTypeConfig() throws Exception {
    String originalXmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                               "  <offheap-resources>\n" +
                               "    <offheap-resource name=\"" + TEST_OFFHEAP_RESOURCE_NAME + "\" unit=\"" + TEST_OFFHEAP_RESOURCE_UNIT + "\">" + TEST_OFFHEAP_RESOURCE_VALUE + "</offheap-resource>\n" +
                               "  </offheap-resources>\n" +
                               "  <disk-resources>\n" +
                               "    <disk-resource name=\"" + TEST_DISK_RESOURCE_NAME + "\" file-mode=\""+ TEST_DISK_RESOURCE_FILE_MODE + "\">" + TEST_DISK_RESOURCE_PATH + "</disk-resource>\n" +
                               "  </disk-resources>\n" +
                               "  <dataset name=\"" + TEST_DATASET_NAME + "\" key-type=\"" + TEST_DATASET_KEY_TYPE + "\">\n" +
                               "    <tcs:offheap-resource>" + TEST_DATASET_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                               "    <tcs:disk-resource storage-type=\"" + TEST_FRS_STORAGE_TYPE + "\">" + TEST_DISK_RESOURCE_NAME + "</tcs:disk-resource>\n" +
                               "  </dataset>\n" +
                               "</embedded>";
    assertParseAndUnparse(originalXmlConfig);
  }

  private void assertParseAndUnparse(String originalXmlConfig) throws Exception {
    EmbeddedXmlConfigurationParser parser = new EmbeddedXmlConfigurationParser();
    Document document = getValidatedDocument(new ByteArrayInputStream(originalXmlConfig.getBytes()),
        Collections.singletonList(parser.getSchema()));

    DatasetManagerConfiguration datasetManagerConfiguration = parser.parse(document);

    Document translatedDocument = parser.unparse(datasetManagerConfiguration);
    String translatedXmlConfig = XmlUtils.convertToString(translatedDocument);
    Diff diff = DiffBuilder.compare(Input.fromString(originalXmlConfig))
        .withTest(Input.from(translatedXmlConfig))
        .ignoreWhitespace()
        .build();
    assertThat(diff.hasDifferences()).isFalse().withFailMessage(diff.toString());
  }
}