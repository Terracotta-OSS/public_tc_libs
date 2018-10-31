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
package com.terracottatech.store;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.ElementSelectors;

import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.configuration.AdvancedDatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.ConfigurationMode;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import com.terracottatech.store.manager.XmlConfiguration;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

public class EmbeddedXmlIT {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testParse() {
    URL embeddedXmlFile = EmbeddedXmlIT.class.getResource("/embedded.xml");
    DatasetManagerConfiguration datasetManagerConfiguration = XmlConfiguration.parseDatasetManagerConfig(embeddedXmlFile);
    assertThat(datasetManagerConfiguration).isInstanceOf(EmbeddedDatasetManagerConfiguration.class);
    EmbeddedDatasetManagerConfiguration embeddedDatasetManagerConfiguration = (EmbeddedDatasetManagerConfiguration)datasetManagerConfiguration;
    Map<String, Long> offheapResources = embeddedDatasetManagerConfiguration.getResourceConfiguration()
                                                                            .getOffheapResources();
    assertThat(offheapResources.containsKey("offheap")).isTrue();
    assertThat(offheapResources.get("offheap")).isEqualTo(10000);

    Map<String, DiskResource> diskResources = embeddedDatasetManagerConfiguration.getResourceConfiguration()
                                                                                 .getDiskResources();
    assertThat(diskResources.containsKey("disk")).isTrue();
    DiskResource diskResource = diskResources.get("disk");
    DiskResource diskResource1 = diskResources.get("disk1");
    assertThat(diskResource.getPersistenceMode()).isNull();
    assertThat(diskResource.getFileMode()).isEqualTo(EmbeddedDatasetManagerBuilder.FileMode.REOPEN);
    assertThat(diskResource.getDataRoot()).isEqualTo(Paths.get("disk"));
    assertThat(diskResource1.getPersistenceMode()).isEqualTo(null);
    assertThat(diskResource1.getFileMode()).isEqualTo(EmbeddedDatasetManagerBuilder.FileMode.REOPEN);
    assertThat(diskResource1.getDataRoot()).isEqualTo(Paths.get("disk1"));

    Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
    assertThat(datasets.size()).isEqualTo(2);
    assertThat(datasets.containsKey("dataset")).isEqualTo(true);
    assertThat(datasets.containsKey("dataset1")).isEqualTo(true);

    DatasetManagerConfiguration.DatasetInfo<?> datasetInfo = datasets.get("dataset");
    assertThat(datasetInfo.getType()).isEqualTo(Type.STRING);

    DatasetConfiguration datasetConfiguration = datasetInfo.getDatasetConfiguration();
    assertThat(datasetConfiguration.getDiskDurability().isPresent()).isTrue();
    assertThat(datasetConfiguration.getDiskDurability().get().getDurabilityEnum()).isEqualTo(DiskDurability.DiskDurabilityEnum.EVENTUAL);
    assertThat(datasetConfiguration.getDiskResource().isPresent()).isTrue();
    assertThat(datasetConfiguration.getDiskResource().get()).isEqualTo("disk");
    assertThat(datasetConfiguration.getOffheapResource()).isEqualTo("offheap");
    assertThat(datasetConfiguration.getPersistentStorageType().isPresent()).isFalse();

    Map<CellDefinition<?>, IndexSettings> indexes = datasetConfiguration.getIndexes();
    assertThat(indexes.size()).isEqualTo(1);
    assertThat(indexes.containsKey(CellDefinition.define("cell", Type.BOOL))).isTrue();
    assertThat(indexes.get(CellDefinition.define("cell", Type.BOOL))).isEqualTo(IndexSettings.BTREE);

    assertThat(((AdvancedDatasetConfiguration)datasetConfiguration).getConcurrencyHint().isPresent()).isTrue();
    assertThat(((AdvancedDatasetConfiguration)datasetConfiguration).getConcurrencyHint().get()).isEqualTo(16);

    DatasetManagerConfiguration.DatasetInfo<?> datasetInfo1 = datasets.get("dataset1");
    assertThat(datasetInfo1.getType()).isEqualTo(Type.STRING);

    DatasetConfiguration datasetConfiguration1 = datasetInfo1.getDatasetConfiguration();
    assertThat(datasetConfiguration1.getDiskDurability().isPresent()).isFalse();
    assertThat(datasetConfiguration1.getDiskResource().isPresent()).isTrue();
    assertThat(datasetConfiguration1.getDiskResource().get()).isEqualTo("disk1");
    assertThat(datasetConfiguration1.getPersistentStorageType().isPresent()).isTrue();
    assertThat(datasetConfiguration1.getPersistentStorageType().get()).isEqualTo(PersistentStorageEngine.HYBRID);
    assertThat(datasetConfiguration1.getOffheapResource()).isEqualTo("offheap1");
  }

  @Test
  public void testDatasetManagerCreationUsingXml() throws Exception {
    Path embeddedXmlFile = createXmlConfigFile();
    DatasetManagerConfiguration configuration = XmlConfiguration.parseDatasetManagerConfig(embeddedXmlFile.toUri().toURL());
    try (DatasetManager datasetManager = DatasetManager.using(configuration, ConfigurationMode.CREATE)) {
      datasetManager.destroyDataset("test");
    }
  }

  @Test
  public void testGetDatasetManagerConfiguraiton() throws Exception {
    Path embeddedXmlFile = createXmlConfigFile();
    DatasetManagerConfiguration configuration = XmlConfiguration.parseDatasetManagerConfig(embeddedXmlFile.toUri().toURL());
    try (DatasetManager datasetManager = DatasetManager.using(configuration, ConfigurationMode.CREATE)) {
      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
      Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
      assertThat(datasets.size()).isEqualTo(1);
      assertThat(datasets.containsKey("test")).isTrue();
      datasetManager.destroyDataset("test");
    }
  }

  @Test
  public void testGetDatasetManagerConfigurationAfterDestroyingExistingDatasets() throws Exception {
    String DATASET_ONE = "dataset1";
    Type<Long> KEY_TYPE_ONE = Type.LONG;
    String DATASET_TWO = "datatet2";
    Type<String> KEY_TYPE_TWO = Type.STRING;

    String OFFHEAP_RESOURCE_NAME = "offheap";
    long OFFHEAP_SIZE_IN_BYTES = 100 * 1024 * 1024;
    String DISK_RESOURCE_ONE = "disk1";
    Path DATA_ROOT_ONE = temporaryFolder.newFolder().toPath();
    String DISK_RESOURCE_TWO = "disk2";
    Path DATA_ROOT_TWO = temporaryFolder.newFolder().toPath();

    try (DatasetManager datasetManager = DatasetManager.embedded()
                                                       .offheap(OFFHEAP_RESOURCE_NAME, OFFHEAP_SIZE_IN_BYTES,
                                                                MemoryUnit.B)
                                                       .disk(DISK_RESOURCE_ONE,
                                                             DATA_ROOT_ONE,
                                                             EmbeddedDatasetManagerBuilder.FileMode.NEW)
                                                       .disk(DISK_RESOURCE_TWO,
                                                             DATA_ROOT_TWO,
                                                             EmbeddedDatasetManagerBuilder.FileMode.NEW)
                                                       .build()) {
      DatasetConfigurationBuilder datasetConfigurationBuilder =
          datasetManager.datasetConfiguration()
                        .offheap(OFFHEAP_RESOURCE_NAME)
                        .disk(DISK_RESOURCE_ONE);
      boolean created = datasetManager.newDataset(DATASET_ONE, KEY_TYPE_ONE, datasetConfigurationBuilder);
      assertThat(created).isTrue();

      datasetConfigurationBuilder =
          datasetManager.datasetConfiguration()
                        .offheap(OFFHEAP_RESOURCE_NAME)
                        .disk(DISK_RESOURCE_TWO);
      created = datasetManager.newDataset(DATASET_TWO, KEY_TYPE_TWO, datasetConfigurationBuilder);
      assertThat(created).isTrue();
      datasetManager.destroyDataset(DATASET_ONE);
    }

    try (DatasetManager datasetManager = DatasetManager.embedded()
                                                       .offheap(OFFHEAP_RESOURCE_NAME, OFFHEAP_SIZE_IN_BYTES,
                                                                MemoryUnit.B)
                                                       .disk(DISK_RESOURCE_ONE,
                                                             DATA_ROOT_ONE,
                                                             EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
                                                       .disk(DISK_RESOURCE_TWO,
                                                             DATA_ROOT_TWO,
                                                             EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
                                                       .build()) {
      assertThatExceptionOfType(StoreException.class)
          .isThrownBy(() -> datasetManager.getDataset(DATASET_ONE, KEY_TYPE_ONE))
          .withMessageContaining("Dataset 'dataset1' not found");
      Dataset<String> dataset = datasetManager.getDataset(DATASET_TWO, KEY_TYPE_TWO);
      assertThat(dataset).isNotNull();
      dataset.close();
      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();

      Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
      assertThat(datasets.containsKey(DATASET_ONE)).isFalse();
      assertThat(datasets.containsKey(DATASET_TWO)).isTrue();
      DatasetManagerConfiguration.DatasetInfo<?> datasetInfo = datasets.get(DATASET_TWO);

      DatasetConfiguration datasetConfiguration = datasetInfo.getDatasetConfiguration();

      assertThat(datasetInfo.getType()).isEqualTo(KEY_TYPE_TWO);

      assertThat(datasetConfiguration.getOffheapResource()).isEqualTo(OFFHEAP_RESOURCE_NAME);

      assertThat(datasetConfiguration.getDiskResource()).isPresent().get().isEqualTo(DISK_RESOURCE_TWO);
    }
  }

  @Test
  public void testGetDatasetManagerConfiguraitonWithExistingDataset() throws Exception {
    String DATASET_NAME = "test-dataset";
    Type<Long> KEY_TYPE = Type.LONG;

    String OFFHEAP_RESOURCE_NAME = "offheap";
    long OFFHEAP_SIZE_IN_BYTES = 100 * 1024 * 1024;

    String DISK_RESOURCE_NAME = "disk";
    Path DATA_ROOT = temporaryFolder.newFolder().toPath();

    PersistentStorageType PERSISTENT_STORAGE_TYPE = PersistentStorageEngine.FRS;

    int CONCURRENCY_HINT = 8;

    CellDefinition<Boolean> cellDefinition = CellDefinition.define("cell", Type.BOOL);
    IndexSettings INDEX_SETTINGS = IndexSettings.BTREE;

    try (DatasetManager datasetManager = DatasetManager.embedded()
                                                       .offheap(OFFHEAP_RESOURCE_NAME,
                                                                OFFHEAP_SIZE_IN_BYTES,
                                                                MemoryUnit.B)
                                                       .disk(DISK_RESOURCE_NAME,
                                                             DATA_ROOT,
                                                             EmbeddedDatasetManagerBuilder.FileMode.NEW)
                                                       .build()) {
      DatasetConfigurationBuilder datasetConfigurationBuilder =
          datasetManager.datasetConfiguration()
                        .offheap(OFFHEAP_RESOURCE_NAME)
                        .disk(DISK_RESOURCE_NAME, PERSISTENT_STORAGE_TYPE)
                        .durabilityEventual()
                        .index(cellDefinition, INDEX_SETTINGS)
                        .advanced()
                        .concurrencyHint(CONCURRENCY_HINT);
      boolean created = datasetManager.newDataset(DATASET_NAME, KEY_TYPE, datasetConfigurationBuilder);
      assertThat(created).isTrue();
    }

    try (DatasetManager datasetManager = DatasetManager.embedded()
                                                       .offheap(OFFHEAP_RESOURCE_NAME,
                                                                OFFHEAP_SIZE_IN_BYTES,
                                                                MemoryUnit.B)
                                                       .disk(DISK_RESOURCE_NAME,
                                                             DATA_ROOT,
                                                             EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
                                                       .build()) {
      Dataset<Long> dataset = datasetManager.getDataset(DATASET_NAME, KEY_TYPE);
      assertThat(dataset).isNotNull();
      dataset.close();

      DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();

      Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets = datasetManagerConfiguration.getDatasets();
      assertThat(datasets.containsKey(DATASET_NAME)).isTrue();
      DatasetManagerConfiguration.DatasetInfo<?> datasetInfo = datasets.get(DATASET_NAME);

      DatasetConfiguration datasetConfiguration = datasetInfo.getDatasetConfiguration();

      assertThat(datasetInfo.getType()).isEqualTo(KEY_TYPE);

      assertThat(datasetConfiguration.getOffheapResource()).isEqualTo(OFFHEAP_RESOURCE_NAME);

      assertThat(datasetConfiguration.getDiskResource()).isPresent().get().isEqualTo(DISK_RESOURCE_NAME);

      assertThat(datasetConfiguration.getDiskDurability().get().getDurabilityEnum()).isEqualTo(DiskDurability
                                                                                         .DiskDurabilityEnum.EVENTUAL);

      Map<CellDefinition<?>, IndexSettings> indexes = datasetConfiguration.getIndexes();
      assertThat(indexes.containsKey(cellDefinition)).isTrue();
      assertThat(indexes.get(cellDefinition)).isEqualTo(INDEX_SETTINGS);

      assertThat(datasetConfiguration.getPersistentStorageType()).isPresent().get()
                                                                 .isEqualTo(PersistentStorageEngine.FRS);

      assertThat(((AdvancedDatasetConfiguration)datasetConfiguration).getConcurrencyHint()).isPresent().get()
                                                                                           .isEqualTo(CONCURRENCY_HINT);

      assertThat(datasetManagerConfiguration).isInstanceOf(EmbeddedDatasetManagerConfiguration.class);
      EmbeddedDatasetManagerConfiguration embeddedDatasetManagerConfiguration =
          (EmbeddedDatasetManagerConfiguration)datasetManagerConfiguration;

      Map<String, Long> offheapResources = embeddedDatasetManagerConfiguration.getResourceConfiguration()
                                                                              .getOffheapResources();
      assertThat(offheapResources).isNotNull();
      assertThat(offheapResources.size()).isEqualTo(1);
      assertThat(offheapResources.containsKey(OFFHEAP_RESOURCE_NAME)).isTrue();
      Long offheapSize = offheapResources.get(OFFHEAP_RESOURCE_NAME);
      assertThat(offheapSize).isEqualTo(OFFHEAP_SIZE_IN_BYTES);

      Map<String, DiskResource> diskResources = embeddedDatasetManagerConfiguration.getResourceConfiguration()
                                                                                   .getDiskResources();
      assertThat(diskResources).isNotNull();
      assertThat(diskResources.size()).isEqualTo(1);
      assertThat(diskResources.containsKey(DISK_RESOURCE_NAME)).isTrue();
      DiskResource diskResource = diskResources.get(DISK_RESOURCE_NAME);
      assertThat(diskResource.getDataRoot()).isEqualTo(DATA_ROOT);
      assertThat(diskResource.getFileMode()).isEqualTo(EmbeddedDatasetManagerBuilder.FileMode.REOPEN);
    }
  }


  @Test
  public void testUnparse() throws Exception {
    URL xmlConfigFile = EmbeddedXmlIT.class.getResource("/embedded.xml");
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
    URL xmlConfigFile = EmbeddedXmlIT.class.getResource("/minimal_embedded_config.xml");
    DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("offheap", 10000L, MemoryUnit.B)
        .build();
    DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
    String translatedXmlConfig = XmlConfiguration.toXml(datasetManagerConfiguration);
    assertElementXml(xmlConfigFile, translatedXmlConfig);
  }

  @Test
  public void testUnparsingInputProgrammaticConfigToXmlForMinimalDatasetConfig() throws StoreException {
    URL xmlConfigFile = EmbeddedXmlIT.class.getResource("/minimal_embedded_dataset_config.xml");
    DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("offheap", 20000L, MemoryUnit.B).build();
    datasetManager.newDataset("dataset1", Type.INT, datasetManager.datasetConfiguration()
        .offheap("offheap").build());
    DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
    String translatedXmlConfig = XmlConfiguration.toXml(datasetManagerConfiguration);
    assertElementXml(xmlConfigFile, translatedXmlConfig);
  }

  @Test
  public void testUnparsingInputProgrammaticConfigToXmlForMultipleDatasetsWithFullConfiguration() throws StoreException, IOException, URISyntaxException {
    CellDefinition<Boolean> booleanCellDefinition = CellDefinition.defineBool("cell1");
    CellDefinition<Character> characterCellDefinition = CellDefinition.defineChar("cell2");
    CellDefinition<Double> doubleCellDefinition = CellDefinition.defineDouble("cell3");
    CellDefinition<String> stringCellDefinition = CellDefinition.defineString("cell4");
    IndexSettings indexSettings = IndexSettings.BTREE;

    Path disk1 = Files.createTempDirectory("disk1");
    Path disk2 = Files.createTempDirectory("disk2");
    Path disk3 = Files.createTempDirectory("disk3");
    Path disk4 = Files.createTempDirectory("disk4");

    DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("offheap", 104857600L, MemoryUnit.B)
        .disk("disk1", disk1, EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .disk("disk2", disk2, EmbeddedDatasetManagerBuilder.FileMode.NEW)
        .disk("disk3", disk3, EmbeddedDatasetManagerBuilder.FileMode.NEW)
        .disk("disk4", disk4, EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW).build();

    datasetManager.newDataset("dataset1", Type.STRING, datasetManager.datasetConfiguration()
        .offheap("offheap").disk("disk2").index(booleanCellDefinition, indexSettings)
        .durabilityEventual().advanced().concurrencyHint(16).build());
    datasetManager.newDataset("dataset2", Type.BOOL, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .disk("disk2", PersistentStorageType.permanentIdToStorageType(1))
        .index(characterCellDefinition, indexSettings)
        .durabilityEveryMutation()
        .build());
    datasetManager.newDataset("dataset3", Type.INT, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .disk("disk3", PersistentStorageType.permanentIdToStorageType(2))
        .index(doubleCellDefinition, indexSettings)
        .durabilityTimed(5000L, TimeUnit.MILLISECONDS)
        .build());
    datasetManager.newDataset("dataset4", Type.DOUBLE, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .disk("disk2", PersistentStorageType.permanentIdToStorageType(1))
        .index(stringCellDefinition, indexSettings)
        .durabilityEveryMutation()
        .build());

    DatasetManagerConfiguration datasetManagerConfiguration = datasetManager.getDatasetManagerConfiguration();
    String translatedXmlConfig = XmlConfiguration.toXml(datasetManagerConfiguration);
    try (Stream<String> stream =
             Files.lines(Paths.get(getClass().getResource("/single_index_embedded_dataset.xml")
                 .toURI()))) {
      List<String> lines1 = stream.map(s -> {

        s = s.replace(("${DISKA_TEMPLATE}"), disk1.toString());
        s = s.replace(("${DISKB_TEMPLATE}"), disk2.toString());
        s = s.replace(("${DISKC_TEMPLATE}"), disk3.toString());
        s = s.replace(("${DISKD_TEMPLATE}"), disk4.toString());
        return s;
      }).collect(Collectors.toList());

      Path tmpFile = Files.createTempFile("test", ".xml");
      Files.write(tmpFile, lines1);

      assertElementXmlWithUri(tmpFile.toUri(), translatedXmlConfig);
      Files.delete(tmpFile);
      deleteFiles(disk1.toFile());
      deleteFiles(disk2.toFile());
      deleteFiles(disk3.toFile());
      deleteFiles(disk4.toFile());
    }
  }

  private static void assertElementXml(URL input, String config) {
    Assert.assertThat(Input.from(config), isSimilarTo(Input.fromURL(input)).ignoreComments()
        .ignoreWhitespace()
        .withNodeFilter(n -> !"concurrency-hint".equals(n.getLocalName()))
        .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes)));
  }

  private static void assertElementXmlWithUri(URI input, String config) {
    Assert.assertThat(Input.from(config), isSimilarTo(Input.fromURI(input)).ignoreComments()
        .ignoreWhitespace()
        .withNodeFilter(n -> !"concurrency-hint".equals(n.getLocalName()))
        .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes)));
  }

  private void deleteFiles(File tmpPath) {
    File[] allContents = tmpPath.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        file.delete();
      }
    }
  }

  private Path createXmlConfigFile() throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" " +
                       "xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"offheap\" unit=\"MB\">" + 10 + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "  <dataset name=\"test\" key-type=\"LONG\">\n" +
                       "    <tcs:offheap-resource>offheap</tcs:offheap-resource>\n" +
                       "  </dataset>\n" +
                       "</embedded>";
    Path embeddedXmlFile = temporaryFolder.newFolder().toPath().resolve("test.xml");
    Files.write(embeddedXmlFile, xmlConfig.getBytes());

    return embeddedXmlFile;
  }
}
