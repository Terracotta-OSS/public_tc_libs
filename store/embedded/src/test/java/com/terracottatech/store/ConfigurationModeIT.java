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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.ConfigurationMode;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.terracottatech.store.manager.XmlConfiguration.parseDatasetManagerConfig;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConfigurationModeIT {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String DATASET_NAME = ConfigurationModeIT.class.getName() + "-dataset";

  @Test
  public void testCreateModeWhenDatasetNotExists() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    URL xmlConfigFile = createXmlConfigFile(diskPath);
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE).close();
  }


  @Test
  public void testCreateModeWhenDatasetExists() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    createDataset(diskPath);

    URL xmlConfigFile = createXmlConfigFile(diskPath);
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("A dataset with the name " + DATASET_NAME + " already exists");
    // explicitly call close in case test does not take exception path
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE).close();
  }

  @Test
  public void testCreateModeWhenDatasetExistsWithDifferentKeyType() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    createDatasetWithDifferentKeyType(diskPath);

    URL xmlConfigFile = createXmlConfigFile(diskPath);
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("A dataset with the name " + DATASET_NAME + " already exists");
    // explicitly call close in case test does not take exception path
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE).close();
  }

  @Test
  public void testValidateModeWhenDatasetExists() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    createDataset(diskPath);

    URL xmlConfigFile = createXmlConfigFile(diskPath);
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.VALIDATE).close();
  }

  @Test
  public void testValidateModeWhenDatasetMissing() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    URL xmlConfigFile = createXmlConfigFile(diskPath);
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("Dataset with name " + DATASET_NAME + " is missing");
    // explicitly call close in case test does not take exception path
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.VALIDATE).close();
  }

  @Test
  public void testValidateModeWhenDatasetExistsWithDifferentKeyType() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    createDatasetWithDifferentKeyType(diskPath);

    URL xmlConfigFile = createXmlConfigFile(diskPath);
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("Validation failed for dataset with name " + DATASET_NAME);
    // explicitly call close in case test does not take exception path
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.VALIDATE).close();
  }

  @Test
  public void testAutoModeWhenDatasetNotExists() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    URL xmlConfigFile = createXmlConfigFile(diskPath);
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.AUTO).close();
  }

  @Test
  public void testAutoModeWhenDatasetExists() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    createDataset(diskPath);

    URL xmlConfigFile = createXmlConfigFile(diskPath);
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.AUTO).close();
  }

  @Test
  public void testAutoModeWhenDatasetExistsWithDifferentKeyType() throws Exception {
    Path diskPath = temporaryFolder.newFolder().toPath();
    createDatasetWithDifferentKeyType(diskPath);

    URL xmlConfigFile = createXmlConfigFile(diskPath);
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("Validation failed for dataset with name " + DATASET_NAME);
    // explicitly call close in case test does not take exception path
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.AUTO).close();
  }

  private URL createXmlConfigFile(Path diskPath) throws Exception {
    String xmlConfig = "<embedded xmlns=\"http://www.terracottatech.com/v1/terracotta/store/embedded\" xmlns:tcs=\"http://www.terracottatech.com/v1/terracotta/store\">\n" +
                       "  <offheap-resources>\n" +
                       "    <offheap-resource name=\"offheap\" unit=\"MB\">" + 10 + "</offheap-resource>\n" +
                       "  </offheap-resources>\n" +
                       "  <disk-resources>\n" +
                       "    <disk-resource name=\"disk\" " + "file-mode=\"REOPEN_OR_NEW\">"
                       + diskPath + "</disk-resource>" +
                       "  </disk-resources>\n" +
                       "  <dataset name=\"" + DATASET_NAME + "\" key-type=\"STRING\">\n" +
                       "    <tcs:offheap-resource>offheap</tcs:offheap-resource>\n" +
                       "    <tcs:disk-resource>disk</tcs:disk-resource>\n" +
                       "  </dataset>\n" +
                       "</embedded>";
    Path embeddedXmlFile = temporaryFolder.newFolder().toPath().resolve("test.xml");
    Files.write(embeddedXmlFile, xmlConfig.getBytes());
    return embeddedXmlFile.toUri().toURL();
  }


  private void createDataset(Path diskPath) throws Exception {
    createDataset(Type.STRING, diskPath);
  }

  private void createDatasetWithDifferentKeyType(Path diskPath) throws Exception {
    createDataset(Type.LONG, diskPath);
  }

  private <K extends Comparable<K>> void createDataset(Type<K> type, Path diskPath) throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
                                                       .offheap("offheap", 10, MemoryUnit.MB)
                                                       .disk("disk", diskPath,
                                                             EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
                                                             EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
                                                       .build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("offheap").disk("disk").build();
      boolean created = datasetManager.newDataset(DATASET_NAME, type, datasetConfiguration);
      assertThat(created, is(true));
    }
  }
}
