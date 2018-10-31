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

import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.ConfigurationMode;
import com.terracottatech.store.manager.DatasetManager;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URL;

import static com.terracottatech.store.manager.XmlConfiguration.parseDatasetManagerConfig;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConfigurationModeIT extends BaseClusterWithOffheapAndFRSTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String DATASET_NAME = ConfigurationModeIT.class.getName() + "-dataset";

  @Test
  public void testCreateModeWhenDatasetNotExists() throws Exception {
    URL xmlConfigFile = createXmlConfigFile();
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE);
  }


  @Test
  public void testCreateModeWhenDatasetExists() throws Exception {
    createDataset();

    URL xmlConfigFile = createXmlConfigFile();
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("A dataset with the name " + DATASET_NAME + " already exists");
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE);
  }

  @Test
  public void testCreateModeWhenDatasetExistsWithDifferentKeyType() throws Exception {
    createDatasetWithDifferentKeyType();

    URL xmlConfigFile = createXmlConfigFile();
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("A dataset with the name " + DATASET_NAME + " already exists");
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.CREATE);
  }

  @Test
  public void testValidateConfigureModeWhenDatasetExists() throws Exception {
    createDataset();

    URL xmlConfigFile = createXmlConfigFile();
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.VALIDATE).close();
  }

  @Test
  public void testValidateConfigureModeWhenDatasetExistsWithoutClosingDatasetManager() throws Exception {
    createDataset();

    URL xmlConfigFile = createXmlConfigFile();
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.VALIDATE);
  }

  @Test
  public void testValidateModeWhenDatasetMissing() throws Exception {
    URL xmlConfigFile = createXmlConfigFile();
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("Dataset with name " + DATASET_NAME + " is missing");
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.VALIDATE);
  }

  @Test
  public void testValidateModeWhenDatasetExistsWithDifferentKeyType() throws Exception {
    createDatasetWithDifferentKeyType();

    URL xmlConfigFile = createXmlConfigFile();
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("Validation failed for dataset with name " + DATASET_NAME);
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.VALIDATE);
  }

  @Test
  public void testAutoModeWhenDatasetNotExists() throws Exception {
    URL xmlConfigFile = createXmlConfigFile();
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.AUTO).close();
  }

  @Test
  public void testAutoModeWhenDatasetExists() throws Exception {
    createDataset();

    URL xmlConfigFile = createXmlConfigFile();
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.AUTO).close();
  }

  @Test
  public void testAutoModeWhenDatasetExistsWithDifferentKeyType() throws Exception {
    createDatasetWithDifferentKeyType();

    URL xmlConfigFile = createXmlConfigFile();
    exception.expect(StoreRuntimeException.class);
    exception.expectMessage("Validation failed for dataset with name " + DATASET_NAME);
    DatasetManager.using(parseDatasetManagerConfig(xmlConfigFile), ConfigurationMode.AUTO);
  }

  private URL createXmlConfigFile() {
    String datasetConfig = "  <dataset name=\"" + DATASET_NAME + "\" key-type=\"STRING\">\n" +
                           "    <tcs:offheap-resource>" +  CLUSTER_OFFHEAP_RESOURCE + "</tcs:offheap-resource>\n" +
                           "  </dataset>\n";
    return XmlUtils.createXmlConfigFile(CLUSTER.getClusterHostPorts(), datasetConfig, temporaryFolder);
  }

  @After
  public void tearDown() throws Exception {
    destroyDataset();
  }

  private void createDataset() throws Exception {
    createDataset(Type.STRING);
  }

  private void createDatasetWithDifferentKeyType() throws Exception {
    createDataset(Type.LONG);
  }

  private <K extends Comparable<K>> void createDataset(Type<K> type) throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap(CLUSTER_OFFHEAP_RESOURCE).build();
      boolean created = datasetManager.newDataset(DATASET_NAME, type, datasetConfiguration);
      assertThat(created, is(true));
    }
  }

  private void destroyDataset() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      datasetManager.destroyDataset(DATASET_NAME);
    }
  }
}
