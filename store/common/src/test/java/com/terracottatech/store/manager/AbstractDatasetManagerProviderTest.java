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

package com.terracottatech.store.manager;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManagerConfiguration.DatasetInfo;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractDatasetManagerProviderTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final String DATASET_NAME = "test-dataset";
  private static final Type<String> TYPE = Type.STRING;
  private static final DatasetConfiguration DATASET_CONFIGURATION = mock(DatasetConfiguration.class);
  private static final DatasetManagerConfiguration DATASET_MANAGER_CONFIGURATION = mockDatasetManagerConfiguration();

  private DatasetManager datasetManager = mock(DatasetManager.class);

  @Test
  public void testCreateConfigurationModeWhenDatasetNotExists() throws StoreException {
    when(datasetManager.newDataset(DATASET_NAME, TYPE, DATASET_CONFIGURATION)).thenReturn(true);
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.CREATE);
  }

  @Test
  public void testCreateConfigurationModeWhenDatasetExists() throws StoreException {
    when(datasetManager.newDataset(DATASET_NAME, TYPE, DATASET_CONFIGURATION)).thenReturn(false);
    expectedException.expect(StoreRuntimeException.class);
    expectedException.expectMessage("A dataset with the name " + DATASET_NAME + " already exists");
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.CREATE);
  }

  @Test
  public void testCreateConfigurationModeWhenDatasetExistsWithDifferentKeyType() throws StoreException {
    when(datasetManager.newDataset(DATASET_NAME, TYPE, DATASET_CONFIGURATION))
        .thenThrow(mock(DatasetKeyTypeMismatchException.class));
    expectedException.expect(StoreRuntimeException.class);
    expectedException.expectMessage("A dataset with the name " + DATASET_NAME + " already exists");
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.CREATE);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testValidateConfigureModeWhenDatasetExists() throws StoreException {
    when(datasetManager.getDataset(DATASET_NAME, TYPE)).thenReturn(mock(Dataset.class));
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.VALIDATE);
  }

  @Test
  public void testValidateConfigureModeWhenDatasetExistsWithDifferentKeyType() throws StoreException {
    when(datasetManager.getDataset(DATASET_NAME, TYPE)).thenThrow(mock(DatasetKeyTypeMismatchException.class));
    expectedException.expect(StoreRuntimeException.class);
    expectedException.expectMessage("Validation failed for dataset with name " + DATASET_NAME);
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.VALIDATE);
  }

  @Test
  public void testValidateConfigureModeWhenDatasetMissing() throws StoreException {
    when(datasetManager.getDataset(DATASET_NAME, TYPE)).thenThrow(DatasetMissingException.class);
    expectedException.expect(StoreRuntimeException.class);
    expectedException.expectMessage("Dataset with name " + DATASET_NAME + " is missing");
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.VALIDATE);
  }

  @Test
  public void testAutoConfigurationModeWhenDatasetNotExists() throws StoreException {
    when(datasetManager.newDataset(DATASET_NAME, TYPE, DATASET_CONFIGURATION)).thenReturn(true);
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.AUTO);
  }

  @Test
  public void testAutoConfigurationModeWhenDatasetExists() throws StoreException {
    when(datasetManager.newDataset(DATASET_NAME, TYPE, DATASET_CONFIGURATION)).thenReturn(false);
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.AUTO);
  }

  @Test
  public void testAutoConfigurationModeWhenDatasetExistsWithDifferentKeyType() throws StoreException {
    when(datasetManager.newDataset(DATASET_NAME, TYPE, DATASET_CONFIGURATION)).thenThrow(DatasetKeyTypeMismatchException.class);
    expectedException.expect(StoreRuntimeException.class);
    expectedException.expectMessage("Validation failed for dataset with name " + DATASET_NAME);
    provider.ensureDatasets(datasetManager, DATASET_MANAGER_CONFIGURATION, ConfigurationMode.AUTO);
  }


  private AbstractDatasetManagerProvider provider = new AbstractDatasetManagerProvider() {
    @Override
    public Set<Type> getSupportedTypes() {
      return null;
    }

    @Override
    public ClusteredDatasetManagerBuilder clustered(URI uri) {
      return null;
    }

    @Override
    public ClusteredDatasetManagerBuilder clustered(Iterable<InetSocketAddress> servers) {
      return null;
    }

    @Override
    public ClusteredDatasetManagerBuilder secureClustered(URI uri, Path securityRootDirectory) {
      return null;
    }

    @Override
    public ClusteredDatasetManagerBuilder secureClustered(Iterable<InetSocketAddress> servers, Path securityRootDirectory) {
      return null;
    }

    @Override
    public EmbeddedDatasetManagerBuilder embedded() {
      return null;
    }

    @Override
    public DatasetManager using(DatasetManagerConfiguration datasetManagerConfiguration, ConfigurationMode configurationMode) {
      return null;
    }
  };

  private static DatasetManagerConfiguration mockDatasetManagerConfiguration() {
    DatasetManagerConfiguration datasetManagerConfiguration = mock(DatasetManagerConfiguration.class);
    Map<String, DatasetInfo<?>> datasetInfoMap = new HashMap<>();
    datasetInfoMap.put(DATASET_NAME, new DatasetInfo<>(TYPE, DATASET_CONFIGURATION));
    when(datasetManagerConfiguration.getDatasets()).thenReturn(datasetInfoMap);
    return datasetManagerConfiguration;
  }
}