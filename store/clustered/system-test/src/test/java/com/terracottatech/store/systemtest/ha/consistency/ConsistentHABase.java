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
package com.terracottatech.store.systemtest.ha.consistency;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.builder.datasetconfiguration.ClusteredDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.systemtest.BaseSystemTest;
import com.terracottatech.testing.rules.EnterpriseCluster;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public abstract class ConsistentHABase extends BaseSystemTest {

  protected static final String DATASET_NAME = "employee";

  @ClassRule
  public static EnterpriseCluster CLUSTER = initConsistentClusterWithPassive("OffheapAndFRS.xmlfrag", 1, 2);

  protected enum ConfigType {

    OFFHEAP_ONLY,
    OFFHEAP_DISK {
      @Override
      protected DatasetConfigurationBuilder getDatasetBuilder() {
        return super.getDatasetBuilder().disk(CLUSTER_DISK_RESOURCE);
      }
    };

    public Tuple<ClusteredDatasetManagerBuilder, DatasetConfigurationBuilder> getConfigTuple() {
      return Tuple.of(getDatasetManagerBuilder(), getDatasetBuilder());
    }

    protected ClusteredDatasetManagerBuilder getDatasetManagerBuilder() {
      return DatasetManager.clustered(CLUSTER.getConnectionURI());
    }

    protected DatasetConfigurationBuilder getDatasetBuilder() {
      DatasetConfigurationBuilder configurationBuilder = new ClusteredDatasetConfigurationBuilder().offheap(CLUSTER_OFFHEAP_RESOURCE);
      configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
      return configurationBuilder;
    }
  }

  private final Tuple<ClusteredDatasetManagerBuilder, DatasetConfigurationBuilder> configTuple;

  protected DatasetManager datasetManager;
  protected Dataset<Integer> dataset;
  protected DatasetWriterReader<Integer> employeeWriterReader;
  protected DatasetReader<Integer> employeeReader;

  public ConsistentHABase(ConfigType configType) {
    this.configTuple = configType.getConfigTuple();
  }

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    datasetManager = getDatasetManagerBuilder().build();
    datasetManager.newDataset(getDatasetName(), Type.INT, getDatasetBuilder().build());
    dataset = datasetManager.getDataset(DATASET_NAME, Type.INT);

    employeeWriterReader = dataset.writerReader();
    employeeReader = dataset.reader();
  }

  protected String getDatasetName() {
    return DATASET_NAME;
  }

  protected ClusteredDatasetManagerBuilder getDatasetManagerBuilder() {
    return configTuple.getFirst();
  }

  protected DatasetConfigurationBuilder getDatasetBuilder() {
    return configTuple.getSecond();
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    if (dataset != null) {
      dataset.close();
    }
    if (datasetManager != null) {
      assertThat("Dataset could not be destroyed", datasetManager.destroyDataset(DATASET_NAME), is(true));
      datasetManager.close();
    }
  }

}
