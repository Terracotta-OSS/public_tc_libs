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

import com.terracottatech.store.Cell;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.TestDataUtil;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import com.terracottatech.testing.rules.EnterpriseCluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.terracottatech.store.systemtest.ServiceConfigHelper.getServiceConfig;
import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.utilities.InetSocketAddressConvertor.getInetSocketAddresses;

/**
 * Base class for all the CRUD integration test classes.
 * Creates different types of DatasetManagers and then creates a dataset using every dataset manager
 * and finally the WriterReader those datasets become the parameters for the testcase for CRUD operations.
 */
@RunWith(Parameterized.class)
public abstract class CRUDTests {
  private static final String CLUSTER_OFFHEAP_RESOURCE = "primary-server-resource";
  private static final String CLUSTER_DISK_RESOURCE = "cluster-disk-resource";

  // Variations for every testcase
  public enum DatasetManagerType {
    TRANSIENT_EMBEDDED_DATASET_MANAGER {
      @Override
      void start() throws Exception {
        manager = DatasetManager.embedded()
            .offheap("offheap1", 128, MemoryUnit.MB)
            .build();
      }

      @Override
      void stop() {
        manager.close();
      }

      @Override
      DatasetConfiguration getDatasetConfiguration() {
        DatasetConfigurationBuilder configurationBuilder = manager.datasetConfiguration()
            .offheap("offheap1");
        configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
        return configurationBuilder.build();
      }

      @Override
      boolean isEmbedded() {
        return true;
      }
    },

    PERSISTENT_INMEMORY_EMBEDDED_DATASET_MANAGER {
      @Override
      void start() throws Exception {
        manager = DatasetManager.embedded()
            .offheap("offheap2", 128, MemoryUnit.MB)
            .disk("disk2",
                temporaryFolder.newFolder().toPath(),
                EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
                EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
            .build();
      }

      @Override
      DatasetConfiguration getDatasetConfiguration() {
        DatasetConfigurationBuilder configurationBuilder = manager.datasetConfiguration()
            .disk("disk2")
            .offheap("offheap2");
        configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
        return configurationBuilder.build();
      }

      @Override
      boolean isEmbedded() {
        return true;
      }
    },

    PERSISTENT_HYBRID_EMBEDDED_DATASET_MANAGER {
      @Override
      void start() throws Exception {
        manager = DatasetManager.embedded()
            .offheap("offheap2", 128, MemoryUnit.MB)
            .disk("disk2",
                temporaryFolder.newFolder().toPath(),
                EmbeddedDatasetManagerBuilder.PersistenceMode.HYBRID,
                EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
            .build();
      }

      @Override
      DatasetConfiguration getDatasetConfiguration() {
        DatasetConfigurationBuilder configurationBuilder = manager.datasetConfiguration()
            .disk("disk2")
            .offheap("offheap2");
        configurationBuilder = ((AdvancedDatasetConfigurationBuilder)configurationBuilder).concurrencyHint(2);
        return configurationBuilder.build();
      }

      @Override
      boolean isEmbedded() {
        return true;
      }
    },

    INMEMORY_CLUSTERED_DATASET_MANAGER {
      @Override
      void start() throws Exception {
        CLUSTER.getClusterControl().waitForActive();
        CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
        manager = DatasetManager.clustered(getInetSocketAddresses(CLUSTER.getClusterHostPorts()))
            .withClientAlias(name())
            .withClientTags("node-1", "webapp-2", "testing")
            .build();
      }

      @Override
      DatasetConfiguration getDatasetConfiguration() {
        DatasetConfigurationBuilder configurationBuilder = manager.datasetConfiguration()
            .offheap(CLUSTER_OFFHEAP_RESOURCE);
        configurationBuilder = ((AdvancedDatasetConfigurationBuilder)configurationBuilder).concurrencyHint(2);
        return configurationBuilder.build();
      }

      @Override
      boolean isEmbedded() {
        return false;
      }
    },

    HYBRID_CLUSTERED_DATASET_MANAGER {

      @Override
      void start() throws Exception {
        CLUSTER.getClusterControl().waitForActive();
        CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
        manager = DatasetManager.clustered(getInetSocketAddresses(CLUSTER.getClusterHostPorts()))
            .withClientAlias(name())
            .withClientTags("node-1", "webapp-2", "testing")
            .build();
      }

      @Override
      DatasetConfiguration getDatasetConfiguration() {
        DatasetConfigurationBuilder configurationBuilder = manager.datasetConfiguration()
            .offheap(CLUSTER_OFFHEAP_RESOURCE).disk(CLUSTER_DISK_RESOURCE, PersistentStorageEngine.HYBRID);
        configurationBuilder = ((AdvancedDatasetConfigurationBuilder)configurationBuilder).concurrencyHint(2);
        return configurationBuilder.build();
      }

      @Override
      boolean isEmbedded() {
        return false;
      }
    };

    protected DatasetManager manager;

    final DatasetManager getDatasetManager() {
      return manager;
    }

    abstract DatasetConfiguration getDatasetConfiguration();

    abstract boolean isEmbedded();

    abstract void start() throws Exception;

    void stop() {
      manager.close();
    }

  }

  @Parameterized.Parameter
  public DatasetManagerType datasetManagerType;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<DatasetManagerType> datasets() throws Exception {
    return Arrays.asList(DatasetManagerType.values());
  }

  @ClassRule
  public static final EnterpriseCluster CLUSTER = newCluster(2).withPlugins(getServiceConfig(CRUDTests.class, "OffheapAndFRS.xmlfrag")).build();

  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  @BeforeClass
  public static void start() throws Exception {
    for (DatasetManagerType type : DatasetManagerType.values()) {
      type.start();
    }
  }

  @AfterClass
  public static void stop() throws Exception {
    for (DatasetManagerType type : DatasetManagerType.values()) {
      type.stop();
    }
  }

  protected static List<Employee> employeeList;

  protected int id(int index) {
    return employeeList.get(index).getEmpID();
  }

  protected Cell<?>[] cellSetArray(int index) {
    return cellSet(index).toArray(new Cell<?>[0]);
  }

  protected CellSet cellSet(int index) {
    return employeeList.get(index).getCellSet();
  }

  protected static void initializeEmployeeList() {
    employeeList = TestDataUtil.getEmployeeList();
  }

  protected Dataset<Integer> getTestDataset() {
    try {
      DatasetManager manager = datasetManagerType.getDatasetManager();
      String name = getClass().getSimpleName() + "#" + testName.getMethodName();
      manager.newDataset(name, Type.INT, datasetManagerType.getDatasetConfiguration());
      return manager.getDataset(name, Type.INT);
    } catch (StoreException e) {
      throw new AssertionError(e);
    }
  }
}
