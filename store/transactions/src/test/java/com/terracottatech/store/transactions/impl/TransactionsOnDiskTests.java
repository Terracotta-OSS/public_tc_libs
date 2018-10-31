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
package com.terracottatech.store.transactions.impl;

import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

public class TransactionsOnDiskTests extends TransactionsTests {
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  Path currentDataRootDirectory;

  @Before
  @Override
  public void startUp() throws Exception {
    currentDataRootDirectory = temporaryFolder.newFolder().toPath();
    datasetManager = DatasetManager.embedded()
      .offheap("offheap", 128L, MemoryUnit.MB)
      .disk("disk",
        currentDataRootDirectory,
        EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
        EmbeddedDatasetManagerBuilder.FileMode.OVERWRITE)
      .build(); // <1>

    DatasetConfigurationBuilder embeddedConfigBuilder =
      datasetManager.datasetConfiguration()
        .offheap("offheap")
        .disk("disk"); // <2>

    datasetManager.newDataset("employee", Type.INT, embeddedConfigBuilder);
    datasetManager.newDataset("customer", Type.STRING, embeddedConfigBuilder);

    controller = new TransactionControllerImpl(datasetManager, embeddedConfigBuilder);

    employeeDataset = datasetManager.getDataset("employee", Type.INT);
    customerDataset = datasetManager.getDataset("customer", Type.STRING);

    employeeR = employeeDataset.reader();
    customerR = customerDataset.reader();

    employeeWR = employeeDataset.writerReader();
    customerWR = customerDataset.writerReader();
  }

  @After
  @Override
  public void cleanUp() throws StoreException {
    employeeDataset.close();
    customerDataset.close();
    datasetManager.destroyDataset("employee");
    datasetManager.destroyDataset("customer");
    datasetManager.close();
  }

  public void reboot() throws StoreException {
    employeeDataset.close();
    customerDataset.close();
    datasetManager.close();

    datasetManager = DatasetManager.embedded()
      .offheap("offheap", 128L, MemoryUnit.MB)
      .disk("disk",
        currentDataRootDirectory,
        EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
        EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
      .build(); // <1>

    controller = new TransactionControllerImpl(datasetManager);

    employeeDataset = datasetManager.getDataset("employee", Type.INT);
    customerDataset = datasetManager.getDataset("customer", Type.STRING);

    employeeR = employeeDataset.reader();
    customerR = customerDataset.reader();

    employeeWR = employeeDataset.writerReader();
    customerWR = customerDataset.writerReader();
  }
}
