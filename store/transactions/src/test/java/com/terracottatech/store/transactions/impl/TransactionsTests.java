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

import com.terracottatech.store.Cell;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.api.TransactionController;
import com.terracottatech.store.transactions.impl.TransactionControllerImpl;
import com.terracottatech.store.transactions.impl.TransactionOperation;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import static com.terracottatech.store.transactions.impl.TransactionID.getTransactionID;
import static com.terracottatech.store.transactions.impl.TransactionImpl.TRANSACTIONAL_UNCOMMITED_CELL_PREFIX;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertTrue;

public class TransactionsTests {
  DatasetManager datasetManager;
  Dataset<Integer> employeeDataset;
  Dataset<String> customerDataset;

  TransactionController controller;

  DatasetReader<Integer> employeeR ;
  DatasetReader<String> customerR;

  DatasetWriterReader<Integer> employeeWR ;
  DatasetWriterReader<String> customerWR;

  public static final DoubleCellDefinition EXPENDITURE = CellDefinition.defineDouble("expenditure");

  @Before
  public void startUp() throws Exception {
    datasetManager = DatasetManager.embedded()
      .offheap("offheap", 128L, MemoryUnit.MB)
      .build(); // <1>

    DatasetConfigurationBuilder embeddedConfigBuilder =
      datasetManager.datasetConfiguration()
        .offheap("offheap"); // <2>

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
  public void cleanUp() throws StoreException {
    employeeDataset.close();
    customerDataset.close();
    datasetManager.destroyDataset("employee");
    datasetManager.destroyDataset("customer");
    datasetManager.close();
  }

  void testUncommittedEmployeeRecord(DatasetReader<Integer> reader, TransactionOperation operation,
                                     int key, CellSet unCommittedcells, CellSet committedcells) {
    committedcells.forEach(c-> {
      MatcherAssert.assertThat(reader.get(key).get(), hasItem(c));
    });

    unCommittedcells.forEach(c -> {
      String newCellName = TRANSACTIONAL_UNCOMMITED_CELL_PREFIX + c.definition().name();
      Cell<?> cell = Cell.cell(newCellName, c.value());
      MatcherAssert.assertThat(reader.get(key).get(), hasItem(cell));
    });

    MatcherAssert.assertThat(reader.get(key).get(), hasItem(operation.cell()));
    assertTrue(getTransactionID(reader.get(key).get()).isPresent());

    assertTrue(reader.get(key).get().stream().count() == (unCommittedcells.size() + committedcells.size() + 2));
  }
}
