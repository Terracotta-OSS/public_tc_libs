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

import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.NoSuchElementException;

import static com.terracottatech.store.CellSet.of;
import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.common.test.Employee.CELL_NUMBER;
import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.STREET_ADDRESS;
import static com.terracottatech.store.common.test.Employees.addNEmployeeRecords;
import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.stream;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ErrorResponseIT extends BaseClusterWithOffheapAndFRSTest {

  @Rule
  public Timeout globalTimeout = Timeout.seconds(900);

  @Test
  public void unavailableExceptionAtClientSideTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap("limited-server-resource")
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();

        // Induce out of memory by inserting large number of rows
        CellSet cellSet = of(NAME.newCell("Lalu Yadav"),
            CELL_NUMBER.newCell(108L),
            STREET_ADDRESS.newCell("Taraiya"),
            CITY_ADDRESS.newCell("Chapra"),
            COUNTRY_ADDRESS.newCell("Bihar"));

        // StoreRuntimeExecption should be thrown at client side with SovereignExtinctionException at server side
        try {
          addNEmployeeRecords(datasetWriterReader, cellSet, MAX_VALUE);
          fail("Offheap Memory provided to the test is too much, reduce it to induce out of memory");
        } catch (StoreRuntimeException e) {
          String msg = "Dataset operation failed due to server memory constraints. Please allocate more resources.";
          assertThat(e.getMessage(), containsString(msg));
          msg = "[com.terracottatech.store.common.exceptions.UnavailableException]: [com.terracottatech.sovereign.exceptions.SovereignExtinctionException]: Resource Allocation Failure:";
          assertThat(e.getCause().getMessage(), containsString(msg));
        }
      }

      datasetManager.destroyDataset("employee");
    }
  }

  @Test
  public void availableExceptionAtClientSideTest() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .build();

      assertThat(datasetManager.newDataset("employee", INT, datasetConfiguration), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", INT)) {
        DatasetWriterReader<Integer> datasetWriterReader = dataset.writerReader();

        CellSet cellSet = of(NAME.newCell("Muhammad Ali"),
            CELL_NUMBER.newCell(911L),
            STREET_ADDRESS.newCell("Madison Square Garden"),
            CITY_ADDRESS.newCell("New York"),
            COUNTRY_ADDRESS.newCell("USA"));
        int employeeID = generateUniqueEmpID();
        datasetWriterReader.add(employeeID, cellSet);

        try {
          datasetWriterReader.records().map(SALARY.valueOrFail()).count();

          fail("Expecting a NoSuchElementException here");
        } catch (NoSuchElementException e) {
          assertTrue(stream(e.getStackTrace())
              .anyMatch(stackTraceElement -> stackTraceElement.getClassName().equals(ErrorResponseIT.class.getName())));
        }
        ;
      }

      datasetManager.destroyDataset("employee");
    }
  }
}

