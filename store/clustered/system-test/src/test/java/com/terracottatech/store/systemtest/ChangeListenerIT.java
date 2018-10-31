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
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.UpdateOperation;

import com.terracottatech.store.common.test.Employee;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.store.common.test.Employees.generateUniqueEmpID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class ChangeListenerIT extends CRUDTests {

  private ChangeReplicator changeReplicator;
  private ArrayList<ChangeEvent> manualChangeList;

  @Before
  public void initializeTest() throws Exception {
    // Currently, ChangeListener is supported only for clustered Datasets, so ignore the tests for embedded DatasetManagers
    assumeTrue(!datasetManagerType.isEmbedded());
    manualChangeList = new ArrayList<>();
  }

  @After
  public void cleanup() throws Exception {
    if (changeReplicator != null) {
      changeReplicator.close();
    }
  }

  @Test
  public void successfulChangeTests() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      // Expected Number of events to be caught by Change Listener in this test case
      ChangeRecorder changeRecorder = new ChangeRecorder(5);
      changeReplicator = new ChangeReplicator(datasetManagerType.getDatasetManager(), datasetManagerType.getDatasetConfiguration(), employeeReader, 5);

      // Register both the change listeners
      employeeReader.registerChangeListener(changeRecorder);
      employeeReader.registerChangeListener(changeReplicator);

      // 1. Add first record
      int employeeID1 = generateUniqueEmpID();
      CellSet cellSet = CellSet.of(
          Employee.NAME.newCell("Sachin Tendulkar"),
          Employee.TELEPHONE.newCell(100L)
      );
      employeeWriterReader.add(employeeID1, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID1));

      // 2. Add second record
      int employeeID2 = generateUniqueEmpID();
      cellSet = CellSet.of(
          Employee.NAME.newCell("Ricky Ponting"),
          Employee.TELEPHONE.newCell(512L)
      );
      employeeWriterReader.add(employeeID2, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID2));

      // 3. Add third record
      int employeeID3 = generateUniqueEmpID();
      cellSet = CellSet.of(
          Employee.NAME.newCell("Brian Lara"),
          Employee.TELEPHONE.newCell(911L)
      );
      employeeWriterReader.add(employeeID3, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID3));

      // 4. Update first record
      employeeWriterReader.update(employeeID1, UpdateOperation.write(Employee.TELEPHONE.newCell(911L)));
      manualChangeList.add(ChangeEvent.updateEvent(employeeID1));

      // 5. Delete second record
      employeeWriterReader.delete(employeeID2);
      manualChangeList.add(ChangeEvent.deleteEvent(employeeID2));

      // Wait until all the event processing has completed
      changeRecorder.waitForEventCountDown();
      changeReplicator.waitForEventCountDown();

      // The manual list and the list created by the listener should be same
      assertTrue(changeRecorder.changeListContainsInAnyOrder(manualChangeList));

      // The employee and the employeeReplica datasets should have the same content
      List<Employee> employees = employeeReader.records().map(Employee::new).collect(toList());
      List<Employee> replicaEmployees = changeReplicator.getEmployeeList();
      assertThat(employees, containsInAnyOrder(replicaEmployees.toArray()));

      assertFalse(changeRecorder.hasProcessedMissedEvent());
      assertFalse(changeReplicator.hasProcessedMissedEvent());
    }
  }

  @Test
  public void unsuccessfulChangeTest() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      // Expected Number of events to be caught by Change Listener in this test case
      ChangeRecorder changeRecorder = new ChangeRecorder(2);
      changeReplicator = new ChangeReplicator(datasetManagerType.getDatasetManager(), datasetManagerType.getDatasetConfiguration(), employeeReader, 2);

      // Register both the change listeners
      employeeReader.registerChangeListener(changeRecorder);
      employeeReader.registerChangeListener(changeReplicator);

      // Add first record
      int employeeID1 = generateUniqueEmpID();
      CellSet cellSet = CellSet.of(Employee.NAME.newCell("Virat Kohli"), Employee.TELEPHONE.newCell(100L));
      employeeWriterReader.add(employeeID1, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID1));

      // Adding record with same key should fail
      cellSet = CellSet.of(Employee.NAME.newCell("Joe Root"), Employee.TELEPHONE.newCell(512L));
      assertFalse(employeeWriterReader.add(employeeID1, cellSet));

      // Updating non existing record should fail
      int employeeID2 = generateUniqueEmpID();
      assertFalse(employeeWriterReader.update(employeeID2, UpdateOperation.write(Employee.TELEPHONE.newCell(911L))));

      // Deleting non existing record should fail
      assertFalse(employeeWriterReader.delete(employeeID2));

      // Successfully added another row to sandwich the failed operations above
      cellSet = CellSet.of(Employee.NAME.newCell("A B De Villiers"), Employee.TELEPHONE.newCell(112L));
      employeeWriterReader.add(employeeID2, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID2));


      // Wait until all the event processing has completed
      changeRecorder.waitForEventCountDown();
      changeReplicator.waitForEventCountDown();

      // All the unsuccessful Add, Update and Delete operations above should not have invoked the ChangeListeners
      // The manual list and the list created by the listener should be same
      assertTrue(changeRecorder.changeListContainsInAnyOrder(manualChangeList));

      // The employee and the employeeReplica datasets should have the same content
      List<Employee> employees = employeeReader.records().map(Employee::new).collect(toList());
      List<Employee> replicaEmployees = changeReplicator.getEmployeeList();
      assertThat(employees, containsInAnyOrder(replicaEmployees.toArray()));

      assertFalse(changeRecorder.hasProcessedMissedEvent());
      assertFalse(changeReplicator.hasProcessedMissedEvent());
    }
  }

  @Test
  public void registerUnregisterTest() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // Expected Number of events to be caught by Change Listener before it gets de-registered
      ChangeRecorder changeRecorder = new ChangeRecorder(3);
      changeReplicator = new ChangeReplicator(datasetManagerType.getDatasetManager(), datasetManagerType.getDatasetConfiguration(), employeeReader, 3);

      // 1. Register both the change listeners
      employeeReader.registerChangeListener(changeRecorder);
      employeeReader.registerChangeListener(changeReplicator);

      // 2. Add three records
      int employeeID1 = generateUniqueEmpID();
      CellSet cellSet = CellSet.of(
          Employee.NAME.newCell("R. Ashwin"),
          Employee.COUNTRY_ADDRESS.newCell("India"),
          Employee.CITY_ADDRESS.newCell("Bangalore")
      );
      employeeWriterReader.add(employeeID1, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID1));

      int employeeID2 = generateUniqueEmpID();
      cellSet = CellSet.of(
          Employee.NAME.newCell("J.R. Hazlewood"),
          Employee.COUNTRY_ADDRESS.newCell("Australia"),
          Employee.CITY_ADDRESS.newCell("Sydney")
      );
      employeeWriterReader.add(employeeID2, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID2));

      int employeeID3 = generateUniqueEmpID();
      cellSet = CellSet.of(
          Employee.NAME.newCell("J.M. Anderson"),
          Employee.COUNTRY_ADDRESS.newCell("England"),
          Employee.CITY_ADDRESS.newCell("London")
      );
      employeeWriterReader.add(employeeID3, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID3));

      // Wait until all the event processing has completed
      changeRecorder.waitForEventCountDown();
      changeReplicator.waitForEventCountDown();

      // 3. Unregister the change listeners
      employeeReader.deregisterChangeListener(changeRecorder);
      employeeReader.deregisterChangeListener(changeReplicator);

      // 4. Add fourth record which will not trigger the call to change listener
      int employeeID4 = generateUniqueEmpID();
      cellSet = CellSet.of(
          Employee.NAME.newCell("D.W. Steyn"),
          Employee.COUNTRY_ADDRESS.newCell("South Africa"),
          Employee.CITY_ADDRESS.newCell("Durban")
      );
      employeeWriterReader.add(employeeID4, cellSet);
      Employee missingAddEmployee = new Employee(employeeReader.get(employeeID4).get());

      // 5. Delete first record which will again not trigger call to change listener
      employeeWriterReader.delete(employeeID1);
      int missingDeleteEmployee = employeeID1;

      // 6. Update second record which will not trigger call to change listener
      employeeWriterReader.update(employeeID2, UpdateOperation.write(Employee.CITY_ADDRESS.newCell("Gold Coast")));
      int missingUpdateEmployee = employeeID2;
      UpdateOperation.CellUpdateOperation<Integer, String> missingCellUpdateOperation = UpdateOperation.write(Employee.CITY_ADDRESS.newCell("Gold Coast"));

      // 7. Register both the change listeners back again after all the three operations have been lost by change listeners
      changeRecorder.resetExpectedEventCount(3);
      changeReplicator.resetExpectedEventCount(3);
      employeeReader.registerChangeListener(changeRecorder);
      employeeReader.registerChangeListener(changeReplicator);

      // 8. Add fifth record which will now trigger the call to change listener
      int employeeID5 = generateUniqueEmpID();
      cellSet = CellSet.of(
          Employee.NAME.newCell("S.C.J. Broad"),
          Employee.COUNTRY_ADDRESS.newCell("England"),
          Employee.CITY_ADDRESS.newCell("Nottingham")
      );
      employeeWriterReader.add(employeeID5, cellSet);
      manualChangeList.add(ChangeEvent.addEvent(employeeID5));

      // 9. Delete third record
      employeeWriterReader.delete(employeeID3);
      manualChangeList.add(ChangeEvent.deleteEvent(employeeID3));

      // 10. Update fifth record
      employeeWriterReader.update(employeeID5, UpdateOperation.write(Employee.CITY_ADDRESS.newCell("Burnley")));
      manualChangeList.add(ChangeEvent.updateEvent(employeeID5));

      // Wait until all the event processing has completed
      changeRecorder.waitForEventCountDown();
      changeReplicator.waitForEventCountDown();

      // The manual list and the list created by the listener should be same
      assertTrue(changeRecorder.changeListContainsInAnyOrder(manualChangeList));

      // The employee and the employeeReplica datasets should **not** have the same content because of the missed events on replica
      List<Employee> employees = employeeReader.records().map(Employee::new).collect(toList());
      List<Employee> replicaEmployees = changeReplicator.getEmployeeList();
      assertThat(employees, not(containsInAnyOrder(replicaEmployees.toArray())));

      // But after performing the missing operations on replica, both should be identical
      changeReplicator.getEmployeeReplicaWriterReader().add(missingAddEmployee.getEmpID(), missingAddEmployee.getCellSet());
      changeReplicator.getEmployeeReplicaWriterReader().delete(missingDeleteEmployee);
      changeReplicator.getEmployeeReplicaWriterReader().update(missingUpdateEmployee, missingCellUpdateOperation);

      employees = employeeReader.records().map(Employee::new).collect(toList());
      replicaEmployees = changeReplicator.getEmployeeList();
      assertThat(employees, containsInAnyOrder(replicaEmployees.toArray()));

      assertFalse(changeRecorder.hasProcessedMissedEvent());
      assertFalse(changeReplicator.hasProcessedMissedEvent());
    }
  }

  @Test
  public void missedEventTest() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      // Current size of events buffer is 10_000 events, hence taking this as 11_000
      int numEvents = 11_000;
      SlowChangeListener slowChangeListener = new SlowChangeListener();
      employeeReader.registerChangeListener(slowChangeListener);

      for (int i = 0; i < numEvents; i++) {
        employeeWriterReader.add(generateUniqueEmpID());
      }

      slowChangeListener.unblockListener();
      assertTrue(slowChangeListener.hasProcessedMissedEvent());
    }
  }
}
