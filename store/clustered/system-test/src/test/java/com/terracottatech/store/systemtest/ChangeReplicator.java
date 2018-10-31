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

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

@SuppressWarnings("try")
public class ChangeReplicator implements ChangeListener<Integer>, AutoCloseable {
  private final Dataset<Integer> employeeReplicaDataset;
  private volatile CountDownLatch countDownLatch;
  private final DatasetManager datasetManager;
  private final DatasetReader<Integer> employeeReader;
  private volatile boolean missedEvent = false;
  private final DatasetWriterReader<Integer> employeeReplicaWriterReader;
  private final DatasetReader<Integer> employeeReplicaReader;

  public ChangeReplicator(DatasetManager datasetManager, DatasetConfiguration datasetConfiguration,
                          DatasetReader<Integer> employeeReader, int expectedEventCount) throws Exception {
    this.datasetManager = datasetManager;
    datasetManager.newDataset("employeeReplica", Type.INT, datasetConfiguration);
    employeeReplicaDataset = datasetManager.getDataset("employeeReplica", Type.INT);
    employeeReplicaWriterReader = employeeReplicaDataset.writerReader();
    employeeReplicaReader = employeeReplicaDataset.reader();
    countDownLatch = new CountDownLatch(expectedEventCount);
    this.employeeReader = employeeReader;
  }

  @Override
  public void close() throws Exception {
    try {
      employeeReplicaDataset.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    datasetManager.destroyDataset("employeeReplica");
  }

  @Override
  public synchronized void onChange(Integer key, ChangeType changeType) {
    Optional<Record<Integer>> employeeRecordOptional;

    switch (changeType) {
      case ADDITION:
        employeeRecordOptional = employeeReader.get(key);
        if (employeeRecordOptional.isPresent()) {
          Employee employee = new Employee(employeeRecordOptional.get());
          employeeReplicaWriterReader.add(key, employee.getCellSet());
        }
        break;

      case DELETION:
        employeeReplicaWriterReader.delete(key);
        break;

      case MUTATION:
        // Delete the record from the replica and then insert the fresh record
        employeeReplicaWriterReader.delete(key);

        employeeRecordOptional = employeeReader.get(key);
        if (employeeRecordOptional.isPresent()) {
          Employee employee = new Employee(employeeRecordOptional.get());
          employeeReplicaWriterReader.add(key, employee.getCellSet());
        }
        break;

      default:
        throw new IllegalArgumentException("Illegal ChangeType passed in");
    }

    countDownLatch.countDown();
  }

  @Override
  public void missedEvents() {
    missedEvent = true;
  }

  public void resetExpectedEventCount(int expectedEventCount) {
    countDownLatch = new CountDownLatch(expectedEventCount);
  }

  public void waitForEventCountDown() throws Exception {
    countDownLatch.await(10_000, TimeUnit.MILLISECONDS);
  }

  public boolean hasProcessedMissedEvent() throws Exception {
    return missedEvent;
  }

  public List<Employee> getEmployeeList() {
    return employeeReplicaReader.records().map(Employee::new).collect(toList());
  }

  public DatasetWriterReader<Integer> getEmployeeReplicaWriterReader() {
    return employeeReplicaWriterReader;
  }
}
