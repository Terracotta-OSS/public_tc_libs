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
package com.terracottatech.store.systemtest.ha;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.terracottatech.store.Cell;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.TestDataUtil;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class CRUDOpsHAIT extends HABase {

  private static List<Employee> employees = TestDataUtil.getEmployeeList();
  private static int maxEmployees = employees.size();
  private static Random random = new Random();

  @Parameterized.Parameters(name = "{0}")
  public static ConfigType[] datasets() throws Exception {
    return ConfigType.values();
  }

  @Mock
  private Map<Integer, Employee> employeeMap;

  @Mock
  private AtomicReference<Throwable> failure;

  public CRUDOpsHAIT(ConfigType configType) {
    super(configType);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCRUD() throws Exception {
    Cell<?> cells[] = new Cell<?>[] {
        Employee.NAME.newCell("Pierce Brosnan"),
        Employee.SSN.newCell(007),
        Employee.CURRENT.newCell(false)
    };

    employeeWriterReader.add(1, cells);

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    assertThat(employeeReader.get(1).get(), containsInAnyOrder(cells));
    String newName = "Daniel Craig";
    employeeWriterReader.update(1, UpdateOperation.write(Employee.NAME).value(newName));

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    assertThat(employeeReader.get(1).get().get(Employee.NAME).get(), is(newName));
    employeeWriterReader.delete(1);

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    assertThat(employeeReader.get(1).isPresent(), is(false));
  }

  @Test
  public void testStreamOrderMaintainedByCRUD() throws Exception {
    getAdder(30).run();
    getUpdater(20).run();
    getAdder(10).run();
    getDeleter(10).run();
    getUpdater(5).run();

    List<Employee> beforeFailover = employeeWriterReader.records().map(Employee::new).collect(Collectors.toList());
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();
    List<Employee> afterFailover = employeeWriterReader.records().map(Employee::new).collect(Collectors.toList());

    assertThat(afterFailover, is(beforeFailover));
  }

  @Test
  public void testFailoverActiveCluster() throws Exception {
    AtomicBoolean stopped = new AtomicBoolean(false);
    AtomicReference<Throwable> getFailed = new AtomicReference<>();
    AtomicReference<Throwable> addFailed = new AtomicReference<>();
    AtomicReference<Throwable> updateFailed = new AtomicReference<>();
    AtomicReference<Throwable> deleteFailed = new AtomicReference<>();

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    IntStream.range(0, 4).forEach(i -> executorService.submit(getGetter(stopped, getFailed)));
    IntStream.range(0, 3).forEach(i -> executorService.submit(getAdder(stopped, addFailed)));
    IntStream.range(0, 2).forEach(i -> executorService.submit(getUpdater(stopped, updateFailed)));
    IntStream.range(0, 1).forEach(i -> executorService.submit(getDeleter(stopped, deleteFailed)));

    Thread.sleep(2000); // Sleeping just to give some time for the operating threads to run
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    Thread.sleep(2000); // Sleeping some more to let the operating threads run
    stopped.set(true);  //Stopping all dataset user threads
    executorService.shutdown();

    assertNotFailed("get", getFailed);
    assertNotFailed("add", addFailed);
    assertNotFailed("update", updateFailed);
    assertNotFailed("delete", deleteFailed);
  }

  private void assertNotFailed(String operation, AtomicReference<Throwable> failureReference) {
    Throwable t = failureReference.get();
    if (t != null) {
      throw new AssertionError(operation + " failed", t);
    }
  }

  private Runnable getGetter(AtomicBoolean stopped, AtomicReference<Throwable> getFailed) {
    return getGetter(stopped, getFailed, employeeMap, Integer.MAX_VALUE);
  }

  private Runnable getGetter(int limit) {
    return getGetter(mock(AtomicBoolean.class), failure, employeeMap, limit);
  }

  private Runnable getGetter(AtomicBoolean stopped, AtomicReference<Throwable> failed, Map<Integer, Employee> employeeMap, int limit) {
    return getExecutor(stopped, failed, limit, () -> {
      int key = random.nextInt(maxEmployees);
      employeeWriterReader.get(key).ifPresent(cells -> employeeMap.put(key, new Employee(cells)));
    });
  }

  private Runnable getAdder(AtomicBoolean stopped, AtomicReference<Throwable> addFailed) {
    return getAdder(stopped, addFailed, employeeMap, Integer.MAX_VALUE);
  }

  private Runnable getAdder(int limit) {
    return getGetter(mock(AtomicBoolean.class), failure, employeeMap, limit);
  }

  private Runnable getAdder(AtomicBoolean stopped, AtomicReference<Throwable> failed, Map<Integer, Employee> employeeMap, int limit) {
    return getExecutor(stopped, failed, limit, () -> {
      Employee employee = employees.get(random.nextInt(maxEmployees));
      int key = random.nextInt(maxEmployees);
      boolean added = employeeWriterReader.add(key, employee.getCellSet());
      if (added) {
        employeeMap.put(key, employee);
      }
    });
  }

  private Runnable getUpdater(AtomicBoolean stopped, AtomicReference<Throwable> updateFailed) {
    return getUpdater(stopped, updateFailed, employeeMap, Integer.MAX_VALUE);
  }

  private Runnable getUpdater(int limit) {
    return getGetter(mock(AtomicBoolean.class), failure, employeeMap, limit);
  }

  private Runnable getUpdater(AtomicBoolean stopped, AtomicReference<Throwable> failed, Map<Integer, Employee> employeeMap, int limit) {
    return getExecutor(stopped, failed, limit, () -> {
      Employee employee = employees.get(random.nextInt(maxEmployees));
      int key = random.nextInt(maxEmployees);
      boolean updated = employeeWriterReader.update(key, UpdateOperation.install(employee.getCellSet()));
      if (updated) {
        employeeMap.put(key, employee);
      }
    });
  }

  private Runnable getDeleter(AtomicBoolean stopped, AtomicReference<Throwable> deleteFailed) {
    return getDeleter(stopped, deleteFailed, employeeMap, Integer.MAX_VALUE);
  }

  private Runnable getDeleter(int limit) {
    return getDeleter(mock(AtomicBoolean.class), failure, employeeMap, limit);
  }

  private Runnable getDeleter(AtomicBoolean stopped, AtomicReference<Throwable> failed, Map<Integer, Employee> employeeMap, int limit) {
    return getExecutor(stopped, failed, limit, () -> {
      int key = random.nextInt(maxEmployees);
      boolean deleted = employeeWriterReader.delete(key);
      if (deleted) {
        employeeMap.remove(key);
      }
    });
  }

  private Runnable getExecutor(AtomicBoolean stopped, AtomicReference<Throwable> failed, int limit, Runnable runnable) {
    return () -> {
      for (int i = 0; i < limit; i++) {
        if (stopped.get()) {
          break;
        }
        try {
          runnable.run();
        } catch (Throwable t) {
          failed.set(t);
          stopped.set(true);
        }
      }
    };
  }
}
