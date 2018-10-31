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

package com.terracottatech.store.client.stream;

import com.terracottatech.store.Record;
import com.terracottatech.store.StoreStreamTerminatedException;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.TestDataUtil;
import com.terracottatech.store.stream.RecordStream;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.tc.util.Assert.assertEquals;
import static com.terracottatech.store.client.stream.AbstractBaseHAPassthroughTest.TestSetup.MULTI_STRIPE_SINGLE_ACTIVE_FAILOVER;
import static com.terracottatech.store.common.test.Employee.GENDER;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.SSN;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

public class RetryableStreamTest extends AbstractBaseHAPassthroughTest {

  List<Employee> employeeList;

  @Before
  public void initializeMapAndDataset() {

    employeeList = TestDataUtil.getEmployeeList();

    // Fill in data to employee Dataset using the employeeList
    employeeList.stream()
        .forEach(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));
  }

  @Test
  public void recordStreamtoArrayRetryTest() {
    Object expected[] = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .toArray();

    boolean[] failoverOccurred = new boolean[1];
    Object actual[] = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .toArray();

    assertTrue(failoverOccurred[0]);
    assertThat(asList(actual), containsInAnyOrder(expected));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void recordStreamtoArrayIntFunctionRetryTest() {
    Stream<Employee> expected = asList(employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .toArray(Record[]::new)).stream().map(Employee::new);

    boolean[] failoverOccurred = new boolean[1];
    Stream<Employee> actual = asList(employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .toArray(Record[]::new)).stream().map(Employee::new);

    assertTrue(failoverOccurred[0]);
    assertThat(actual.collect(toList()), containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void recordStreamReduceRetryTest() {
    Employee expected = new Employee(employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .reduce((record1, record2) -> (record1.get(SALARY).get() > record2.get(SALARY).get()) ? record1 : record2)
        .get());

    boolean[] failoverOccurred = new boolean[1];
    Employee actual = new Employee(employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .reduce((record1, record2) -> (record1.get(SALARY).get() > record2.get(SALARY).get()) ? record1 : record2)
        .get());

    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamReduce2RetryTest() {
    double expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .reduce(0D,
            (max, record) -> (max > record.get(SALARY).get()) ? max : record.get(SALARY).get(),
            (x, y) -> (x > y) ? x : y);

    boolean[] failoverOccurred = new boolean[1];
    double actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .reduce(0D,
            (max, record) -> (max > record.get(SALARY).get()) ? max : record.get(SALARY).get(),
            (x, y) -> (x > y) ? x : y);

    assertTrue(failoverOccurred[0]);
    assertEquals(actual, expected, 1e-8);
  }

  @Test
  public void recordStreamCollectRetryTest() {
    List<Employee> expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .map(Employee::new)
        .collect(toList());

    boolean[] failoverOccurred = new boolean[1];
    List<Employee> actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .map(Employee::new)
        .collect(toList());

    assertTrue(failoverOccurred[0]);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void recordStreamCollect2RetryTest() {
    List<Employee> expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .map(Employee::new)
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    boolean[] failoverOccurred = new boolean[1];
    List<Employee> actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .map(Employee::new)
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertTrue(failoverOccurred[0]);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void recordStreamMinRetryTest() {
    String expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .min((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
        .get()
        .get(NAME)
        .get();

    boolean[] failoverOccurred = new boolean[1];
    String actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .min((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
        .get()
        .get(NAME)
        .get();

    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamMaxRetryTest() {
    String expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .max((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
        .get()
        .get(NAME)
        .get();

    boolean[] failoverOccurred = new boolean[1];
    String actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .max((record1, record2) -> Integer.compare(record1.get(NAME).get().length(), record2.get(NAME).get().length()))
        .get()
        .get(NAME)
        .get();

    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamCountRetryTest() {
    long expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    boolean[] failoverOccurred = new boolean[1];
    long actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamAnyMatchRetryTest() {
    boolean expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .anyMatch(record -> record.get(GENDER).get().equals('F'));

    boolean[] failoverOccurred = new boolean[1];
    boolean actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .anyMatch(record -> record.get(GENDER).get().equals('F'));


    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamAllMatchRetryTest() {
    boolean expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .allMatch(record -> record.get(GENDER).get().equals('M'));

    boolean[] failoverOccurred = new boolean[1];
    boolean actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .allMatch(record -> record.get(GENDER).get().equals('M'));


    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamNoneMatchRetryTest() {
    boolean expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .noneMatch(record -> record.get(GENDER).get().equals('F'));

    boolean[] failoverOccurred = new boolean[1];
    boolean actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .noneMatch(record -> record.get(GENDER).get().equals('F'));


    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamFindFirstRetryTest() {
    Optional<Record<Integer>> expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .filter(record -> record.get(GENDER).get().equals('F'))
        .findFirst();

    boolean[] failoverOccurred = new boolean[1];
    Optional<Record<Integer>> actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .filter(record -> record.get(GENDER).get().equals('F'))
        .findFirst();


    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamFindAnyRetryTest() {
    Optional<Record<Integer>> expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .filter(record -> record.get(GENDER).get().equals('F'))
        .findAny();

    boolean[] failoverOccurred = new boolean[1];
    Optional<Record<Integer>> actual = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .filter(record -> record.get(GENDER).get().equals('F'))
        .findAny();


    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void recordStreamWithNonPortablePeekRetryTest() {
    long expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    try {
      boolean[] failoverOccurred = new boolean[1];
      long count = employeeReader.records().batch(1)
          .filter(Employee.GENDER.valueOr('X').is('M'))
          .peek(t -> {})
          .filter(rec -> {
            if (!failoverOccurred[0]) {
              shutdownActive();
              failoverOccurred[0] = true;
            }

            return true;
          })
          .filter(rec -> rec.get(SSN).get() % 2 == 0)
          .count();

      //In a multi-stripe single failover setup we might failover the stripe that is not accessed first.
      //In that case the test can pass, but then we should check the results.
      assertThat(testSetup, is(MULTI_STRIPE_SINGLE_ACTIVE_FAILOVER));
      assertThat(failoverOccurred[0], is(true));
      assertThat(count, is(expected));
    } catch (StoreStreamTerminatedException e) {
      String msg = "Stream execution was prematurely terminated due to an active server failover";
      assertThat(e.getMessage(), containsString(msg));
    }
  }

  @Test
  public void recordStreamWithLogPeekInNonPortableRetryTest() {
    long expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    try {
      boolean[] failoverOccurred = new boolean[1];
      long count = employeeReader.records().batch(1)
          .filter(Employee.GENDER.valueOr('X').is('M'))
          .filter(rec -> {
            if (!failoverOccurred[0]) {
              shutdownActive();
              failoverOccurred[0] = true;
            }

            return true;
          })
          .peek(RecordStream.log("Name = {}", NAME.valueOrFail()))
          .filter(rec -> rec.get(SSN).get() % 2 == 0)
          .count();

      //In a multi-stripe single failover setup we might failover the stripe that is not accessed first.
      //In that case the test can pass, but then we should check the results.
      assertThat(testSetup, is(MULTI_STRIPE_SINGLE_ACTIVE_FAILOVER));
      assertThat(failoverOccurred[0], is(true));
      assertThat(count, is(expected));
    } catch (StoreStreamTerminatedException e) {
      String msg = "Stream execution was prematurely terminated due to an active server failover";
      assertThat(e.getMessage(), containsString(msg));
    }
  }

  @Test
  public void recordStreamWithLogPeekInPortableRetryTest() {
    long expected = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    boolean[] failoverOccurred = new boolean[1];
    long actual = employeeReader.records().batch(1)
        .peek(RecordStream.log("Name = {}", NAME.valueOrFail()))
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred[0]) {
            shutdownActive();
            failoverOccurred[0] = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    assertTrue(failoverOccurred[0]);
    assertThat(actual, equalTo(expected));
  }
}
