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

import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.TestDataUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.SSN;
import static com.terracottatech.store.common.test.Employee.TELEPHONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class RetryableStreamFailoverIT extends HABase {

  List<Employee> employeeList;
  private static boolean failoverOccurred;

  public RetryableStreamFailoverIT() {
    super(ConfigType.OFFHEAP_ONLY);
  }

  @Before
  public void initializeMapAndDataset() {

    employeeList = TestDataUtil.getEmployeeList();

    // Fill in data to employee Dataset using the employeeList
    employeeList.stream()
        .forEach(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));

    failoverOccurred = false;
  }

  @Test
  public void recordStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred && rec.get(SSN).orElse(24) % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void recordToReferenceStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .map(rec -> rec.get(SSN).get())
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .filter(rec -> {
          if (!failoverOccurred && rec.get(SSN).orElse(24) % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(rec -> rec.get(SSN).get() % 2 == 0)
        .map(rec -> rec.get(SSN).get())
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void recordToIntStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToInt(rec -> rec.get(SSN).orElse(100))
        .filter(ssn -> ssn % 2 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToInt(rec -> rec.get(SSN).orElse(100))
        .filter(ssn -> {
          if (!failoverOccurred && ssn % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(ssn -> ssn % 2 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void recordToLongStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToLong(rec -> rec.get(TELEPHONE).orElse(100L))
        .filter(telephone -> telephone % 2 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToLong(rec -> rec.get(TELEPHONE).orElse(100L))
        .filter(telephone -> {
          if (!failoverOccurred && telephone % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(telephone -> telephone % 2 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void recordToDoubleStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToDouble(rec -> rec.get(SALARY).orElse(100D))
        .filter(salary -> salary % 1 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToDouble(rec -> rec.get(SALARY).orElse(100D))
        .filter(salary -> {
          if (!failoverOccurred && salary % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(salary -> salary % 1 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void referenceStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .map(Employee.SSN.valueOrFail())
        .filter(ssn -> ssn % 2 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .map(Employee.SSN.valueOrFail())
        .filter(ssn -> {
          if (!failoverOccurred && ssn % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(ssn -> ssn % 2 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void intStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToInt(Employee.SSN.intValueOrFail())
        .filter(ssn -> ssn % 2 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToInt(Employee.SSN.intValueOrFail())
        .filter(ssn -> {
          if (!failoverOccurred && ssn % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(ssn -> ssn % 2 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void longStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToLong(Employee.TELEPHONE.longValueOrFail())
        .filter(telephone -> telephone % 2 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToLong(Employee.TELEPHONE.longValueOrFail())
        .filter(telephone -> {
          if (!failoverOccurred && telephone % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(telephone -> telephone % 2 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }

  @Test
  public void doubleStreamRetryTest() {
    long countWithoutFailover = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToDouble(Employee.SALARY.doubleValueOrFail())
        .filter(salary -> salary % 1 == 0)
        .count();

    long count = employeeReader.records().batch(1)
        .filter(Employee.GENDER.valueOr('X').is('M'))
        .mapToDouble(Employee.SALARY.doubleValueOrFail())
        .filter(salary -> {
          if (!failoverOccurred && salary % 10 == 0) {
            shutdownActiveAndWaitForPassiveToBecomeActive();
            failoverOccurred = true;
          }

          return true;
        })
        .filter(salary -> salary % 1 == 0)
        .count();

    assertTrue(failoverOccurred);
    assertThat(count, equalTo(countWithoutFailover));
  }
}
