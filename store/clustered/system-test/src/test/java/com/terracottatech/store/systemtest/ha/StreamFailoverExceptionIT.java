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

import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.StoreStreamTerminatedException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.TestDataUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.terracottatech.store.common.test.Employee.BONUS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

public class StreamFailoverExceptionIT extends HABase {

  List<Employee> employeeList;

  public StreamFailoverExceptionIT() {
    super(ConfigType.OFFHEAP_ONLY);
  }

  @Before
  public void initializeMapAndDataset() {

    employeeList = TestDataUtil.getEmployeeList();

    // Fill in data to employee Dataset using the employeeList
    employeeList.stream()
        .forEach(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));
  }

  @Test
  public void nonMutativeStatelessPortableOpTest() {
    try {
      employeeReader.records().batch(1)
          .filter(Employee.SSN.intValueOr(0).isGreaterThan(0))
          .filter(rec -> {
            if (rec.get(Employee.SSN).orElse(24) % 10 == 0) {
              shutdownActiveAndWaitForPassiveToBecomeActive();
            }

            return true;
          })
          .forEach(t -> {});

      fail("Exception was expected to be throw with stream execution terminating in between due to active failover");
    } catch(StoreStreamTerminatedException e) {
      assertStreamFailureOnFailover(e);
    }
  }

  @Test
  public void nonMutativeWithStatefulPortableOpTest() {
    try {
      employeeReader.records().batch(1)
          .skip(2)
          .filter(rec -> {
            if (rec.get(Employee.SSN).orElse(24) % 10 == 0) {
              shutdownActiveAndWaitForPassiveToBecomeActive();
            }

            return true;
          })
          .forEach(t -> {});
      fail("Exception was expected to be throw with stream execution terminating in between due to active failover");
    } catch(StoreStreamTerminatedException e) {
      assertStreamFailureOnFailover(e);
    }
  }

  @Test
  public void mutativeOpInNonPortablePortionStatelessOnlyInPortableTest() {
    try {
      employeeWriterReader.records()
          .filter(Employee.SSN.intValueOr(0).isGreaterThan(0))
          .filter(rec -> {
            if (rec.get(Employee.SSN).orElse(24) % 10 == 0) {
              shutdownActiveAndWaitForPassiveToBecomeActive();
            }

            return true;
          })
          .mutate(UpdateOperation.write(BONUS).doubleResultOf(BONUS.doubleValueOr(100).increment()));
      fail("Exception was expected to be throw with stream execution terminating in between due to active failover");
    } catch(StoreStreamTerminatedException e) {
      assertStreamFailureOnFailover(e);
    }
  }

  @Test
  public void mutativeOpInNonPortablePortionStatefulInPortableTest() {
    try {
      employeeWriterReader.records()
          .skip(2)
          .filter(rec -> {
            if (rec.get(Employee.SSN).orElse(24) % 10 == 0) {
              shutdownActiveAndWaitForPassiveToBecomeActive();
            }

            return true;
          })
          .deleteThen()
          .count();
      fail("Exception was expected to be throw with stream execution terminating in between due to active failover");
    } catch(StoreStreamTerminatedException e) {
      assertStreamFailureOnFailover(e);
    }
  }

  @Test
  public void mutativeOpInPortablePortionStatelessOnlyInPortableTest() {
    try {
      employeeWriterReader.records().batch(1)
          .filter(Employee.SSN.intValueOr(0).isGreaterThan(0))
          .mutateThen(UpdateOperation.write(BONUS).doubleResultOf(BONUS.doubleValueOr(100).increment()))
          .map(Tuple::getSecond)
          .filter(rec -> {
            if (rec.get(Employee.SSN).orElse(24) % 10 == 0) {
              shutdownActiveAndWaitForPassiveToBecomeActive();
            }

            return true;
          })
          .count();
      fail("Exception was expected to be throw with stream execution terminating in between due to active failover");
    } catch(StoreStreamTerminatedException e) {
      assertStreamFailureOnFailover(e);
    }
  }

  @Test
  public void mutativeOpInPortablePortionStatefulInPortableTest() {
    try {
      employeeWriterReader.records().batch(1)
          .mutateThen(UpdateOperation.write(BONUS).doubleResultOf(BONUS.doubleValueOr(100).increment()))
          .map(Tuple::getSecond)
          .filter(rec -> {
            if (rec.get(Employee.SSN).orElse(24) % 10 == 0) {
              shutdownActiveAndWaitForPassiveToBecomeActive();
            }

            return true;
          })
          .count();
      fail("Exception was expected to be throw with stream execution terminating in between due to active failover");
    } catch(StoreStreamTerminatedException e) {
      assertStreamFailureOnFailover(e);
    }
  }

  private static void assertStreamFailureOnFailover(StoreRuntimeException e) {
    String msg = "Stream execution was prematurely terminated due to an active server failover";
    assertThat(e.getMessage(), containsString(msg));
  }
}
