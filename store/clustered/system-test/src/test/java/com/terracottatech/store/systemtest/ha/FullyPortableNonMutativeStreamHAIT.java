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
import com.terracottatech.store.Tuple;
import com.terracottatech.store.common.test.Employees;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.terracottatech.store.common.test.Employee.CELL_NUMBER;
import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.STREET_ADDRESS;
import static com.terracottatech.store.common.test.Employees.addNEmployeeRecords;
import static com.terracottatech.store.common.test.Employees.cellNumber;
import static com.terracottatech.store.common.test.Employees.city;
import static com.terracottatech.store.common.test.Employees.country;
import static com.terracottatech.store.common.test.Employees.name;
import static com.terracottatech.store.common.test.Employees.street;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class FullyPortableNonMutativeStreamHAIT extends HABase {

  int numRecords = 10_000;

  public FullyPortableNonMutativeStreamHAIT() {
    super(ConfigType.OFFHEAP_ONLY);
  }

  @Before
  public void loadData() {

    try {
      addNEmployeeRecords(employeeWriterReader, numRecords);
    } catch (StoreRuntimeException e) {
      System.out.println("*****************Inserted " + Employees.generateUniqueEmpID() + " records.");
    }
  }

  @Test
  public void test() throws Exception {
    Callable<Tuple<Long, Long>> task = () -> {
      long startTime = System.nanoTime();
      long count = employeeReader.records()
          .filter(NAME.value().is(name))
          .filter(CELL_NUMBER.value().is(cellNumber))
          .filter(STREET_ADDRESS.value().is(street))
          .filter(CITY_ADDRESS.value().is(city))
          .filter(COUNTRY_ADDRESS.value().is(country))
          .count();
      return Tuple.of(count, System.nanoTime() - startTime);
    };

    ExecutorService executor = Executors.newFixedThreadPool(100);
    try {
      List<Future<Tuple<Long, Long>>> futures = Collections.nCopies(100, task).stream().map(t -> {
        try {
          return executor.submit(t);
        } catch (Throwable e) {
          throw new AssertionError(e);
        }

      }).collect(Collectors.toList());
      Thread.sleep(100);
      shutdownActive();

      // A passive to become new active should take at least 5sec (election time)
      List<Tuple<Long, Long>> results = futures.stream().map(f -> {
        try {
          return f.get();
        } catch (Throwable t) {
          throw new AssertionError(t);
        }
      }).sorted(comparing(Tuple<Long, Long>::getSecond).reversed()).collect(Collectors.toList());

      assertThat(results.get(0).getSecond(), greaterThan(SECONDS.toNanos(5)));
      assertThat(results.stream().map(Tuple::getFirst).collect(Collectors.toList()), everyItem(is((long) numRecords)));
    } finally {
      executor.shutdown();
    }
  }
}
