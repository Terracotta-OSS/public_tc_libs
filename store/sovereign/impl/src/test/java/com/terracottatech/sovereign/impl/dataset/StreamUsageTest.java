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
package com.terracottatech.sovereign.impl.dataset;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.store.Record;
import com.terracottatech.store.intrinsics.impl.CellExtractor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.testsupport.EmployeeSchema.LAST_STATE_IN_CSV;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.MAX_EMPLOYEE_RECORDS;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.age;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idx;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxFirst;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxLast;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRange1000to5000;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRange100to500;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRange2500;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRange5000;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeFirst;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeFirst10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeLast;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeLast10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.loadData;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.photo;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxFirst;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxLastMinus10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxLastMinus5;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRange1000to5000;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRange100to500;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRange5000;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRangeFirst;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRangeFirst10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRangeLast;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRangeLast10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.salary;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.state;

/**
 * Test class that tests various kinds of stream usage with "real life" data from a csv file
 * that has several cell definitions.
 *
 * This tests are run in two modes;
 *     mode 1: Mock the record, data container and locator and index map
 *     mode 2: Using the real SovereignDataset API and real index
 *
 * @author RKAV
 */
public abstract class StreamUsageTest {

  // load input data only once
  protected static List<Map<String, Object>> loadedData;

  public StreamUsageTest() {
  }

  @BeforeClass
  public static void setUpClass() throws IOException {
    loadedData = loadData();
  }

  @AfterClass
  public static void tearDownClass() {
  }

  protected abstract Stream<Record<Integer>> getRecordStream();
  protected abstract void verifyData(int times);

  /**
   * Test using tc-store cell functions. This test is failing and currently ignored.
   *
   * TODO: discuss and add support for byte array comparisons in tc-store api
   */
  @Ignore("TAB-5955")
  @Test
  public void testByteArrayEquals() {
    byte[] t = {99, 88, 77};
    Predicate<Record<?>> lt = photo.value().is(t);
    long count = getRecordStream().filter(lt).filter(idxFirst).count();
    Assert.assertEquals(1L, count);
  }

  /**
   * Test byte array cell comparison using normal filters using record.get after an
   * index cell
   */
  @Test
  public void testByteArrayEqualsWithGet() {
    byte[] t = {99, 88, 77};
    long count = getRecordStream()
        .filter(idxFirst)
        .filter((r) -> r.get(photo).isPresent() && Arrays.equals(r.get(photo).get(), t))
        .count();
    Assert.assertEquals(1L, count);
  }

  /**
   * Test integer cell comparisons using tc-store cell functions on an indexed range
   */
  @Test
  public void testMiddleAgedInRangeWithCellFunction() {
    Predicate<Record<?>> ageGt50 = age.value().isGreaterThan(50);
    long count = getRecordStream().filter(ageGt50.and(idxRange100to500)).count();
    Assert.assertEquals(157, count);
    verifyData(400);
  }

  /**
   * Test integer cell comparisons using tc-store cell functions on an indexed range
   * Test negation. Find number of folks with age <= 50 and empId > 4999 </=>
   */
  @Test
  public void testMiddleAgedInRangeWithTransparentFilterAndNegate() {
    Predicate<Record<?>> ageGt50 = age.value().isGreaterThan(50);
    final Predicate<Record<?>> idxRange5000 = idx.value().isLessThanOrEqualTo(4999);
    final Predicate<Record<?>> idxRange5000Negate = idx.value().isGreaterThan(4999).negate();
    long count1 = getRecordStream().filter((ageGt50.and(idxRange5000))).count();
    verifyData(5000);
    long count2 = getRecordStream().filter((ageGt50.and(idxRange5000Negate))).count();
    Assert.assertEquals(count1, count2);
    verifyData(10000);
  }

  /**
   * Test age range with mixed cell functions and get (with and without index selection)
   */
  @Test
  public void testMiddleAgedInRangeWithPartialCellFunction() {
    final AtomicBoolean filterCheckPass = new AtomicBoolean(false);
    long count1 = getRecordStream()
        .filter(idxRangeFirst)
        .filter((r) -> {
          if (r.get(idx).get() > 5000) {
            filterCheckPass.set(true);
          }
          return r.get(age).get() > 50;
        })
        .filter(idxRange5000)
        .count();
    verifyData(MAX_EMPLOYEE_RECORDS);

    if (!filterCheckPass.get()) {
      Assert.fail("Missing records in the filter pipeline");
    }

    long count2 = getRecordStream()
        .filter((r) -> r.get(age).get() > 50)
        .filter(idxRange5000)
        .filter(idxRangeFirst)
        .count();
    verifyData(2 * MAX_EMPLOYEE_RECORDS);

    filterCheckPass.set(true);
    long count3 = getRecordStream()
        .filter(idxRange5000)
        .filter(idxRangeFirst)
        .filter((r) -> {
          if (r.get(idx).get() > 5000) {
            filterCheckPass.set(false);
          }
          return r.get(age).get() > 50;
        })
        .count();
    verifyData(2 * MAX_EMPLOYEE_RECORDS + 5001);

    if (!filterCheckPass.get()) {
      Assert.fail("Unexpected records in the filter pipeline");
    }

    Assert.assertEquals(1849, count1);
    Assert.assertEquals(count1, count2);
    Assert.assertEquals(count3, count2);
  }

  /**
   * Test age range with no cell functions and no index selection
   */
  @Test
  public void testMiddleAgedInRangeWithoutCellFunction() {
    final AtomicInteger recordCount = new AtomicInteger(0);
    long count1 = getRecordStream()
        .filter(recordIdxRangeFirst)
        .filter((r) -> {
          recordCount.getAndIncrement();
          return r.get(age).get() > 50;
        })
        .filter(recordIdxRange5000)
        .count();

    verifyData(MAX_EMPLOYEE_RECORDS);
    Assert.assertEquals("Missing records in the filter pipeline", MAX_EMPLOYEE_RECORDS, recordCount.get());

    long count2 = getRecordStream()
        .filter((r) -> r.get(age).get() > 50)
        .filter(recordIdxRange5000)
        .filter(recordIdxRangeFirst)
        .count();
    verifyData(2 * MAX_EMPLOYEE_RECORDS);

    long count3 = getRecordStream()
        .filter(recordIdxRange5000)
        .filter(recordIdxRangeFirst)
        .filter((r) -> r.get(age).get() > 50)
        .count();
    verifyData(3*MAX_EMPLOYEE_RECORDS);

    Assert.assertEquals(1849, count1);
    Assert.assertEquals(count1, count2);
    Assert.assertEquals(count3, count2);
  }

  /**
   * Test the limit with and salary
   */
  @Test
  public void testLimitForAgeAndSalary() {
    Predicate<Record<?>> salaryLte500000 = salary.value().isLessThanOrEqualTo(500000.0);
    final AtomicBoolean filterCheckPass = new AtomicBoolean(true);
    long count = getRecordStream()
        .filter(idxRange1000to5000)
        .limit(1000)
        .filter((r) -> {
          if (r.get(idx).get() > 2010) {
            filterCheckPass.set(false);
          }
          return r.get(age).get() > 30;
        })
        .filter(idxRange2500)
        .filter(salaryLte500000)
        .count();

    verifyData(1000);
    Assert.assertEquals(156, count);

    if (!filterCheckPass.get()) {
      Assert.fail("Unexpected records in the filter pipeline");
    }
  }

  @Test
  public void testAllMatchForSalaryForYouth() {
    Predicate<Record<?>> salaryGt100000 = salary.value().isGreaterThan(100000.0);
    final AtomicBoolean filterCheckPass =  new AtomicBoolean(false);
    boolean allShouldMatch = getRecordStream()
        .filter(idxRangeLast)
        .peek((d) -> {
          if (d.get(idx).get() < MAX_EMPLOYEE_RECORDS - 10) {
            filterCheckPass.set(true);
          }
        })
        .filter(idxRangeLast10)
        .filter((r) -> r.get(age).get() < 30)
        .filter(salaryGt100000)
        .mapToDouble((r) -> (r.get(salary).get()))
        .allMatch((r) -> r > 100000.0);

    Assert.assertEquals(allShouldMatch, true);
    if (!filterCheckPass.get()) {
      Assert.fail("Missing records in the filter pipeline");
    }

    filterCheckPass.set(true);
    boolean noneShouldMatch = getRecordStream()
        .filter((r) -> r.get(age).get() < 30)
        .filter(salaryGt100000)
        .peek((d) -> {
          if (d.get(salary).get() <= 100000.0) {
            filterCheckPass.set(false);
          }})
        .filter(recordIdxRangeLast10)
        .mapToDouble((r) -> (r.get(salary).get()))
        .peek((d) -> {
          if (d <= 100000.0) {
            filterCheckPass.set(false);
          }})
        .noneMatch((r) -> r <= 100000.0);

    Assert.assertEquals(noneShouldMatch, true);
    if (!filterCheckPass.get()) {
      Assert.fail("Wrong records in the filter pipeline");
    }
  }

  /**
   * Test the limit with and salary
   */
  @Test
  public void testSkipAndLimitForAll() {
    for (int j = 0; j < 10; j++) {
      long count = getRecordStream()
          .filter(idxRangeFirst10)
          .skip(j)
          .limit(10-j)
          .filter(recordIdxRangeFirst10)
          .count();
      Assert.assertEquals(10-j, count);
    }
  }

  /**
   * Test sorted with idx
   */

  @Test
  public void testSortedIdx() {
    final AtomicInteger recordCount = new AtomicInteger(0);
    for (int j = 0; j < 10; j++) {
      recordCount.set(0);
      OptionalInt first = getRecordStream()
          .filter(idxRangeLast10)
          .filter(idxRangeLast)
          .peek((r) -> recordCount.incrementAndGet())
          .mapToInt((d) -> d.get(idx).get())
          .sorted()
          .skip(j)
          .limit(1000 - j)
          .findFirst();
      Assert.assertEquals(MAX_EMPLOYEE_RECORDS - 10 + j, first.getAsInt());
      Assert.assertEquals(10, recordCount.get());
      verifyData(10 * (j + 1));
    }
  }

  /**
   * Test getting the last 10 employee ids with a complex set of comparison style
   * predicates (created when using tc store api, that are mixed with
   * normal user created predicates.
   */
  @Test
  public void testSortedIdxWithUnknowns() {
    final AtomicInteger recordCount = new AtomicInteger(0);
    OptionalInt first = getRecordStream()
        .filter(idxRangeLast)
        .filter(idxRangeFirst)
        .filter(idxRangeLast10.and(recordIdxRangeLast10.and(recordIdxLastMinus10.or(recordIdxLastMinus5).or(idxLast))))
        .peek((r) -> recordCount.incrementAndGet())
        .mapToInt((d) -> d.get(idx).get())
        .sorted()
        .findFirst();
    Assert.assertEquals(MAX_EMPLOYEE_RECORDS - 10, first.getAsInt());
    Assert.assertEquals(3, recordCount.get());
    verifyData(MAX_EMPLOYEE_RECORDS);
  }

  /**
   * Test getting the last 10 employee ids with a complex set of comparison style
   * predicates (created when using tc store api which are mixed
   * with unknown predicates and ensure that unknown predicates does not loose
   * records in the pipeline.
   */
  @Test
  public void testLastFewEmployeeIdsWithMixedPredicates() {
    final AtomicInteger recordCount = new AtomicInteger(0);
    OptionalInt first = getRecordStream()
        .filter(idxRangeLast.and((r) -> { recordCount.incrementAndGet(); return true; }))
        .filter(idxRangeLast10.and(recordIdxRangeLast10.and(recordIdxLastMinus10.or(recordIdxLastMinus5).or(idxLast))))
        .peek((r) -> recordCount.incrementAndGet())
        .mapToInt((d) -> d.get(idx).get())
        .sorted()
        .findFirst();
    Assert.assertEquals(MAX_EMPLOYEE_RECORDS - 10, first.getAsInt());
    Assert.assertEquals(MAX_EMPLOYEE_RECORDS + 3, recordCount.get());
    verifyData(MAX_EMPLOYEE_RECORDS);
  }

  /**
   * Test sorted states
   */
  @Test
  public void testReverseSortedState() {
    Optional<String> first = getRecordStream()
        .filter(recordIdxRange100to500.or(recordIdxRange1000to5000).or(recordIdxFirst).or(recordIdxRangeFirst.and(recordIdxRangeLast)))
        .filter((r) -> r.get(state).isPresent())
        .map((r) -> r.get(state).get())
        .distinct()
        .sorted(Comparator.<String>reverseOrder())
        .findFirst();
    Assert.assertEquals(LAST_STATE_IN_CSV, first.get());
  }

  @Test
  public void testMixedConjunctionAndDisjunctionForEmployeeId() {
    final Predicate<Record<?>> a = CellExtractor.extractComparable(idx).isGreaterThanOrEqualTo(10);
    final Predicate<Record<?>> b = CellExtractor.extractComparable(idx).isLessThan(500);
    final Predicate<Record<?>> c = CellExtractor.extractComparable(idx).isLessThan(503).negate();
    final Predicate<Record<?>> d = CellExtractor.extractComparable(idx).isLessThanOrEqualTo(MAX_EMPLOYEE_RECORDS);

    final Predicate<Record<?>> e = CellExtractor.extractComparable(idx).is(501);
    final Predicate<Record<?>> f = CellExtractor.extractComparable(idx).is(502);

    final long count = getRecordStream().filter((a.and(b.or(c.and(d)))).or(e.or(f))).count();
    verifyData(MAX_EMPLOYEE_RECORDS - 8);

    final long count1 = getRecordStream().filter((a.and(b)).or(c.and(d)).or(e.or(f))).count();
    verifyData((MAX_EMPLOYEE_RECORDS - 8)*2);
    // all except id 0 to 9 must be detected.. 10-9999 must pass, except 500..so count is
    // MAX_EMPLOYEE_RECORDS - 11..
    Assert.assertEquals(MAX_EMPLOYEE_RECORDS-11, count);
    Assert.assertEquals(MAX_EMPLOYEE_RECORDS-11, count1);
  }

  @Test
  public void testMixedSubRangesForEmployeeId() {
    final Predicate<Record<?>> a = CellExtractor.extractComparable(idx).isGreaterThanOrEqualTo(9000);
    final Predicate<Record<?>> b = CellExtractor.extractComparable(idx).isLessThan(1000);
    final Predicate<Record<?>> c = CellExtractor.extractComparable(idx).isLessThan(500);
    final Predicate<Record<?>> d = CellExtractor.extractComparable(idx).isLessThanOrEqualTo(400).negate();
    final Predicate<Record<?>> e = CellExtractor.extractComparable(idx).isGreaterThanOrEqualTo(9500);
    final Predicate<Record<?>> f = CellExtractor.extractComparable(idx).isLessThan(10000);

    final long count = getRecordStream().filter((a.or(b)).and(c.and(d)).or(e.and(f))).count();
    verifyData(600);
    // actual subrange due to and should be between 400 to 500 (both exclusive)
    // or between 9500 and 10000 (with 9500 inclusive)
    Assert.assertEquals(599, count);
  }
}
