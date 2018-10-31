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
package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.dataset.SpliteratorUtility;
import com.terracottatech.sovereign.impl.dataset.StreamUsageTest;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.sovereign.impl.SovereignDataSetConfig.StorageType.OFFHEAP;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.MAX_EMPLOYEE_RECORDS;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.age;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.country;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.employeeCellDefinitionMap;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idx;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRange9990to9999;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeFirst;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeFirst10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeLast;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idxRangeLast10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRange1000to5000;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRange9990to9999;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRangeFirst10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.recordIdxRangeLast10;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.salary;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.state;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.weight;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.alterRecord;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.assign;
import static com.terracottatech.sovereign.testsupport.TestUtility.formatNanos;
import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.groupingBy;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test for stream usage at the sovereign API level with a real {@code Dataset}.
 * Reuses the abstract {@link StreamUsageTest} for running all the stream usage tests on
 * a real {@link SovereignDataset}.
 *
 * In addition to normal non-mutative Stream usage tests (which are mainly filtering), this
 * test class adds a bunch of mutative and concurrency tests to the midst.
 *
 * @author RKAV
 */
public class SovereignDatasetStreamUsageTest extends StreamUsageTest {
  private SovereignDataset<Integer> employeeDataset;

  public SovereignDatasetStreamUsageTest() {
  }

  @Before
  public void setUp() throws Exception {
    final SovereignDataSetConfig<Integer, FixedTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, FixedTimeReference.class)
        .resourceSize(OFFHEAP, 128 * 1024 * 1024)
        .versionLimit(4);
    this.employeeDataset = new SovereignBuilder<>(config).build();
    final SovereignIndexing indexing = employeeDataset.getIndexing();
    indexing.createIndex(idx, SovereignIndexSettings.btree()).call();
    fillDatasetWithEmployees(loadedData);
  }

  @After
  public void tearDown() throws Exception {
    if (this.employeeDataset != null) {
      employeeDataset.getStorage().destroyDataSet(employeeDataset.getUUID());
      this.employeeDataset = null;
    }
  }

  /**
   * return a real stream returned by the data set api
   *
   * @return a stream of records
   */
  @Override
  protected Stream<Record<Integer>> getRecordStream() {
    return employeeDataset.records();
  }

  /**
   * Since this test class uses a "real" dataset, mock verification of data
   * container iteration is not possible. So this is just a no-op
   *
   * @param times number of times the data container is supposed to be iterated
   *              based on the test case.
   */
  @Override
  protected void verifyData(int times) {
  }

  /**
   * Additional mutative and concurrency tests, that can only be run on a real dataset.
   */

  /**
   * Test Group by with index selection and transparent filters.
   *
   * i.e compute average salary by country for only the last 10 entries
   *
   * TODO: Add assertions using explain to assert for index selection.
   *
   */
  @Test
  public void testGroupingWithTransparentFilters() {
    try (final RecordStream<Integer> employeeStream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(employeeDataset, employeeStream, assignedIndexCell, null);
      Map<String, Double> averageSalaryByCountryInLast10Range = employeeStream
          .filter(idxRangeFirst.and(idxRangeLast)) // full range
          .sorted(country.valueOrFail().asComparator())
          .filter(idxRange9990to9999)
          .collect(groupingBy(country.valueOr(""),
              averagingDouble(salary.doubleValueOr(Double.NaN))));

      Assert.assertEquals(((827882.0d + 698775.0) / 2.0d), averageSalaryByCountryInLast10Range.get("United States"), 0.01d);
      assertThat(assignedIndexCell.get(), is(idx));
    }
  }

  /**
   * Test Group by without index select (opaque filters).
   *
   * i.e compute average weight by country and state for ID range 100-500
   *
   */
  @Test
  public void testGroupingWithOpaqueFilters() {
    try (final RecordStream<Integer> employeeStream = getTestStream()) {
      final AtomicInteger recordCount = new AtomicInteger(0);
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(employeeDataset, employeeStream, assignedIndexCell, null);
      Map<String, Map<String, Map<Integer, Double>>> employeeWeightByCountryStateAndAge = employeeStream
          .peek((r) -> recordCount.incrementAndGet())
          .filter(recordIdxRange1000to5000.and(r -> r.get(weight).isPresent()))
          .sorted(Comparator.comparing(r -> r.get(weight).get(),
              (w1, w2) -> Double.compare(Math.abs(w1), Math.abs(w2))))
          .collect(
              groupingBy(country.valueOr(""),
                  groupingBy(state.valueOr(""),
                      groupingBy(age.valueOr(0),
                          averagingDouble(weight.doubleValueOrFail())))));


      Assert.assertEquals(MAX_EMPLOYEE_RECORDS, recordCount.get()); // no index selection so all records are scanned

      // assert some expected values from the grouping map
      Assert.assertEquals(124.3, employeeWeightByCountryStateAndAge.get("United States")
          .get("Colorado")
          .get(32), 0.0001d);
      Assert.assertEquals(61.25, employeeWeightByCountryStateAndAge.get("Vatican City").get("").get(45), 0.0001d);
      assertThat(assignedIndexCell.get(), is(nullValue()));
    }
  }

  /**
   * Test Mutation with opaque filters (i.e no index selection) with direct mutation
   *
   * Mutate the Last record, by changing the salary
   */
  @Test
  public void testDirectMutationWithOpaqueFilter() {
    long beforeMutationCount = employeeDataset.records()
        .filter(recordIdxRangeLast10)
        .count();

    Integer lastEmpId = employeeDataset.get(MAX_EMPLOYEE_RECORDS - 1).get(idx).get();
    Double newSalary = 100000.0d;

    employeeDataset.applyMutation(IMMEDIATE, MAX_EMPLOYEE_RECORDS - 1, r -> true, alterRecord(assign(salary, newSalary)));

    long afterMutationCount = employeeDataset.records()
        .filter(recordIdxRangeLast10)
        .count();

    Assert.assertEquals(beforeMutationCount, afterMutationCount);
    Assert.assertEquals(newSalary, employeeDataset.get(MAX_EMPLOYEE_RECORDS - 1).get(salary).get());
    Assert.assertEquals(lastEmpId, employeeDataset.get(MAX_EMPLOYEE_RECORDS - 1).get(idx).get());
  }

  /**
   * Test Mutation with opaque filters (i.e no index selection) with stream mutation
   *
   * Mutate the salaries of folks in United States and having last 10 employee Ids.
   */
  @Test
  public void testStreamMutationWithOpaqueFilter() {
    long beforeMutationCount = employeeDataset.records()
        .filter(recordIdxRangeFirst10)
        .count();

    Double newSalary = 10000000.0d;

    List<Integer> luckyEmployees = new ArrayList<>();
    employeeDataset.records()
        .filter(recordIdxRangeFirst10)
        .filter((r) -> r.get(country).get().equals("United States"))
        .peek((r) -> luckyEmployees.add(r.getKey()))
        .forEach(employeeDataset.applyMutation(IMMEDIATE, alterRecord(assign(salary, newSalary))));

    long afterMutationCount = employeeDataset.records()
        .filter(recordIdxRangeFirst10)
        .count();

    Assert.assertEquals(beforeMutationCount, afterMutationCount);
    Assert.assertEquals(2, luckyEmployees.size());
    for (Integer empId : luckyEmployees) {
      Assert.assertEquals(newSalary, employeeDataset.get(empId).get(salary).get());
      Assert.assertEquals(empId, employeeDataset.get(empId).get(idx).get());
    }
  }

  /**
   * Test Mutation with transparent filters (i.e with index selection) with direct mutation
   *
   * Mutate the Last record, by changing the salary.
   *
   */
  @Test
  public void testDirectMutationWithTransparentFilter() {
    long beforeMutationCount = employeeDataset.records()
        .filter(idxRangeLast10)
        .count();

    Integer lastEmpId = employeeDataset.get(MAX_EMPLOYEE_RECORDS - 1).get(idx).get();
    Double newSalary = 100000.0d;

    employeeDataset.applyMutation(IMMEDIATE, MAX_EMPLOYEE_RECORDS - 1, r -> true, alterRecord(assign(salary, newSalary)));

    long afterMutationCountWithOpaqueFilter = employeeDataset.records()
        .filter(recordIdxRangeLast10)
        .count();

    long afterMutationCountWithTransparentFilter = employeeDataset.records()
        .filter(idxRangeLast10)
        .count();

    Assert.assertEquals(newSalary, employeeDataset.get(MAX_EMPLOYEE_RECORDS - 1).get(salary).get());
    Assert.assertEquals(lastEmpId, employeeDataset.get(MAX_EMPLOYEE_RECORDS - 1).get(idx).get());

    Assert.assertEquals(((afterMutationCountWithOpaqueFilter < beforeMutationCount) ?
            "Opaque filters not able to detect mutated records post mutation" :
            "Opaque filters seeing more records than expected post mutation"),
        beforeMutationCount, afterMutationCountWithOpaqueFilter);
    Assert.assertEquals(((afterMutationCountWithTransparentFilter < beforeMutationCount) ?
        "Transparent Filters Not able to detect mutated records post mutation" :
        "Transparent Filters seeing more records than expected post mutation"),
        beforeMutationCount, afterMutationCountWithTransparentFilter);
  }

  /**
   * Test Mutation with transparent filters (i.e with index selection) with stream mutation
   *
   * Mutate the salaries of folks in United States and having last 10 employee Ids.
   *
   */
  @Test
  public void testStreamMutationWithTransparentFilter() {
    long beforeMutationCount = employeeDataset.records()
        .filter(idxRangeFirst10)
        .count();

    Double newSalary = 10000000.0d;

    List<Integer> luckyEmployees = new ArrayList<>();
    employeeDataset.records()
        .filter(idxRangeFirst10)
        .filter((r) -> r.get(country).get().equals("United States"))
        .peek((r) -> luckyEmployees.add(r.getKey()))
        .forEach(employeeDataset.applyMutation(IMMEDIATE, alterRecord(assign(salary, newSalary))));

    long afterMutationCountWithTransparentFilter = employeeDataset.records()
        .filter(idxRangeFirst10)
        .count();

    long afterMutationCountWithOpaqueFilter = employeeDataset.records()
        .filter(recordIdxRangeFirst10)
        .count();

    Assert.assertEquals(((afterMutationCountWithOpaqueFilter < beforeMutationCount) ?
        "Opaque filters not able to detect mutated records post mutation" :
        "Opaque filters seeing more records than expected post mutation"),
        beforeMutationCount,
        afterMutationCountWithOpaqueFilter);
    Assert.assertEquals(((afterMutationCountWithTransparentFilter < beforeMutationCount) ?
        "Transparent filters (with index selection) not able to detect mutated records" :
        "Transparent filters (with index selection) seeing more records than expected post mutation"),
        beforeMutationCount, afterMutationCountWithTransparentFilter);

    Assert.assertEquals(2, luckyEmployees.size());
    for (Integer empId : luckyEmployees) {
      Assert.assertEquals(newSalary, employeeDataset.get(empId).get(salary).get());
      Assert.assertEquals(empId, employeeDataset.get(empId).get(idx).get());
    }
  }

  /**
   * Test high concurrent mutation on N different record ranges (no collisions) with a parallel query
   * Use a fork join pool mechanism to split the mutation over the record ranges into parallel ranges
   *
   * No reads injected and transparent filters (with index selection) is used.
   * (i.e {@code injectRead} is false and {@code useOpaque} is false.
   */
  @Test
  public void testHighConcurrencyMutation() {
    concurrentMutateSalary(10, 5000.0d, false, false);
  }

  /**
   * Test low concurrent mutation on N different record ranges (no collisions) with a parallel query
   * Use a fork join pool mechanism to split the mutation over the record ranges into parallel ranges
   *
   * No reads injected and transparent filters (with index selection) is used.
   * (i.e {@code injectRead} is false and {@code useOpaque} is false.
   */
  @Test
  public void testLowConcurrencyMutation() {
    concurrentMutateSalary(1000, 10000.0d, false, false);
  }

  /**
   * Test low concurrent mutation on N different record ranges (no collisions) with a parallel query
   * Use a fork join pool mechanism to split the mutation over the record ranges into parallel ranges
   */
  @Test
  public void testZeroConcurrencyMutation() {
    concurrentMutateSalary(10000, 15000.0d, false, false);
  }

  /**
   * Test high concurrent mutation on N different record ranges (no collisions) with a parallel query
   * Use a fork join pool mechanism to split the mutation over the record ranges into parallel ranges
   *
   * Do it with occasional reads and opaque filters (i.e no index selection)
   */
  @Test
  public void testHighConcurrencyMutationWithReads() {
    System.out.println("Starting testHighConcurrencyMutationWithReads");

    final Timer timer = new Timer(true);
//    timer.schedule(
//        new TimerTask() {
//          @Override
//          public void run() {
//            Diagnostics.threadDump(System.out);
//          }
//        },
//        TimeUnit.SECONDS.toMillis(45L));

    long duration = -1;
    try {
      final long startTime = System.nanoTime();

      concurrentMutateSalary(100, 5000.0d, true, true);

      duration = System.nanoTime() - startTime;
    } finally {
      timer.cancel();
    }

    System.out.format("Ended testHighConcurrencyMutationWithReads; duration = %s%n", formatNanos(duration));
  }

  /**
   * Test low concurrent mutation on N different record ranges (no collisions) with a parallel query
   * Use a fork join pool mechanism to split the mutation over the record ranges into parallel ranges
   */
  @Test
  public void testLowConcurrencyMutationWithReads() {
    concurrentMutateSalary(1000, 10000.0d, true, true);
  }

  /**
   * Test low concurrent mutation on N different record ranges (no collisions) with a parallel query
   * Use a fork join pool mechanism to split the mutation over the record ranges into parallel ranges
   */
  @Test
  public void testZeroConcurrencyMutationWithReads() {
    concurrentMutateSalary(10000, 15000.0d, true, true);
  }

  /*****************************************************************************************************
   * Classes private and used only by the "real" dataset stream usage test cases
   *****************************************************************************************************/

  private void concurrentMutateSalary(int thresholdPerTask, double increment, boolean injectReads, boolean useOpaque) {

    LongAdder totalSalaryPreMutate = new LongAdder();
    LongAdder totalSalaryPostMutate = new LongAdder();
    LongAdder totalAgePreMutate = new LongAdder();
    LongAdder totalAgePostMutate = new LongAdder();

    Consumer<Record<Integer>> preMutation = (injectReads) ? (r) -> {
      totalSalaryPreMutate.add(r.get(salary).orElse(0d).longValue());
      totalAgePreMutate.add(r.get(age).orElse(0).longValue());
    } : null;

    Consumer<Record<Integer>> mutation = (r) ->
        employeeDataset.applyMutation(IMMEDIATE, r.getKey(), r1 -> true, alterRecord(assign(salary, r.get(salary).get() + increment)));

    Consumer<Record<Integer>> postMutation = (injectReads) ? (r) -> {
      totalSalaryPostMutate.add(r.get(salary).orElse(0d).longValue());
      totalAgePostMutate.add(r.get(age).orElse(0).longValue());
    } : null;

    ParallelMutate mutationTask = new ParallelMutate(0, MAX_EMPLOYEE_RECORDS, preMutation, mutation, postMutation, thresholdPerTask, useOpaque);

    // create a fork join pool with a higher parallelism level
    ForkJoinPool pMutationPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 8);
    pMutationPool.invoke(mutationTask);

    if (injectReads) {
      Assert.assertEquals("Age must not change post mutation",
          totalAgePreMutate.sum(), totalAgePostMutate.sum());
      Assert.assertEquals("Total Salary increase not as expected post mutation",
          totalSalaryPreMutate.sum() + (long)increment * MAX_EMPLOYEE_RECORDS, totalSalaryPostMutate.sum());
    }

    Map<String, Double> averageSalaryByCountryInFirst10Range = employeeDataset.records()
        .filter(recordIdxRangeFirst10) // full range
        .sorted(country.valueOrFail().asComparator())
        .collect(groupingBy(country.valueOrFail(),
            averagingDouble(salary.doubleValueOr(Double.NaN))));

    Assert.assertEquals(((758670.0 + 1215203.0 + (increment * 2.0d)) / 2.0),
        averageSalaryByCountryInFirst10Range.get("United States"), 0.01d);

    Map<String, Double> averageSalaryByCountryInLast10Range = employeeDataset.records()
        .filter(recordIdxRange9990to9999) // full range
        .sorted(country.valueOrFail().asComparator())
        .collect(groupingBy(country.valueOrFail(),
            averagingDouble(salary.doubleValueOr(Double.NaN))));

    Assert.assertEquals(((827882.0d + 698775.0 + (increment * 2)) / 2.0d),
        averageSalaryByCountryInLast10Range.get("United States"), 0.01d);
  }

  private RecordStream<Integer> getTestStream() {
    return employeeDataset.records();
  }

  /**
   * fill the dataset with test data from the {@code loadedData} list. loadedData is
   * one time loaded from a csv file.
   *
   * @param loadedData data that is loaded from a csv file into this list as a list of maps.
   */
  @SuppressWarnings("unchecked")
  private void fillDatasetWithEmployees(List<Map<String, Object>> loadedData) {
    int recordIndex = 0;
    for (Map<String, Object> m : loadedData) {
      Cell<?>[] cells = new Cell<?>[m.size()+1];
      cells[0] = idx.newCell(recordIndex);
      int cellIndex = 1;
      for (String k : m.keySet()) {
        Object o = m.get(k);
        CellDefinition<?> cd = employeeCellDefinitionMap.get(k);
        if (cd != null && o != null) {
          if (o instanceof Integer) {
            Integer ii = (Integer)o;
            cells[cellIndex++] = ((CellDefinition<Integer>)cd).newCell(ii);
          } else if (o instanceof Long) {
            Long l = (Long)o;
            cells[cellIndex++] = ((CellDefinition<Long>)cd).newCell(l);
          } else if (o instanceof Double) {
            Double d = (Double)o;
            cells[cellIndex++] = ((CellDefinition<Double>)cd).newCell(d);
          } else if (o instanceof Boolean) {
            Boolean b = (Boolean)o;
            cells[cellIndex++] = ((CellDefinition<Boolean>)cd).newCell(b);
          } else if (o instanceof byte[]) {
            byte[] b = (byte[])o;
            cells[cellIndex++] = ((CellDefinition<byte[]>)cd).newCell(b);
          } else if (o instanceof String) {
            String s = (String)o;
            cells[cellIndex++] = ((CellDefinition<String>)cd).newCell(s);
          }
        }
      }
      employeeDataset.add(IMMEDIATE, recordIndex++, cells);
    }
  }

  private class ParallelMutate extends RecursiveAction {
    private static final long serialVersionUID = -2275271577216084702L;
    private final int startEmployeeId;
    private final int numEmployeesToMutate;
    private final int perTaskThreshold;
    private final Consumer<Record<Integer>> mutateAction;
    private final Consumer<Record<Integer>> preMutateAction;
    private final Consumer<Record<Integer>> postMutateAction;
    private final boolean useOpaque;

    public ParallelMutate(int startId, int numRecordsToMutate,
                          Consumer<Record<Integer>> preMutateAction,
                          Consumer<Record<Integer>> mutateAction,
                          Consumer<Record<Integer>> postMutateAction,
                          int perTaskThreshold,
                          boolean useOpaque) {
      this.startEmployeeId = startId;
      this.numEmployeesToMutate = numRecordsToMutate;
      this.mutateAction = mutateAction;
      this.perTaskThreshold = perTaskThreshold;
      this.preMutateAction = preMutateAction;
      this.postMutateAction = postMutateAction;
      this.useOpaque = useOpaque;
    }

    @Override
    protected void compute() {
      if (numEmployeesToMutate <= perTaskThreshold) {
        try {
          computeSerial();
        } catch (NullPointerException e) {
          synchronized (System.err) {
            System.err.format("[%s] Failed startEmployeeId=%d; numberOfemployeesToMutate=%d%n",
                Thread.currentThread().getName(), startEmployeeId, numEmployeesToMutate);
            e.printStackTrace(new InternalPrintStream(System.err, String.format("[%s] ", Thread.currentThread().getName())));
            System.err.flush();
          }
          throw new AssertionError(String.format("startEmployee=%s", startEmployeeId), e);
        }
        return;
      }
      int numInEachHalf = numEmployeesToMutate /2;
      invokeAll(new ParallelMutate(startEmployeeId, numInEachHalf, preMutateAction, mutateAction, postMutateAction, perTaskThreshold, useOpaque),
          new ParallelMutate(startEmployeeId + numInEachHalf, numEmployeesToMutate - numInEachHalf, preMutateAction, mutateAction, postMutateAction, perTaskThreshold, useOpaque));
    }

    private void computeSerial() {
      final int lastEmployeeId = startEmployeeId + numEmployeesToMutate - 1;
      final Predicate<Record<?>> employeeRange = (useOpaque) ?
          (r) -> (r.get(idx).get() >= startEmployeeId && r.get(idx).get() <= lastEmployeeId) :
          idx.value().isGreaterThanOrEqualTo(startEmployeeId).and(idx.value().isLessThanOrEqualTo(lastEmployeeId));

      if (preMutateAction != null) {
        employeeDataset.records().filter(employeeRange).forEach(preMutateAction);
      }

      final AtomicBoolean seenStartEmployeeId = new AtomicBoolean(false);
      final AtomicBoolean seenLastEmployeeId = new AtomicBoolean(false);

      employeeDataset.records()
          .filter(employeeRange)
          .peek(r -> { if (r.get(idx).get().equals(startEmployeeId)) seenStartEmployeeId.set(true); })
          .peek(r -> { if (r.get(idx).get().equals(lastEmployeeId)) seenLastEmployeeId.set(true); })
          .forEach(mutateAction);

      assertThat(seenStartEmployeeId.get(), is(true));
      assertThat(seenLastEmployeeId.get(), is(true));

      if (postMutateAction != null) {
        employeeDataset.records().filter(employeeRange).forEach(postMutateAction);
      }
    }
  }

  private final class InternalPrintStream extends PrintStream {

    private final String linePrefix;

    public InternalPrintStream(final PrintStream wrappedStream, final String linePrefix) {
      super(wrappedStream, true);
      this.linePrefix = linePrefix;
    }

    @Override
    public void println(final Object x) {
      super.append(this.linePrefix);
      super.println(x);
    }
  }
}
