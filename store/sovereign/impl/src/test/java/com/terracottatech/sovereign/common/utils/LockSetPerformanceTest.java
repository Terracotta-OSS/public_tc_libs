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

package com.terracottatech.sovereign.common.utils;

import com.terracottatech.sovereign.impl.utils.LockSet;
import org.junit.Test;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.terracottatech.sovereign.testsupport.TestUtility.formatNanos;
import static java.util.stream.Collectors.summarizingLong;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

/**
 * @author Clifford W. Johnson
 */
public class LockSetPerformanceTest {

  /**
   * When this system property is set to {@code true}, the {@link #testCompareFairVsNonFair()}
   * test is run.
   */
  public static final String FAIR_TEST_PROPERTY = "com.terracottatech.sovereign.lockSet.fairTest";

  /**
   * Reference to {@link ThreadMXBean}.  This reference is used to obtain statistics
   * about lock and thread performance.
   */
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  static {
    THREAD_MX_BEAN.setThreadContentionMonitoringEnabled(true);
    THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
  }

  /**
   * Reflective method handle used to determine the lock index assigned to an object.
   */
  private static final Method lockSetIndex;
  static {
    final Method indexMethod;
    try {
      indexMethod = LockSet.class.getDeclaredMethod("index", Object.class);
      indexMethod.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
    lockSetIndex = indexMethod;
  }

  /**
   * Calculates statistics and compares the performance of fair versus non-fair {@link LockSet}
   * instances.  This test only runs if the <code>{@value #FAIR_TEST_PROPERTY}</code> system property
   * is {@code true}.
   */
  @Test
  public void testCompareFairVsNonFair() throws Exception {
    assumeThat(Boolean.valueOf(System.getProperty(FAIR_TEST_PROPERTY, "false")), is(true));

    final int processorCount = Runtime.getRuntime().availableProcessors();
    final int threadCount = 4 * processorCount;
    final int concurrency = 2 * processorCount;
    final long duration = 10L;
    final TimeUnit units = TimeUnit.MINUTES;

    final Runnable normalPayload = () -> {
      try {
        TimeUnit.NANOSECONDS.sleep(1000L);
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    };

    final Runnable baselinePayload = () -> {
      final long startNanos = System.nanoTime();
      //noinspection StatementWithEmptyBody
      do {
        // empty
      } while (System.nanoTime() - startNanos < TimeUnit.NANOSECONDS.toNanos(1000L));
    };

    /*
     * Warm-up
     */
    System.out.println("Performing 20 SECOND warm-up");
    testLockSet(baselinePayload, 10, 5, true, 10, TimeUnit.SECONDS, false);
    testLockSet(baselinePayload, 10, 5, false, 10, TimeUnit.SECONDS, false);

    /*
     * Attempt to gauge BASELINE performance of locking -- does not use WAIT is *not* used in the
     * payload of the timing loop.
     */
    {
      long startNanos;
      System.out.println();
      System.out.format("Performing FAIR baseline: threadCount=%d, concurrency=%d, duration=%d %s%n",
          threadCount, concurrency, duration, units);
      startNanos = System.nanoTime();
      final LockSetStats fairLockSetStats =
          testLockSet(normalPayload, threadCount, concurrency, true, duration, units, true);
      System.out.format("BASELINE TotalFairLocksTaken=%d, %s%n",
          fairLockSetStats.getTotalLocksTaken(), formatNanos(System.nanoTime() - startNanos));
      System.out.println();
      System.out.format("Performing NonFAIR baseline: threadCount=%d, concurrency=%d, duration=%d %s%n",
          threadCount, concurrency, duration, units);
      startNanos = System.nanoTime();
      final LockSetStats nonFairLockSetStats =
          testLockSet(normalPayload, threadCount, concurrency, false, duration, units, true);
      System.out.format("BASELINE TotalNonFairLocksTaken=%d, %s%n",
          nonFairLockSetStats.getTotalLocksTaken(), formatNanos(System.nanoTime() - startNanos));

      displayResults(fairLockSetStats, nonFairLockSetStats);
    }

    /*
     * Attempt to gauge "normal" performance of locking -- WAIT is used in the payload of the timing loop.
     */
    {
      long startNanos;
      System.out.println();
      System.out.format("Performing FAIR test: threadCount=%d, concurrency=%d, duration=%d %s%n",
          threadCount, concurrency, duration, units);
      startNanos = System.nanoTime();
      final LockSetStats fairLockSetStats =
          testLockSet(normalPayload, threadCount, concurrency, true, duration, units, true);
      System.out.format("TotalFairLocksTaken=%d, %s%n",
          fairLockSetStats.getTotalLocksTaken(), formatNanos(System.nanoTime() - startNanos));

      System.out.println();
      System.out.format("Performing NonFAIR test: threadCount=%d, concurrency=%d, duration=%d %s%n",
          threadCount, concurrency, duration, units);
      startNanos = System.nanoTime();
      final LockSetStats nonFairLockSetStats =
          testLockSet(normalPayload, threadCount, concurrency, false, duration, units, true);
      System.out.format("TotalNonFairLocksTaken=%d, %s%n",
          nonFairLockSetStats.getTotalLocksTaken(), formatNanos(System.nanoTime() - startNanos));

      displayResults(fairLockSetStats, nonFairLockSetStats);
    }
  }

  private void displayResults(final LockSetStats fairLockSetStats, final LockSetStats nonFairLockSetStats) {
    System.out.println();
    System.out.format("%15.15s  %-20.20s  %-20.20s  %-11.11s%n", "", "FAIR", "NonFAIR", "Difference");
    System.out.format("%1$15.15s  %1$-20.20s  %1$-20.20s  %1$-11.11s%n", "--------------------");

    printStat("locksTaken", fairLockSetStats.getLocksTakenStats(), nonFairLockSetStats.getLocksTakenStats());
    printStat("locksPerIndex", fairLockSetStats.getLocksPerIndexStats(), nonFairLockSetStats.getLocksPerIndexStats());
    if (fairLockSetStats.getBlockedCountStats().getSum() != 0 || nonFairLockSetStats.getBlockedCountStats().getSum() != 0) {
      printStat("blockedCount", fairLockSetStats.getBlockedCountStats(), nonFairLockSetStats.getBlockedCountStats());
      printStat("blockedTime", fairLockSetStats.getBlockedTimeStats(), nonFairLockSetStats.getBlockedTimeStats());
    }
    printStat("waitedCount", fairLockSetStats.getWaitedCountStats(), nonFairLockSetStats.getWaitedCountStats());
    printStat("waitedTime", fairLockSetStats.getWaitedTimeStats(), nonFairLockSetStats.getWaitedTimeStats());
  }

  private void printStat(final String statId, final Stats fairStats, final Stats nonFairStats) {
    System.out.format("%s:%n", statId);
    printStat("Min", fairStats.getMin(), nonFairStats.getMin());
    printStat("Max", fairStats.getMax(), nonFairStats.getMax());
    printStat("Sum", fairStats.getSum(), nonFairStats.getSum());
    printStat("Average", fairStats.getAvg(), nonFairStats.getAvg());
    printStat("StdDev", fairStats.getStdDev(), nonFairStats.getStdDev());
  }

  private void printStat(final String lineId, final long fair, final long nonFair) {
    System.out.format("%15.15s  %20d  %20d  %+10.2f%%%n", lineId, fair, nonFair, ((nonFair - fair) * 100) / (fair * 1.0f));
  }

  private void printStat(final String lineId, final double fair, final double nonFair) {
    System.out.format("%15.15s  %20.4f  %20.4f  %+10.2f%%%n", lineId, fair, nonFair, ((nonFair - fair) * 100) / fair);
  }

  /**
   * Performs a {@link LockSet} performance test.
   *
   * @param payload the operation to perform under lock
   * @param threadCount the number of threads to use in the test
   * @param concurrency the number of locks to allocate for the test
   * @param isFair the type of lock to allocate for the test
   * @param duration the time the test is to run
   * @param units the units of {@code duration}
   * @param showStats indicates if performance statistics for each thread are displayed
   *
   * @return the total number of locks taken
   *
   * @throws InterruptedException if test thread execution is interrupted
   */
  private LockSetStats testLockSet(final Runnable payload,
                                   final int threadCount,
                                   final int concurrency,
                                   final boolean isFair,
                                   final long duration, final TimeUnit units,
                                   final boolean showStats)
      throws InterruptedException {
    final LockSet lockSet = new LockSet(concurrency, isFair);

    /*
     * ExecutorService under which the performance threads are run
     */
    final ExecutorService service = Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
      private int idCount = 0;
      @Override
      public Thread newThread(@SuppressWarnings("NullableProblems") final Runnable r) {
        return new Thread(r, String.format("LockSetTest[%d]", ++idCount));
      }
    });

    /*
     * Assemble the test task and duplicate for the number of test threads.
     */
    final TestCallable testTask = new TestCallable(lockSet, payload);
    final List<TestCallable> testTasks = new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      testTasks.add(testTask);
    }

    /*
     * A timer to end the test.
     */
    final Timer testTimer = new Timer("LockSetTestTimer", true);
    testTimer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            for (TestCallable task : testTasks) {
              task.setIsDone(true);
            }
          }
        },
        units.toMillis(duration));

    /*
     * Start and run the test threads/tasks until the timer (above) stops the tests.
     */
    final List<Future<ThreadDetails>> futures = service.invokeAll(testTasks);

    /*
     * Collect the results and report any exceptions thrown as a test failure.
     */
    final List<ThreadDetails> results = new ArrayList<>(futures.size());
    AssertionError fault = null;
    for (final Future<ThreadDetails> future : futures) {
      try {
        results.add(future.get());
      } catch (CancellationException | InterruptedException | ExecutionException e) {
        if (fault == null) {
          fault = new AssertionError(e);
        } else {
          fault.addSuppressed(e);
        }
      }
    }
    if (fault != null) {
      throw fault;
    }

    /*
     * Collect and report out the statistics.
     */
    final LockSetStats lockSetStats = new LockSetStats(concurrency);
    results.forEach(lockSetStats::collect);

    if (showStats) {
      lockSetStats.display(System.out);
    }

    return lockSetStats;
  }

  /**
   * Rounds up to the next power of 2.  The algorithm works for non-negative integers.
   * @param i the number for which the next power of 2 is desired
   * @return the power of 2 greater than or equal to {@code i}
   * @see <cite>Hacker's Delight, Chapter 3, Figure 3-3</cite>
   */
  private static int clp2(int i) {
    i -= 1;
    i |= i >> 1;
    i |= i >> 2;
    i |= i >> 4;
    i |= i >> 16;
    return i + 1;
  }

  /**
   * Calculates the lock index within a {@link LockSet} for the {@code Object} provided.
   *
   * @param lockSet the {@code LockSet} for the index
   * @param k the object for which the index is determined
   *
   * @return the index into {@code LockSet} for {@code k}
   */
  private static int lockIndex(final LockSet lockSet, final Object k) {
    try {
      return (int)lockSetIndex.invoke(lockSet, k);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * A data structure in which the performance details for a {@code Thread} is collected.
   */
  private static final class ThreadDetails {
    private final int lockIndex;
    private final long locksTaken;
    private final long threadId;
    private final String threadName;
    private final long blockedCount;
    private final long blockedTime;
    private final long waitedCount;
    private final long waitedTime;
    private final long cpuTime;
    private final long userTime;

    public ThreadDetails(final int lockIndex, final long locksTaken, final ThreadInfo threadInfo, final long cpuTime, final long userTime) {
      this.lockIndex = lockIndex;
      this.locksTaken = locksTaken;
      this.cpuTime = cpuTime;
      this.userTime = userTime;
      this.threadId = threadInfo.getThreadId();
      this.threadName = threadInfo.getThreadName();
      this.blockedCount = threadInfo.getBlockedCount();
      this.blockedTime = threadInfo.getBlockedTime();
      this.waitedCount = threadInfo.getWaitedCount();
      this.waitedTime = threadInfo.getWaitedTime();
    }

    public int getLockIndex() {
      return lockIndex;
    }

    public long getLocksTaken() {
      return locksTaken;
    }

    public long getBlockedCount() {
      return blockedCount;
    }

    public long getBlockedTime() {
      return blockedTime;
    }

    public long getWaitedCount() {
      return waitedCount;
    }

    public long getWaitedTime() {
      return waitedTime;
    }

    @SuppressWarnings("StringBufferReplaceableByString")
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ThreadDetails{");
      sb.append(this.threadName).append('(').append(this.threadId).append(')');
      sb.append(", lockIndex=").append(lockIndex);
      sb.append(", locksTaken=").append(locksTaken);
      sb.append(", blockedCount=").append(blockedCount);
      sb.append(", blockedTime=").append(blockedTime);
      sb.append(", waitedCount=").append(waitedCount);
      sb.append(", waitedTime=").append(waitedTime);
      sb.append(", cpuTime=").append(cpuTime);
      sb.append(", userTime=").append(userTime);
      sb.append('}');
      return sb.toString();
    }
  }

  private static final class LockSetStats {

    private final List<ThreadDetails> results = new ArrayList<>();
    private final Stats locksTakenStats = new Stats();
    private final Stats locksPerIndexStats = new Stats();
    private final Stats blockedCountStats = new Stats();
    private final Stats blockedTimeStats = new Stats();
    private final Stats waitedCountStats = new Stats();
    private final Stats waitedTimeStats = new Stats();

    private final long[] locksPerIndex;
    private long totalLocksTaken = 0;

    private boolean computed = false;

    public LockSetStats(final int concurrency) {
      this.locksPerIndex = new long[clp2(concurrency)];
    }

    public void collect(final ThreadDetails threadDetails) {
      if (computed) {
        throw new IllegalStateException();
      }
      results.add(threadDetails);
      totalLocksTaken += threadDetails.getLocksTaken();
      locksTakenStats.collect(threadDetails.getLocksTaken());
      blockedCountStats.collect(threadDetails.getBlockedCount());
      blockedTimeStats.collect(threadDetails.getBlockedTime());
      waitedCountStats.collect(threadDetails.getWaitedCount());
      waitedTimeStats.collect(threadDetails.getWaitedTime());
      locksPerIndex[threadDetails.getLockIndex()] += threadDetails.getLocksTaken();
    }

    public long getTotalLocksTaken() {
      this.compute();
      return totalLocksTaken;
    }

    @SuppressWarnings("unused")
    public List<ThreadDetails> getResults() {
      this.compute();
      return results;
    }

    public Stats getLocksTakenStats() {
      this.compute();
      return locksTakenStats;
    }

    public Stats getLocksPerIndexStats() {
      this.compute();
      return locksPerIndexStats;
    }

    public Stats getBlockedCountStats() {
      this.compute();
      return blockedCountStats;
    }

    public Stats getBlockedTimeStats() {
      this.compute();
      return blockedTimeStats;
    }

    public Stats getWaitedCountStats() {
      this.compute();
      return waitedCountStats;
    }

    public Stats getWaitedTimeStats() {
      this.compute();
      return waitedTimeStats;
    }

    @SuppressWarnings("unused")
    public long[] getLocksPerIndex() {
      this.compute();
      return locksPerIndex;
    }

    public LockSetStats compute() {
      if (!computed) {
        computed = true;
        for (long lockCount : locksPerIndex) {
          locksPerIndexStats.collect(lockCount);
        }
      }
      return this;
    }

    public void display(final PrintStream out) {
      this.compute();
      results.forEach(out::println);
      out.format("locksTaken: %s%n", locksTakenStats.compute().toString());
      out.format("blockedCountStats: %s%n", blockedCountStats.compute().toString());
      out.format("blockedTimeStats: %s%n", blockedTimeStats.compute().toString());
      out.format("waitedCountStats: %s%n", waitedCountStats.compute().toString());
      out.format("waitedTimeStats: %s%n", waitedTimeStats.compute().toString());
      out.format("locksPerIndex: %s%n", locksPerIndexStats.compute().toString());
    }
  }

  /**
   * A data structure used to calculate statistics for a performance attribute.
   */
  private static final class Stats {
    private final List<Long> observations = new ArrayList<>();
    private volatile boolean computed = false;
    private long min;
    private long max;
    private long sum;
    private double avg;
    private double variance;
    private double stdDev;

    public void collect(long observation) {
      if (this.computed) {
        throw new IllegalStateException();
      }
      this.observations.add(observation);
    }

    public Stats compute() {
      if (computed) {
        return this;
      }
      this.computed = true;

      final LongSummaryStatistics summaryStatistics = observations.stream().collect(summarizingLong(l -> l));
      this.min = summaryStatistics.getMin();
      this.max = summaryStatistics.getMax();
      this.sum = summaryStatistics.getSum();
      this.avg = summaryStatistics.getAverage();
      final long count = summaryStatistics.getCount();

      double sumOfSquareDeviations = 0;
      for (final Long observation : observations) {
        sumOfSquareDeviations += Math.pow(observation - avg, 2.0D);
      }
      this.variance = sumOfSquareDeviations / count;
      this.stdDev = Math.sqrt(this.variance);

      return this;
    }

    public long getMin() {
      this.compute();
      return min;
    }

    public long getMax() {
      this.compute();
      return max;
    }

    public long getSum() {
      this.compute();
      return sum;
    }

    public double getAvg() {
      this.compute();
      return avg;
    }

    @SuppressWarnings("unused")
    public double getVariance() {
      this.compute();
      return variance;
    }

    public double getStdDev() {
      this.compute();
      return stdDev;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Stats{");
      if (this.computed) {
        sb.append("count=").append(observations.size());
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", sum=").append(sum);
        sb.append(", avg=").append(avg);
        sb.append(", variance=").append(variance);
        sb.append(", stdDev=").append(stdDev);
        sb.append('}');
      } else {
        sb.append("computed=").append(computed);
        sb.append(", count=").append(observations.size());
      }
      return sb.toString();
    }
  }

  /**
   * The performance test task.
   */
  private static class TestCallable implements Callable<ThreadDetails> {
    private final LockSet lockSet;
    private final Runnable payload;
    private volatile boolean isDone;

    public TestCallable(final LockSet lockSet, final Runnable payload) {
      this.lockSet = lockSet;
      this.payload = payload;
    }

    @Override
    public ThreadDetails call() throws Exception {
      final Thread thisThread = Thread.currentThread();
      final long beginCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
      final long beginUserTime = THREAD_MX_BEAN.getCurrentThreadUserTime();

      final ReentrantLock lock = lockSet.lockFor(thisThread);
      long locksTaken = 0;
      while (!isDone) {
        lock.lock();
        try {
          locksTaken++;
          payload.run();
        } finally {
          lock.unlock();
        }
      }

      return new ThreadDetails(lockIndex(lockSet, thisThread), locksTaken,
          THREAD_MX_BEAN.getThreadInfo(thisThread.getId()),
          THREAD_MX_BEAN.getCurrentThreadCpuTime() - beginCpuTime,
          THREAD_MX_BEAN.getCurrentThreadUserTime() - beginUserTime);
    }

    public void setIsDone(final boolean isDone) {
      this.isDone = isDone;
    }
  }
}
