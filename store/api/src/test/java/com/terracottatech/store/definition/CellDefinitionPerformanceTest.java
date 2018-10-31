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

package com.terracottatech.store.definition;

import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
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

/**
 * The tests in this class are aimed at assessing the performance of {@link CellDefinition#define(String, Type)}.
 *
 * @author Clifford W. Johnson
 */
@Ignore
public class CellDefinitionPerformanceTest {

  /**
   * Reference to {@link ThreadMXBean}.  This reference is used to obtain statistics
   * about lock and thread performance.
   */
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  static {
    THREAD_MX_BEAN.setThreadContentionMonitoringEnabled(true);
    THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
  }

  @Rule
  public final TestName name = new TestName();

  /**
   * Exercises the performance of {@link CellDefinition#define(String, Type)} for a single name/type pair.
   */
  @Test
  public void testSingleDefinitionPerformance() throws Exception {

    for (int i = 1; i < Runtime.getRuntime().availableProcessors(); i++) {
      /*
       * Task that simple repeatedly defines the CellDefinitions in EmployeeSchema.
       */
      final TestTask task = new TestTask() {
        @Override
        public Result call() throws Exception {
          long definitionCount = 0;

          while (!this.terminated) {
            CellDefinition.define("common", Type.STRING);
            definitionCount++;
          }

          return new Result(definitionCount);
        }
      };

      runPerfTest(task, i, 10L, TimeUnit.SECONDS);
    }
  }

  /**
   * Exercised the performance of {@link CellDefinition#define(String, Type)} for multiple name/type pairs.
   */
  @Ignore
  @Test
  public void testMultipleDefinitionPerformance() throws Exception {
    final List<CellDefinition<?>> definitions = new ArrayList<>();
    definitions.add(CellDefinition.define("empId", Type.INT));
    definitions.add(CellDefinition.define("firstName", Type.STRING));
    definitions.add(CellDefinition.define("lastName", Type.STRING));
    definitions.add(CellDefinition.define("Email", Type.STRING));
    definitions.add(CellDefinition.define("Salary", Type.DOUBLE));
    definitions.add(CellDefinition.define("Age", Type.INT));
    definitions.add(CellDefinition.define("State", Type.STRING));
    definitions.add(CellDefinition.define("Country", Type.STRING));
    definitions.add(CellDefinition.define("Weight", Type.DOUBLE));
    definitions.add(CellDefinition.define("Married", Type.BOOL));
    definitions.add(CellDefinition.define("Phone", Type.STRING));
    definitions.add(CellDefinition.define("Photo", Type.BYTES));

    /*
     * Task that simple repeatedly defines the CellDefinitions above.
     */
    final TestTask task = new TestTask() {
      @Override
      public Result call() throws Exception {
        long definitionCount = 0;

        out:
        while (true) {
          for (final CellDefinition<?> template : definitions) {
            if (this.terminated) {
              break out;
            }
            CellDefinition.define(template.name(), template.type());
            definitionCount++;
          }
        }

        return new Result(definitionCount);
      }
    };

    runPerfTest(task, 1, 10L, TimeUnit.SECONDS);
  }

  /**
   * Runs a task in multiple threads for a fixed amount of time.
   *
   * @param task the task to run
   * @param duration the amount of time to run the test
   * @param units the units of {@code duration}
   */
  private void runPerfTest(final TestTask task, final int threadCount, final long duration, final TimeUnit units) {
    final ExecutorService service = Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
      int threadId = 0;

      @Override
      public Thread newThread(final Runnable r) {
        final Thread thread = new Thread(r, String.format("CellDefPerf-%d", ++threadId));
        thread.setDaemon(true);
        return thread;
      }
    });

    final List<TestTask> taskList = new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      taskList.add(task);
    }

    final long testRunTime = units.toMillis(duration);

    final Timer durationTimer = new Timer(true);
    durationTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        task.terminate();
      }
    }, testRunTime);

    final long startTime = System.nanoTime();
    final List<Future<Result>> futures;
    try {
      futures = service.invokeAll(taskList);
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
    final long testDuration = System.nanoTime() - startTime;

    long totalDefinitions = 0;
    AssertionError fault = null;
    final List<Result> results = new ArrayList<>(futures.size());
    for (final Future<Result> result : futures) {
      try {
        final Result r = result.get();
        results.add(r);
        totalDefinitions += r.definitionCount;
      } catch (CancellationException | ExecutionException | InterruptedException e) {
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

    System.out.println();
    System.out.format("----- %s -----%n", this.name.getMethodName());
    System.out.format("Test duration = %s%n", formatNanos(testDuration));
    System.out.format("CellDefinitions acquired = %,d%n", totalDefinitions);

    for (final Result result : results) {
      final ThreadInfo threadInfo = result.threadInfo;
      System.out.format("Thread[id=%d, name=%s]:", threadInfo.getThreadId(), threadInfo.getThreadName());
      System.out.format(" CellDefinitions acquired = %,d", result.definitionCount);
      System.out.format(", blockedCount=%d, blockedTime=%s",
          threadInfo.getBlockedCount(), formatMillis(threadInfo.getBlockedTime()));
      System.out.format(", waitedCount=%d, waitedTime=%s%n",
          threadInfo.getWaitedCount(), formatMillis(threadInfo.getWaitedTime()));
    }
  }

  /**
   * Formats a millisecond time into a {@code String} of the form
   * {@code <hours> h <minutes> m <seconds>.<fractional_seconds> s}.
   *
   * @param millis the millisecond value to convert
   *
   * @return the formatted result
   */
  private static CharSequence formatMillis(final long millis) {
    return formatNanos(TimeUnit.MILLISECONDS.toNanos(millis));
  }

  /**
   * Formats a nanosecond time into a {@code String} of the form
   * {@code <hours> h <minutes> m <seconds>.<fractional_seconds> s}.
   *
   * @param nanos tbe nanosecond value to convert
   *
   * @return the formatted result
   */
  private static CharSequence formatNanos(final long nanos) {
    final int elapsedNanos = (int)(nanos % 1_000_000_000);
    final long elapsedTimeSeconds = nanos / 1_000_000_000;
    final int elapsedSeconds = (int)(elapsedTimeSeconds % 60);
    final long elapsedTimeMinutes = elapsedTimeSeconds / 60;
    final int elapsedMinutes = (int)(elapsedTimeMinutes % 60);
    final long elapsedHours = elapsedTimeMinutes / 60;

    final StringBuilder sb = new StringBuilder(128);
    if (elapsedHours > 0) {
      sb.append(elapsedHours).append(" h ");
    }
    if (elapsedHours > 0 || elapsedMinutes > 0) {
      sb.append(elapsedMinutes).append(" m ");
    }
    sb.append(elapsedSeconds).append('.').append(String.format("%09d", elapsedNanos)).append(" s");

    return sb;
  }

  private static final class Result {
    final long definitionCount;
    final ThreadInfo threadInfo = THREAD_MX_BEAN.getThreadInfo(Thread.currentThread().getId());

    public Result(final long definitionCount) {
      this.definitionCount = definitionCount;
    }
  }

  private static abstract class TestTask implements Callable<Result> {
    protected volatile boolean terminated = false;

    public void terminate() {
      this.terminated = true;
    }
  }
}
