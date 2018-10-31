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

package com.terracottatech.sovereign.testsupport;

import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Supports running all or selected methods from designated test class.
 * This method is intended to be called from the {@code main} method of a JUnit test class.
 */
public final class SelectedTestRunner {
  private final Class<?> testClass;
  private final PrintStream out;

  public SelectedTestRunner(final Class<?> testClass, final PrintStream out) {
    this.testClass = testClass;
    this.out = out;
  }

  /**
   * Runs all or selected tests from the configured class.
   *
   * @param args the argument list:
   * <ol>
   *   <li>the number of times to repeat the selected tests</li>
   *   <li>zero or more test method names</li>
   * </ol>
   */
  public void runTests(final String[] args) {
    if (args.length < 1) {
      System.err.println("Syntax: <repeatCount> [<testMethod1> [<testMethod2> ...]]");
      System.exit(1);
      throw new AssertionError();
    }
    final int repeatCount = Integer.parseInt(args[0]);

    Request request = Request.aClass(testClass);

    /*
     * If the caller specified test method names, create a filter to limit the tests to
     * those specified.
     */
    if (args.length > 1) {
      final MethodFilter filter = new MethodFilter();
      for (int i = 1; i < args.length; i++) {
        final String testMethod = args[i];
        filter.add(Description.createTestDescription(testClass, testMethod));
      }
      request = request.filterWith(filter);
    }

    /*
     * Run the tests ...
     */
    final JUnitCore jUnitCore = new JUnitCore();
    final LocalListener listener = new LocalListener();
    jUnitCore.addListener(listener);
    for (int i = 1; i <= repeatCount; i++) {
      listener.setIteration(i);
      jUnitCore.run(request);
    }
  }

  /**
   * Inner class providing test result reporting.
   */
  private final class LocalListener extends RunListener {
    private int iteration = 0;

    public void setIteration(final int iteration) {
      this.iteration = iteration;
    }

    @Override
    public void testRunStarted(final Description description) throws Exception {
      out.format("%n%1$tT.%1tL [%02d] Starting %s; iteration=%2$d%n",
          System.currentTimeMillis(), this.iteration, description);
    }

    @Override
    public void testRunFinished(final Result result) throws Exception {
      out.format("%1$tT.%1tL [%02d] Ended  Runtime=%.3f s%n",
          System.currentTimeMillis(), this.iteration, result.getRunTime() / 1000f);
      if (result.getFailureCount() != 0) {
        out.println("Failed tests:");
        for (final Failure failure : result.getFailures()) {
          out.format("    %s%n", failure);
        }
      }
      out.format("Tests run=%d, ignored=%d, failed=%d%n",
          result.getRunCount(), result.getIgnoreCount(), result.getFailureCount());
    }

    @Override
    public void testAssumptionFailure(final Failure failure) {
      out.format("%1$tT.%1tL [%02d] Skipping %s%n", System.currentTimeMillis(), this.iteration, failure);
    }
  }

  /**
   * JUnit {@link Filter} that handles selection of multiple test methods from a {@link Request}.
   */
  private static final class MethodFilter extends Filter {

    final List<Description> selectedMethods = new ArrayList<>();

    public void add(final Description methodDescription) {
      this.selectedMethods.add(methodDescription);
    }

    @Override
    public boolean shouldRun(final Description description) {
      return this.selectedMethods.contains(description);
    }

    @Override
    public String describe() {
      return String.format("Multiple methods: %s", this.selectedMethods);
    }
  }
}
