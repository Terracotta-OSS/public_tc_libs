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
package com.terracottatech.tool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Simplest perf test framework imaginable, useful for during-dev perf checking.
 */
public class TinyTester {

  private final static long REPORT_INTERVAL_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  private final int many;
  private final String tag;
  private ArrayList<TinyStepper> workers = new ArrayList<>();
  private ArrayList<Thread> runningThreads = new ArrayList<>();
  private ArrayList<RateTimer> timers = new ArrayList<>();

  public TinyTester(String tag, int many, Supplier<TinyStepper> supplier) {
    this.many = many;
    this.tag = tag;
    for (int i = 0; i < many; i++) {
      workers.add(supplier.get());
    }
  }

  public void start(long duration, TimeUnit units) {
    System.out.println("Starting tiny perf test for [" + tag + "]");
    timers.clear();
    runningThreads.clear();
    final long stop = TimeUnit.MILLISECONDS.convert(duration, units) + System.currentTimeMillis();
    for (int i = 0; i < many; i++) {
      int finalI = i;
      final RateTimer rt = new RateTimer();
      Thread t = new Thread(() -> {
        long nextReport = System.currentTimeMillis() + REPORT_INTERVAL_MS;
        long step = 1;
        long reports = 0;
        TinyStepper dut = workers.get(finalI);
        for (; ; step++) {
          if ((step & 127) == 0) {
            long n = System.currentTimeMillis();
            if (n > stop) {
              break;
            } else if (n > nextReport) {
              System.out.println("Thread: " + finalI + ", Pass: " + (++reports) + ": " + rt.opsRateString(TimeUnit.SECONDS));
              nextReport = n + REPORT_INTERVAL_MS;
            }
          }
          rt.event(dut);
        }
      });
      t.setDaemon(true);
      t.start();
      timers.add(rt);
      runningThreads.add(t);
    }
  }

  public void awaitFinished() {
    runningThreads.stream().forEach((t) -> {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    System.out.println();
    System.out.println("Final: [" + tag + "]");
    for (int i = 0; i < workers.size(); i++) {
      TinyStepper worker = workers.get(i);
      RateTimer rt = timers.get(i);
      System.out.println("\t" + rt.opsRateString(TimeUnit.SECONDS) + " (" + worker.addendumString() + ")");
    }
  }

  public Collection<RateTimer> getRateTimers() {
    return new ArrayList<>(timers);
  }

  public String rateSummary(TimeUnit units) {
    Collection<RateTimer> t = getRateTimers();
    long events = t.stream().mapToLong(rt -> rt.getTotalStats().eventCount()).sum();
    long elapsedNS = t.stream().mapToLong(rt -> rt.getTotalStats().elapsedNS()).sorted().max().orElse(1l);

    long inUnits = units.convert(elapsedNS, TimeUnit.NANOSECONDS);
    float rate = (float) events / (float) inUnits;

    String s = String.format("%.2f", rate);
    return events + " events @ " + s + "/" + units.name();
  }

  public abstract static class TinyStepper implements Runnable {
    public String addendumString() {
      return "";
    }
  }

}
