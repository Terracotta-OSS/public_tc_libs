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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

/**
 * This runs a timer at the specified interval, allowing for coarser sampling but
 * with much less overhead.
 *
 * @author cschanck
 **/
public class CoarseClock {

  /**
   * Current time millis updater.
   */
  public static final LongUnaryOperator CURRENT_TIME_MILLIS = (prev) -> {
    return System.currentTimeMillis();
  };

  /**
   * Incremental, up 1 each tick of the coarse clock.
   */
  public static final LongUnaryOperator INCREMENTAL_ONLY = (prev) -> {
    return prev + 1;
  };

  private final Timer timer;
  private final long millis;
  private volatile long now;

  public CoarseClock(long granularity, TimeUnit t, LongUnaryOperator nextFunction) {
    this.millis = TimeUnit.MILLISECONDS.convert(granularity, t);
    this.timer = new Timer("CoarseClock", true);
    now = nextFunction.applyAsLong(0);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        now = nextFunction.applyAsLong(now);
      }
    }, millis, millis);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }

  public long currentClock() {
    return now;
  }

  public void close() {
    timer.cancel();
  }

  public static void main(String[] args) {
    CoarseClock coarseClock = new CoarseClock(10, TimeUnit.SECONDS, CURRENT_TIME_MILLIS);

    final int MANY = 1000000000;

    long tmp = System.currentTimeMillis();
    for (int i = 0; i < MANY / 10; i++) {
      tmp = tmp + System.currentTimeMillis();
    }
    System.out.println(tmp);

    tmp = System.currentTimeMillis();
    for (int i = 0; i < MANY / 10; i++) {
      tmp = tmp + coarseClock.currentClock();
    }
    System.out.println(tmp);

    tmp = System.currentTimeMillis();
    long s1 = System.nanoTime();
    for (int i = 0; i < MANY; i++) {
      tmp = tmp + System.currentTimeMillis();
    }
    long t1 = System.nanoTime() - s1;
    System.out.println(tmp);

    tmp = coarseClock.currentClock();
    long s2 = System.nanoTime();
    for (int i = 0; i < MANY; i++) {
      tmp = tmp + coarseClock.currentClock();
    }
    long t2 = System.nanoTime() - s2;
    System.out.println(tmp);

    System.out.println("Millis: " + t1 / (float) MANY + "ns per call");
    System.out.println("Coarse: " + t2 / (float) MANY + "ns per call");

  }
}
