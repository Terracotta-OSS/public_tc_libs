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
package com.terracottatech.sovereign.impl.utils;

import com.terracottatech.sovereign.common.utils.VicariousThreadLocal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Alex Snaps
 */
public class Timestamper implements TimeGenerator {

  /**
   * Value for left shifting System.currentTimeMillis, freeing some space for the counter
   */
  public static final int BIN_DIGITS = Integer.getInteger("net.sf.ehcache.util.Timestamper.shift", 12);
  private static final long serialVersionUID = -964091289236241379L;

  /**
   * What is one milliseconds, based on "counter value reserved space", for this Timestamper
   */
  public final int ONE_MS = 1 << BIN_DIGITS;

  private final AtomicLong VALUE = new AtomicLong();

  /**
   * Returns an increasing unique value based on the System.currentTimeMillis()
   * with some additional reserved space for a counter.
   *
   * @return uniquely &amp; increasing value
   */
  public long next() {
    while (true) {
      long base = SlewClock.timeMillis() << BIN_DIGITS;
      long maxValue = base + ONE_MS - 1;

      for (long current = VALUE.get(), update = Math.max(base, current + 1); update < maxValue;
           current = VALUE.get(), update = Math.max(base, current + 1)) {
        if (VALUE.compareAndSet(current, update)) {
          return update;
        }
      }
    }
  }

  public long elapsedSince(long ts) {
    return SlewClock.timeMillis() - (ts / ONE_MS);
  }

  private static class SlewClock {

    private static final long DRIFT_MAXIMAL = Integer.getInteger("com.terracottatech.sovereign.util.Timestamper.drift.max", 50);

    private static final long SLEEP_MAXIMAL = Integer.getInteger("com.terracottatech.sovereign.util.Timestamper.sleep.max", 50);

    private static final int SLEEP_BASE = Integer.getInteger("com.terracottatech.sovereign.util.Timestamper.sleep.min", 25);

    private static final AtomicLong CURRENT = new AtomicLong(Long.MIN_VALUE);

    private static final VicariousThreadLocal<Long> OFFSET = new VicariousThreadLocal<>();

    private SlewClock() {
      // You shall not instantiate me!
    }

    /**
     * Will return the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
     * But without ever going back. If a movement back in time is detected, the method will slew until time caught up
     *
     * @return The difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
     */
    static long timeMillis() {
      boolean interrupted = false;
      try {
        while (true) {
          long mono = CURRENT.get();
          long wall = System.currentTimeMillis();
          if (wall == mono) {
            OFFSET.remove();
            return wall;
          } else if (wall > mono) {
            if (CURRENT.compareAndSet(mono, wall)) {
              OFFSET.remove();
              return wall;
            }
          } else {
            long delta = mono - wall;
            if (delta < DRIFT_MAXIMAL) {
              OFFSET.remove();
              return mono;
            } else {
              Long lastDelta = OFFSET.get();
              if (lastDelta == null || delta < lastDelta) {
                if (CURRENT.compareAndSet(mono, mono + 1)) {
                  OFFSET.set(Long.valueOf(delta));
                  return mono + 1;
                }
              } else {
                OFFSET.set(Long.valueOf(Math.max(delta, lastDelta)));
                try {
                  Thread.sleep(sleepTime(delta, lastDelta));
                } catch (InterruptedException e) {
                  interrupted = true;
                }
              }
            }
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private static long sleepTime(final long current, final long previous) {
      long target = SLEEP_BASE + (current - previous) * 2;
      return Math.min(target > 0 ? target : SLEEP_BASE, SLEEP_MAXIMAL);
    }
  }
}

