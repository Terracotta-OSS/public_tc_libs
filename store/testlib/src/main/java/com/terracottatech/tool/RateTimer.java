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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author cschanck
 **/
public class RateTimer {

  static EnumMap<TimeUnit, String> nickName = new EnumMap<TimeUnit, String>(TimeUnit.class);

  static {
    nickName.put(TimeUnit.DAYS, "days");
    nickName.put(TimeUnit.HOURS, "hours");
    nickName.put(TimeUnit.MINUTES, "mins");
    nickName.put(TimeUnit.SECONDS, "secs");
    nickName.put(TimeUnit.MICROSECONDS, "usecs");
    nickName.put(TimeUnit.MILLISECONDS, "ms");
    nickName.put(TimeUnit.NANOSECONDS, "ns");
  }

  public static class StatsBaby {
    private final String tag;
    private long eventCount = 0;
    private long elapsedNS = 0l;
    private long minNS = Long.MAX_VALUE;
    private long maxNS = Long.MIN_VALUE;
    private long newMean;
    private double newS;
    private long oldMean;
    private double oldS;

    public StatsBaby(String tag) {
      this.tag = tag;
    }

    public void recordDurationNS(long dur) {
      // Knuth
      if (++eventCount == 1) {
        this.elapsedNS = dur;
        minNS = dur;
        maxNS = dur;
        oldMean = dur;
        newMean = dur;
        oldS = 0.0;
      } else {
        this.elapsedNS = this.elapsedNS + dur;
        minNS = Math.min(dur, minNS);
        maxNS = Math.max(dur, maxNS);
        newMean = oldMean + (dur - oldMean) / eventCount;
        newS = oldS + (dur - oldMean) * (dur - newMean);
        // set up for next iteration
        oldMean = newMean;
        oldS = newS;
      }
    }

    public String getTag() {
      return tag;
    }

    public StatsBaby reset() {
      eventCount = 0;
      elapsedNS = 0l;
      return this;
    }

    public long elapsedNS() {
      return elapsedNS;
    }

    public long eventCount() {
      return eventCount;
    }

    public float meanEventDuration() {
      return (eventCount > 0) ? newMean : (float) 0.0d;
    }

    public long minEventDuration() {
      return (eventCount > 0) ? minNS : 0;
    }

    public long maxEventDuration() {
      return (eventCount > 0) ? maxNS : 0;
    }

    public double varianceEventDuration() {
      return ((eventCount > 1) ? newS / (eventCount - 1) : 0.0);
    }

    public double stdDeviationEventDuration() {
      return Math.sqrt(varianceEventDuration());
    }

    public float opsRate(TimeUnit unit) {
      long inUnits = unit.convert(elapsedNS, TimeUnit.NANOSECONDS);
      return (float) eventCount / (float) inUnits;
    }

    public String opsRateString(TimeUnit unit) {
      String s = String.format("%.2f", opsRate(unit));
      return eventCount() + " events @ " + s + "/" + nickName.get(unit).toLowerCase();
    }

    public String rateReport(TimeUnit unit) {
      StringWriter sw = new StringWriter();
      PrintWriter ps = new PrintWriter(sw);
      ps.println(tagPrefix() + eventCount() + " @ " + opsRate(unit) + " / " + nickName.get(unit).toLowerCase() + "[");
      ps.println("   elapsed: " + elapsedNS() + "ns ");
      ps.println("   minimum: " + minEventDuration() + "ns ");
      ps.println("   maximum: " + maxEventDuration() + "ns ");
      ps.println("   average: " + meanEventDuration() + "ns ");
      ps.println("   std dev: " + stdDeviationEventDuration() + "ns ");
      ps.print("  variance: " + varianceEventDuration() + "ns ]");
      ps.flush();
      return sw.toString();
    }

    private String tagPrefix() {
      if (tag == null || tag.length() == 0) {
        return "";
      } else {
        return tag + ": ";
      }
    }

  }

  private long wallStartMS = System.currentTimeMillis();
  private StatsBaby total = new StatsBaby("Total");
  private StatsBaby delta = new StatsBaby("Delta");

  /**
   * create and start a timer.
   */
  public void RateTimer() {
    start();
  }

  /**
   * Mark the current wall clock time and reset all the event stats.
   *
   * @return
   */
  public RateTimer start() {
    this.wallStartMS = System.currentTimeMillis();
    reset();
    return this;
  }

  public StatsBaby getDelta() {
    return delta;
  }

  public StatsBaby getTotalStats() {
    return total;
  }

  public long getWallStartMS() {
    return wallStartMS;
  }

  public long getElapsedNS() {
    return total.elapsedNS();
  }

  public long getEventCount() {
    return total.eventCount();
  }

  public RateTimer reset() {
    total.reset();
    delta.reset();
    return this;
  }

  public <E extends Exception> RateTimer event(Runnable r) {
    long start = System.nanoTime();
    r.run();
    long dur = (System.nanoTime() - start);
    total.recordDurationNS(dur);
    delta.recordDurationNS(dur);
    return this;
  }

  public <E extends Exception, R> R event(Supplier<R> r) {
    long start = System.nanoTime();
    R ret = r.get();
    long dur = (System.nanoTime() - start);
    total.recordDurationNS(dur);
    delta.recordDurationNS(dur);
    return ret;
  }

  public long elapsedWallClock(TimeUnit unit) {
    return unit.convert(System.currentTimeMillis() - wallStartMS, TimeUnit.MILLISECONDS);
  }

  public String opsRateString(TimeUnit unit) {
    return total.opsRateString(unit);
  }

  public String rateReport(TimeUnit unit) {
    return total.rateReport(unit);
  }

}
