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

/**
 * Stopwatch class.
 *
 * @author cschanck
 */
public class SW {
  private long initTime = 0L;

  private long markTime;

  private long totalInterval;

  private long markDelta;

  private long markInterval;

  private String lastmark = "<Nothing>";

  /**
   * Instantiates a new SW.
   */
  public SW() {
    reset();
  }

  public void reset() {
    initTime = System.currentTimeMillis();
    markTime = initTime;
    markDelta = 0L;
    markInterval = 0L;
    totalInterval = 0L;
  }

  /**
   * Gets total elapsed time.
   *
   * @return the total elapsed time
   */
  public String getTotalElapsedTime() {
    StringBuilder sb = new StringBuilder();
    return getTotalElapsedTime(sb);
  }

  /**
   * Gets total elapsed time.
   *
   * @param sb the sb
   * @return the total elapsed time
   */
  public String getTotalElapsedTime(StringBuilder sb) {
    calcElapsedTime(markTime - initTime, sb);
    return sb.toString();
  }

  /**
   * Gets mark elapsed time.
   *
   * @return the mark elapsed time
   */
  public String getMarkElapsedTime() {
    StringBuilder sb = new StringBuilder();
    return getMarkElapsedTime(sb);
  }

  /**
   * Gets mark elapsed time.
   *
   * @param sb the sb
   * @return the mark elapsed time
   */
  public String getMarkElapsedTime(StringBuilder sb) {
    calcElapsedTime(markDelta, sb);
    return sb.toString();
  }

  /**
   * Gets total rate.
   *
   * @return the total rate
   */
  public String getTotalRate() {
    StringBuilder sb = new StringBuilder();
    return getTotalRate(sb);
  }

  /**
   * Gets total rate.
   *
   * @param sb the sb
   * @return the total rate
   */
  public String getTotalRate(StringBuilder sb) {
    calcRate(markTime - initTime, totalInterval, sb);
    return sb.toString();
  }

  /**
   * Gets mark rate.
   *
   * @return the mark rate
   */
  public String getMarkRate() {
    StringBuilder sb = new StringBuilder();
    return getMarkRate(sb);
  }

  /**
   * Gets mark rate.
   *
   * @param sb the sb
   * @return the mark rate
   */
  public String getMarkRate(StringBuilder sb) {
    calcRate(markDelta, markInterval, sb);
    return sb.toString();
  }

  public String mark(long interval) {
    return mark(System.currentTimeMillis(), interval);
  }

  public String mark(long now, long interval) {

    long lastmarkTime = markTime;
    markTime = now;
    markDelta = markTime - lastmarkTime;
    markInterval = interval;
    totalInterval = totalInterval + interval;
    boolean isSplit = (markInterval != totalInterval);
    boolean gotInterval = (interval >= 0);

    StringBuilder sb = new StringBuilder();

    if (gotInterval) {
      sb.append(totalInterval);
      sb.append(" in ");
    }

    getTotalElapsedTime(sb);

    if (gotInterval) {
      sb.append(" @ ");
      getTotalRate(sb);
      sb.append("/sec");
    }

    if (isSplit) {
      sb.append(" (last ");
      if (gotInterval) {
        sb.append(interval);
        sb.append(" in ");
      }
      getMarkElapsedTime(sb);
      if (gotInterval) {
        sb.append(" @ ");
        getMarkRate(sb);
        sb.append("/sec");
      }
      sb.append(")");
    }

    lastmark = sb.toString();
    return lastmark;
  }

  public String mark() {
    return mark(-1);
  }

  public static void calcElapsedTime(long initialDelta, StringBuilder sb) {

    long delta = initialDelta;
    long hours = delta / 3600000;
    delta = delta - (hours * 3600000);
    long mins = delta / 60000;
    delta = delta - (mins * 60000);
    long secs = delta / 1000;
    delta = delta - (secs * 1000);

    sb.append(hours);
    sb.append(":");
    if (mins < 10) {
      sb.append("0");
    }
    sb.append(mins);
    sb.append(":");
    if (secs < 10) {
      sb.append("0");
    }
    sb.append(secs);
    sb.append(".");
    if (delta >= 10) {
      sb.append("0");
    } else {
      sb.append("00");
    }
    sb.append(delta);
  }

  public static void calcRate(long delta, long count, StringBuilder sb) {
    float ratePerSec = ((float) delta) / (float) 1000;
    if (ratePerSec > 0.0f) {
      ratePerSec = ((float) count / ratePerSec);
    } else {
      ratePerSec = 0.0f;
    }
    sb.append((int) ratePerSec);
  }

  public String toString() {
    return lastmark;
  }

  /**
   * Gets init time.
   *
   * @return the init time
   */
  public long getInitTime() {
    return initTime;
  }

  /**
   * Gets lastmark.
   *
   * @return the lastmark
   */
  public String getLastmark() {
    return lastmark;
  }

  /**
   * Gets mark delta.
   *
   * @return the mark delta
   */
  public long getMarkDelta() {
    return markDelta;
  }

  /**
   * Gets mark interval.
   *
   * @return the mark interval
   */
  public long getMarkInterval() {
    return markInterval;
  }

  /**
   * Gets mark time.
   *
   * @return the mark time
   */
  public long getMarkTime() {
    return markTime;
  }

  /**
   * Gets total interval.
   *
   * @return the total interval
   */
  public long getTotalInterval() {
    return totalInterval;
  }

  public static SW time(Runnable run) {
    SW sw = new SW();
    run.run();
    sw.mark();
    return sw;
  }

}
