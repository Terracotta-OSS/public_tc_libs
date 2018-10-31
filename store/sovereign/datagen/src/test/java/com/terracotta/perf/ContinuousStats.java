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

package com.terracotta.perf;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic statistics class for continuous variables.
 *
 * @author Clifford W. Johnson
 */
// TODO: Convert to use streaming algorithm for variance/stdDev and mean
@SuppressWarnings("unused")
public abstract class ContinuousStats<S extends ContinuousStats<S>> {
  protected volatile boolean computed = false;
  protected double avg;
  protected double variance;
  protected double stdDev;

  public abstract S compute();

  public abstract long getCount();

  public double getAvg() {
    this.compute();
    return avg;
  }

  public double getVariance() {
    this.compute();
    return variance;
  }

  public double getStdDev() {
    this.compute();
    return stdDev;
  }

  /**
   * Supports calculating statistics on {@code long} values.
   */
  public static final class LongStats extends ContinuousStats<LongStats> {
    private final List<Long> observations = new ArrayList<Long>();
    private long min;
    private long max;
    private BigInteger sum;

    public void collect(long observation) {
      if (this.computed) {
        throw new IllegalStateException();
      }
      this.observations.add(observation);
    }

    @Override
    public LongStats compute() {
      if (computed) {
        return this;
      }
      this.computed = true;

      final long count = this.observations.size();
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      BigInteger sum = BigInteger.ZERO;
      for (final long observation : this.observations) {
        min = Math.min(observation, min);
        max = Math.max(observation, max);
        sum = sum.add(BigInteger.valueOf(observation));
      }
      this.min = min;
      this.max = max;
      this.sum = sum;
      this.avg = this.sum.doubleValue() / this.observations.size();

      double sumOfSquareDeviations = 0;
      for (final long observation : observations) {
        sumOfSquareDeviations += Math.pow(observation - avg, 2.0D);
      }
      this.variance = sumOfSquareDeviations / count;
      this.stdDev = Math.sqrt(this.variance);

      return this;
    }

    @Override
    public long getCount() {
      this.compute();
      return this.observations.size();
    }

    public long getMin() {
      this.compute();
      return min;
    }

    public long getMax() {
      this.compute();
      return max;
    }

    public BigInteger getSum() {
      this.compute();
      return sum;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("LongStats{");
      if (this.computed) {
        sb.append("count=").append(observations.size());
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", sum=").append(sum);
        sb.append(", avg=").append(avg);
        sb.append(", variance=").append(variance);
        sb.append(", stdDev=").append(stdDev);
      } else {
        sb.append("computed=").append(computed);
        sb.append(", count=").append(observations.size());
      }
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Supports calculating statistics on {@code double} values.
   */
  @SuppressWarnings("unused")
  public static final class DoubleStats extends ContinuousStats<DoubleStats> {
    private final List<Double> observations = new ArrayList<Double>();
    private double min;
    private double max;
    private double sum;

    public void collect(double observation) {
      if (this.computed) {
        throw new IllegalStateException();
      }
      this.observations.add(observation);
    }

    @Override
    public DoubleStats compute() {
      if (computed) {
        return this;
      }
      this.computed = true;

      final long count = this.observations.size();
      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;
      double sum = 0L;
      for (final double observation : this.observations) {
        min = Math.min(observation, min);
        max = Math.max(observation, max);
        sum += observation;
      }
      this.min = min;
      this.max = max;
      this.sum = sum;
      this.avg = this.sum / this.observations.size();

      double sumOfSquareDeviations = 0;
      for (final double observation : observations) {
        sumOfSquareDeviations += Math.pow(observation - avg, 2.0D);
      }
      this.variance = sumOfSquareDeviations / count;
      this.stdDev = Math.sqrt(this.variance);

      return this;
    }

    @Override
    public long getCount() {
      this.compute();
      return this.observations.size();
    }

    public double getMin() {
      this.compute();
      return min;
    }

    public double getMax() {
      this.compute();
      return max;
    }

    public double getSum() {
      this.compute();
      return sum;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("DoubleStats{");
      if (this.computed) {
        sb.append("count=").append(observations.size());
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", sum=").append(sum);
        sb.append(", avg=").append(avg);
        sb.append(", variance=").append(variance);
        sb.append(", stdDev=").append(stdDev);
      } else {
        sb.append("computed=").append(computed);
        sb.append(", count=").append(observations.size());
      }
      sb.append('}');
      return sb.toString();
    }
  }
}
