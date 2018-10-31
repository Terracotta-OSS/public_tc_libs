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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Supports calculation of statistics over a set of discrete observations.
 *
 * @author Clifford W. Johnson
 */
public final class DiscreteStats<V> {
  private volatile boolean computed = false;
  private final Map<V, Observation<V>> observationMap = new LinkedHashMap<V, Observation<V>>();
  private long totalObservations = 0L;

  /**
   * Make an observation of a discrete value.  Each distinct value
   * (according to {@link Object#equals(Object) equals}) is recorded in an
   * internal map and the number of times the value is observed is counted.
   *
   * @param value the value to collect for observation.
   */
  public void collect(final V value) {
    if (this.computed) {
      throw new IllegalStateException();
    }

    Observation<V> observation = this.observationMap.get(value);
    if (observation == null) {
      observation = new Observation<V>(value);
      this.observationMap.put(value, observation);
    }
    observation.count();
    this.totalObservations++;
  }

  /**
   * Computes the statistics over the observations made by {@link #collect(Object)}.
   * Calling this method closes out observation collection.
   *
   * @return this {@code DiscreteStats} instance
   */
  public DiscreteStats<V> compute() {
    if (this.computed) {
      return this;
    }
    this.computed = true;

    for (final Observation<V> observation : this.observationMap.values()) {
      observation.distribution = 100.0F * observation.count / this.totalObservations;
    }

    return this;
  }

  /**
   * Gets the total number of calls to {@link #collect(Object)}.
   * Calling this method closes out observation collection.
   *
   * @return the total number of calls to {@link #collect(Object)}
   */
  public long getTotalObservations() {
    this.compute();
    return totalObservations;
  }

  /**
   * Gets the map of values presented to the {@link #collect(Object) collect} method to the
   * {@link Observation} instance used to track that value.
   * Calling this method closes out observation collection.
   *
   * @return the observation map
   */
  public Map<V, Observation<V>> getObservationMap() {
    this.compute();
    return Collections.unmodifiableMap(this.observationMap);
  }

  /**
   * Gets a list of observations sorted by decreasing observation count.
   * Calling this method closes out observation collection.
   *
   * @return an list of {@link Observation} instances order by decreasing observation count
   */
  public List<Observation<V>> getObservations() {
    this.compute();

    final List<Observation<V>> observations = new ArrayList<Observation<V>>(this.observationMap.values());
    /*
     * Sort by decreasing observation count.
     */
    Collections.sort(observations, new Comparator<Observation<V>>() {
      @Override
      public int compare(final Observation<V> o1, final Observation<V> o2) {
        return (o1.count == o2.count ? 0 : (o1.count > o2.count ? -1 : 1));
      }
    });

    return observations;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DiscreteStats{");
    if (this.computed) {
      sb.append("totalObservations=").append(totalObservations);
      sb.append(", observationMap=").append(observationMap);
    } else {
      sb.append("computed=").append(computed);
      sb.append(", totalObservations=").append(totalObservations);
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Tracks observations of a discrete value presented to {@link #collect(Object) DiscreteStats.collect}.
   * @param <V> the observed value type
   */
  public static final class Observation<V> {
    private final V value;
    private int count;
    private float distribution;

    private Observation(final V value) {
      this.value = value;
    }

    /**
     * Gets the value associated with this {@code Observation}.
     *
     * @return the observation value
     */
    public V getValue() {
      return value;
    }

    /**
     * Gets the number of time the value associated with this {@code Observation} was
     * presented to {@link #collect(Object) DiscreteStats.collect}.
     *
     * @return the number of times this value was observed
     */
    public int getCount() {
      return count;
    }

    /**
     * The distribution of this value versus all values collected.
     *
     * @return a value between 0.0 and 100.0 indicating this value's distribution
     */
    public float getDistribution() {
      return distribution;
    }

    /**
     * Increments this observation count.
     */
    private void count() {
      this.count++;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Observation{");
      sb.append("value=").append(value);
      sb.append(", count=").append(count);
      sb.append(", distribution=").append(distribution);
      sb.append('}');
      return sb.toString();
    }
  }
}
