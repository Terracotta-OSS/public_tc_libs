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
package com.terracotta.perf.data.sources;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a base on which a list of weighted values can be built.
 *
 * @param <V> the type of the weighted value
 *
 * @author Clifford W. Johnson
 *
 * @see Builder
 */
class WeightedValue<V> implements Comparable<WeightedValue<V>> {
  private final V value;
  private final float weight;

  /**
   * The sum of the weights of this {@code WeightedValue} and all {@code WeightedValue} instances
   * added to the collection (via {@link Builder#weight(Object, float) Builder.weight} before
   * this instance.  The value set by {@link Builder#build() Builder.build} and is expressed as
   * a percentage with values ranging from 0.0 to 100.0.
   */
  private float normalizedCumulativeWeight;

  protected WeightedValue(final V value, final float weight) {
    this.value = value;
    this.weight = weight;
  }

  /**
   * Gets the value from this {@code WeightedValue}.
   *
   * @return the value
   */
  public final V getValue() {
    return value;
  }

  /**
   * Gets the weight assigned to this {@code WeightedValue} instance.
   *
   * @return the weight of this {@code WeightedValue} instance
   */
  public final float getWeight() {
    return weight;
  }

  /**
   * Gets the weight of this {@code WeightedValue} relative to the other {@code WeightedValue}
   * instances added to the {@link Builder} constructing this instance.
   *
   * @return the cumulative weight of this instance expressed as a percentage; values are in the
   *    range 0.0 to 100.0
   */
  public final float getCumulativeWeight() {
    return normalizedCumulativeWeight;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WeightedValue<?> that = (WeightedValue<?>)o;
    return Float.compare(that.weight, this.weight) == 0 && this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    int result = this.value.hashCode();
    result = 31 * result + (this.weight != +0.0f ? Float.floatToIntBits(this.weight) : 0);
    return result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" +
        "value=" + value +
        ", weight=" + weight +
        ", cumulativeWeight=" + normalizedCumulativeWeight +
        '}';
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation provides a {@code WeightedValue} ordering by weight.
   *
   * @param that {@inheritDoc}
   * @return {@inheritDoc}
   */
  @Override
  public int compareTo(@SuppressWarnings("NullableProblems") final WeightedValue<V> that) {
    return Float.compare(this.weight, that.weight);
  }

  /**
   * Supports building a list of {@link WeightedValue} instances internally managing
   * cumulative weighting.  The weightings calculated by this class are expressed in percentage values
   * ranging from 0.0 to 100.0.
   *
   * @param <V> the type of the weighted value
   * @param <W> the {@code WeightedValue} subclass type
   */
  public abstract static class Builder<V, W extends WeightedValue<V>> {
    private float totalCumulativeWeight = 0.0F;
    private final List<W> weightedValues = new ArrayList<W>();

    /**
     * Add a new weighted value to the collection under construction.
     *
     * @param value the value to add
     * @param weight the weight assigned to the value
     *
     * @return this {@code Builder}
     */
    public Builder<V, W> weight(final V value, final float weight) {
      this.totalCumulativeWeight += weight;
      this.weightedValues.add(this.newInstance(value, weight));
      return this;
    }

    /**
     * Returns a list of {@code WeightedValue} instances as specified by the
     * {@link #weight(Object, float) weight} specifications.  This method normalizes the
     * weights so the total of the weights over the values is 100.0%.
     *
     * @return the list of {@code WeightedValue} instances built
     */
    public final List<W> build() {
      float runningCumulativeWeight = 0.0F;
      for (final WeightedValue<V> weightedValue : this.weightedValues) {
        runningCumulativeWeight += weightedValue.weight;
        weightedValue.normalizedCumulativeWeight = runningCumulativeWeight * 100.0F / this.totalCumulativeWeight;
      }

      return this.weightedValues;
    }

    /**
     * Constructs a new {@code WeightedValue} instance from the value and (instance) weight supplied.
     *
     * @param value the value for the new {@code WeightedValue}
     * @param weight the weight assigned to the new {@code WeightedValue}
     *
     * @return a new {@code WeightedValue} instance
     */
    protected abstract W newInstance(final V value, final float weight);
  }
}
