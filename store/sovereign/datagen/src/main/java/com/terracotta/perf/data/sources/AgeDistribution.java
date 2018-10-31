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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Supports weight-based generation of ages.
 * <p>
 * Use {@link Builder} to construct a series of weighted age ranges (as from US Census data)
 * and generate an {@code AgeDistribution} instance.  Use {@link #getAge(Random)} to generate
 * a weighted, pseudo-random age.
 *
 * @see Builder
 *
 * @author Clifford W. Johnson
 */
public final class AgeDistribution {
  private final List<Range> ageRanges;
  private final int lowestAge;
  private final int highestAge;

  private AgeDistribution(final List<Range> ageRanges, final int lowestAge, final int highestAge) {
    this.ageRanges = Collections.unmodifiableList(ageRanges);
    this.lowestAge = lowestAge;
    this.highestAge = highestAge;
  }

  /**
   * Gets an age using the weightings assigned to each age range.
   *
   * @param rnd the {@code Random} instance to use
   *
   * @return the weighted age
   */
  public int getAge(final Random rnd) {
    final float choice = 100.0F * rnd.nextFloat();
    for (final Range range : this.ageRanges) {
      if (choice <= range.cumulativeNormalizedPercent) {
        return range.ageRange[0] + rnd.nextInt(1 + range.ageRange[1] - range.ageRange[0]);
      }
    }
    return this.ageRanges.get(this.ageRanges.size() - 1).ageRange[1];
  }

  /**
   * Returns the lowest age specified in the configured age groups.
   *
   * @return the lowest configured age
   */
  public int getLowestAge() {
    return this.lowestAge;
  }

  /**
   * Returns the highest age specified in the configured age groups.
   *
   * @return the highest configured age
   */
  @SuppressWarnings("unused")
  public int getHighestAge() {
    return this.highestAge;
  }

  /**
   * Builder used to construct an {@link AgeDistribution} instance.
   * <p>
   * The following distribution
   * <pre>{@code
   * final AgeDistribution ageDistribution = new AgeDistribution.Builder()
   *         .range(0, 4, 6.8F)
   *         .range(5, 9, 7.3F)
   *         .build();
   * }</pre>
   * specifies a distribution that does cover 100% of the population.
   */
  public static final class Builder {
    private final List<Range> ranges = new ArrayList<Range>();
    float cumulativePercent = 0.0f;
    int lowestAge = Integer.MAX_VALUE;
    int highestAge = Integer.MIN_VALUE;

    /**
     * Species a weighted age range.  The {@code range} methods must define age ranges
     * in ascending order by age.  While it is generally expected that the cumulative
     * weights (across ranges) will be less than or equal to 100.0%, the {@link #build()}
     * method adjusts the weights so the cumulative total is 100.0%.
     *
     * @param lowerAge the lowest age in this group; the value must be higher than the
     *                 {@code upperAge} of the previous group
     * @param upperAge the highest age in this group; the value must be greater than or
     *                 equal to {@code lowerAge}
     * @param weight the weight for this group expressed as a percentage; values are
     *               expected to range from 0.0 to 100.0 percent
     *
     * @return this {@code Builder}
     */
    public Builder range(final int lowerAge, final int upperAge, final float weight) {
      this.ranges.add(new Range(new int[] {lowerAge, upperAge}, weight));
      this.cumulativePercent += weight;
      return this;
    }

    /**
     * Validates the {@link #range(int, int, float) range} specifications, adjusts the weights,
     * and creates a new {@link AgeDistribution} instance.  This method adjusts the weight of
     * each {@code range} specification so the total across range specifications is 100.0%.
     *
     * @return a new {@code AgeDistribution}
     */
    public AgeDistribution build() {
      this.lowestAge = this.ranges.get(0).ageRange[0];

      float cumulativeNormalizedPercent = 0.0F;
      int lastHighestAge = Integer.MIN_VALUE;
      for (final Range range : this.ranges) {
        if (lastHighestAge >= range.ageRange[0] || range.ageRange[1] < range.ageRange[0]) {
          throw new IllegalStateException("range specifications not ordered");
        }
        lastHighestAge = range.ageRange[1];
        cumulativeNormalizedPercent += range.rangePercent * 100.0f / this.cumulativePercent;
        range.cumulativeNormalizedPercent = cumulativeNormalizedPercent;
      }
      this.highestAge = lastHighestAge;

      return new AgeDistribution(this.ranges, this.lowestAge, this.highestAge);
    }
  }

  /**
   * Describes a weighted age range.
   */
  private static final class Range {
    final int[] ageRange;
    final float rangePercent;
    float cumulativeNormalizedPercent;

    private Range(final int[] ageRange, final float rangePercent) {
      this.ageRange = ageRange;
      this.rangePercent = rangePercent;
    }

    @Override
    public String toString() {
      return "Range{" +
          "ageRange=" + Arrays.toString(ageRange) +
          ", rangePercent=" + rangePercent +
          ", cumulativeNormalizedPercent=" + cumulativeNormalizedPercent +
          '}';
    }
  }
}
