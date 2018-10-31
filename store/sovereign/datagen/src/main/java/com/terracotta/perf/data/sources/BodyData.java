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

import com.terracotta.perf.data.Gender;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/**
 * Provides access to body-related data.
 * <p>
 * The height and BMI data are from the Centers for Disease Control and Prevention (CDC)
 * National Center for Health Statistics (NCHS) &gt;Publications and Information Products &gt; Vital and Health Statistics Series
 * at <a href="http://www.cdc.gov/nchs/products/series/series11.htm">
 *   Series 11. Data From the National Health Examination Survey, the National Health and Nutrition Examination Surveys,
 *   and the Hispanic Health and Nutrition Examination Survey</a>.
 * The specific publication is <i><a href="http://www.cdc.gov/nchs/data/series/sr_11/sr11_252.pdf">
 *   No. 252. Anthropometric Reference Data for Children and Adults: United States, 2007–2010</a></i>.
 * <p>
 * The method used for extending the percentile charts to include 0.0 and 100.0 ranges artificially limits
 * the extremes.  For example, the maximum height for a female is calculated at about 5.8 feet (or about 5'9") --
 * clearly there are much taller women in the US population.
 *
 * @author Clifford W. Johnson
 */
public final class BodyData {

  /**
   * Height data for females aged 20 and over; from table 10 in
   * <i>Anthropometric Reference Data for Children and Adults: United States, 2007–2010</i>.
   * The heights listed in the table are expressed in inches and converted to feet for this
   * table.
   */
  private static final PercentileData FEMALE_HEIGHT =
      new PercentileData(Gender.FEMALE, 5791, inFeet(63.8F),
          new PercentileBuilder()
              .percentile(5, inFeet(59.3F))
              .percentile(10, inFeet(60.3F))
              .percentile(15, inFeet(60.9F))
              .percentile(25, inFeet(61.9F))
              .percentile(50, inFeet(63.8F))
              .percentile(75, inFeet(65.7F))
              .percentile(85, inFeet(66.6F))
              .percentile(90, inFeet(67.3F))
              .percentile(95, inFeet(68.4F))
              .build());

  /**
   * Height data for males aged 20 and over; from table 12 in
   * <i>Anthropometric Reference Data for Children and Adults: United States, 2007–2010</i>.
   * The heights listed in the table are expressed in inches and converted to feet for this
   * table.
   */
  private static final PercentileData MALE_HEIGHT =
      new PercentileData(Gender.MALE, 5647, inFeet(69.3F),
          new PercentileBuilder()
              .percentile(5, inFeet(64.3F))
              .percentile(10, inFeet(65.4F))
              .percentile(15, inFeet(66.2F))
              .percentile(25, inFeet(67.3F))
              .percentile(50, inFeet(69.3F))
              .percentile(75, inFeet(71.2F))
              .percentile(85, inFeet(72.3F))
              .percentile(90, inFeet(73.0F))
              .percentile(95, inFeet(74.1F))
              .build());

  /**
   * BMI data for females aged 20 and over; from Table 14 in
   * <i>Anthropometric Reference Data for Children and Adults: United States, 2007–2010</i>.
   */
  private static final PercentileData FEMALE_BMI =
      new PercentileData(Gender.FEMALE, 5841, 28.7F,
          new PercentileBuilder()
              .percentile(5, 19.5F)
              .percentile(10, 20.7F)
              .percentile(15, 21.7F)
              .percentile(25, 23.3F)
              .percentile(50, 27.3F)
              .percentile(75, 32.5F)
              .percentile(85, 36.1F)
              .percentile(90, 38.2F)
              .percentile(95, 42.0F)
              .build());

  /**
   * BMI data for males aged 20 and over; from Table 15 in
   * <i>Anthropometric Reference Data for Children and Adults: United States, 2007–2010</i>.
   */
  private static final PercentileData MALE_BMI =
      new PercentileData(Gender.MALE, 5635, 28.6F,
          new PercentileBuilder()
              .percentile(5, 20.7F)
              .percentile(10, 22.2F)
              .percentile(15, 23.2F)
              .percentile(25, 24.7F)
              .percentile(50, 27.8F)
              .percentile(75, 31.5F)
              .percentile(85, 33.9F)
              .percentile(90, 35.8F)
              .percentile(95, 39.2F)
              .build());

  /**
   * Selects a gender-specific height value.
   *
   * @param rnd the {@code Random} instance to use for the selection
   * @param gender the gender used to determine the height distribution
   *
   * @return a height value calculated from the height distribution obtained from the CDC
   */
  public static float chooseHeight(final Random rnd, final Gender gender) {
    if (gender == null) {
      throw new NullPointerException("gender");
    }
    return (gender == Gender.FEMALE ? FEMALE_HEIGHT : MALE_HEIGHT).getScaledValue(rnd);
  }

  /**
   * Gets the population mean for the gender-specific height distribution obtained from the CDC.
   *
   * @param gender the gender to use to determine the height distribution
   *
   * @return the population mean for the height distribution for {@code gender}
   */
  public static float getHeightMean(final Gender gender) {
    if (gender == null) {
      throw new NullPointerException("gender");
    }
    return (gender == Gender.FEMALE ? FEMALE_HEIGHT : MALE_HEIGHT).getPopulationMean();
  }

  /**
   * Generates a weight using a Body Mass Index (BMI) calculation with the height.
   * The BMI is chosen using {@link BodyData#chooseBMI(Random, Gender) BodyData.chooseBMI}.
   *
   * @param rnd the {@code Random} instance to use for BMI selection
   * @param gender the gender used to bias the computation
   * @param height the height on which the calculation is based
   *
   * @return the weight, in pounds
   */
  public static float calculateWeight(final Random rnd, final Gender gender, final float height) {
    float heightInInches = height * 12f;
    float bmi = chooseBMI(rnd, gender);
    return (bmi * heightInInches * heightInInches) / 703;
  }

  /**
   * Selects a gender-specific BMI value.
   *
   * @param rnd the {@code Random} instance to use for the selection
   * @param gender the gender used to determine the BMI distribution
   *
   * @return a BMI value calculated from the BMI distribution obtained from the CDC
   */
  public static float chooseBMI(final Random rnd, final Gender gender) {
    if (gender == null) {
      throw new NullPointerException("gender");
    }
    return (gender == Gender.FEMALE ? FEMALE_BMI : MALE_BMI).getScaledValue(rnd);
  }

  /**
   * Gets the population mean for the gender-specific BMI distribution obtained from the CDC.
   *
   * @param gender the gender to use to determine the BMI distribution
   *
   * @return the population mean for the BMI distribution for {@code gender}
   */
  static float getBMIMean(final Gender gender) {
    if (gender == null) {
      throw new NullPointerException("gender");
    }
    return (gender == Gender.FEMALE ? FEMALE_BMI : MALE_BMI).getPopulationMean();
  }

  private static float inFeet(final float inches) {
    return inches / 12;
  }

  /**
   * Describes the gender-based percentile-distributed data.
   */
  @SuppressWarnings("unused")
  private static final class PercentileData {
    private final Gender gender;
    private final int observations;
    private final float populationMean;
    private final NavigableMap<Float, Percentile> percentileMap;

    private PercentileData(final Gender gender, final int observations, final float populationMean, final NavigableMap<Float, Percentile> percentileMap) {
      this.gender = gender;
      this.observations = observations;
      this.populationMean = populationMean;
      this.percentileMap = percentileMap;
    }

    private float getPopulationMean() {
      return populationMean;
    }

    /**
     * Calculates a scaled value extrapolated from the encapsulated percentile distribution.
     * Scaled values are calculated from the bounding percentile distribution values using a
     * linear approximation.
     *
     * @param rnd the {@code Random} instance to use for upper bounds choice
     *
     * @return a calculated scaled value
     */
    private float getScaledValue(final Random rnd) {
      final float choice = 100.0F * rnd.nextFloat();
      final Map.Entry<Float, Percentile> ceilingEntry = this.percentileMap.ceilingEntry(choice);
      float scaledValue = ceilingEntry.getValue().getValue();
      final Map.Entry<Float, Percentile> lowerBoundEntry = this.percentileMap.lowerEntry(ceilingEntry.getKey());
      if (lowerBoundEntry != null) {
        final float slope = (ceilingEntry.getValue().getValue() - lowerBoundEntry.getValue().getValue())
            / (ceilingEntry.getValue().getPercentile() - lowerBoundEntry.getValue().getPercentile());
        scaledValue -= slope * (ceilingEntry.getValue().getPercentile() - choice);
      }
      return scaledValue;
    }
  }

  /**
   * Maps a percentile-scaled value.
   */
  private static class Percentile {
    private final float percentile;
    private final float value;

    public Percentile(final float percentile, final float value) {
      this.percentile = percentile;
      this.value = value;
    }

    public final float getPercentile() {
      return percentile;
    }

    public final float getValue() {
      return value;
    }
  }

  /**
   * Constructs a {@code NavigableMap} for a collection of percentile-distributed {@code float} values.
   * The {@link #build()} method extrapolates entries for the 0.0 and 100.0 percentiles if necessary.
   */
  private static final class PercentileBuilder {
    private final TreeMap<Float, Percentile> percentileMap = new TreeMap<Float, Percentile>();

    private PercentileBuilder percentile(final float percentile, final float value) {
       this.percentileMap.put(percentile, new Percentile(percentile, value));
      return this;
    }

    private NavigableMap<Float, Percentile> build() {

      // TODO: Determine better calculation for 0.0 and 100.0 values
      /*
       * Add upper and lower tails to the values, if necessary
       */
      final Map.Entry<Float, Percentile> firstEntry = this.percentileMap.firstEntry();
      final float firstKey = firstEntry.getKey();
      if (firstKey > 0.0F) {
        final Map.Entry<Float, Percentile> nextEntry = this.percentileMap.higherEntry(firstKey);
        final float slope = (nextEntry.getValue().getValue() - firstEntry.getValue().getValue())
            / (nextEntry.getValue().getPercentile() - firstEntry.getValue().getPercentile());
        float lowerValue = firstEntry.getValue().getValue() - (firstEntry.getValue().getPercentile() * slope);
        this.percentile(0, lowerValue);
      }

      final Map.Entry<Float, Percentile> lastEntry = this.percentileMap.lastEntry();
      final float lastKey = lastEntry.getKey();
      if (lastKey < 100.0F) {
        final Map.Entry<Float, Percentile> prevEntry = this.percentileMap.lowerEntry(lastKey);
        final float slope = (lastEntry.getValue().getValue() - prevEntry.getValue().getValue())
            / (lastEntry.getValue().getPercentile() - prevEntry.getValue().getPercentile());
        float upperValue = lastEntry.getValue().getValue() + (100.0F - lastEntry.getValue().getPercentile()) * slope;
        this.percentile(100, upperValue);
      }

      return this.percentileMap;
    }
  }
}
