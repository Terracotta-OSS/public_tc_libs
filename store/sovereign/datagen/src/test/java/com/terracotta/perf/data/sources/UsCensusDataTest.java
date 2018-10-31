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

import org.junit.Assert;
import org.junit.Test;

import com.terracotta.perf.DiscreteStats;
import com.terracotta.perf.data.Gender;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.fail;

/**
 * Performs validation and operation tests for {@link UsCensusData}.
 *
 * @author Clifford W. Johnson
 */
public class UsCensusDataTest {

  /**
   * Ensures that the {@link UsCensusData#chooseGender(Random)} method
   * works as advertised.
   */
  @Test
  public void testChooseGender() throws Exception {
    final int samples = 1000000;

    final Field maleGenderPercentageField = UsCensusData.class.getDeclaredField("MALE_GENDER_PERCENTAGE");
    maleGenderPercentageField.setAccessible(true);
    final float maleGenderPercentage = (Float)maleGenderPercentageField.get(null);

    final Random rnd = new Random(1L);

    final DiscreteStats<Gender> genderStats = new DiscreteStats<Gender>();
    for (int i = 0; i < samples; i++) {
      genderStats.collect(UsCensusData.chooseGender(rnd));
    }

    final Map<Gender, DiscreteStats.Observation<Gender>> genderObservationMap = genderStats.getObservationMap();
    assertThat(genderObservationMap.size(), is(Gender.values().length));
    assertThat((double)genderObservationMap.get(Gender.MALE).getDistribution(),
        is(closeTo((double)maleGenderPercentage, 0.1)));
  }


  /**
   * Ensures that the {@link UsCensusData#chooseEducationLevel(Random)} method
   * works as advertised.
   */
  @Test
  public void testChooseEducationLevel() throws Exception {
    final int samples = 1000000;
    final Random rnd = new Random(1L);

    final DiscreteStats<Integer> educationLevelStats = new DiscreteStats<Integer>();
    for (int i = 0; i < samples; i++) {
      educationLevelStats.collect(UsCensusData.chooseEducationLevel(rnd));
    }

    final List<WeightedValue<Integer>> educationLevelDistribution = UsCensusData.getEducationLevelDistribution();

    final List<DiscreteStats.Observation<Integer>> observations = educationLevelStats.getObservations();
    assertThat(observations.size(), is(educationLevelDistribution.size()));

    outer:
    for (final DiscreteStats.Observation<Integer> observation : observations) {
      final int educationLevel = observation.getValue();
      for (final WeightedValue<Integer> weightedValue : educationLevelDistribution) {
        if (educationLevel == weightedValue.getValue()) {
          assertThat((double)observation.getDistribution(), is(closeTo(weightedValue.getWeight(), 0.1)));
          continue outer;
        }
      }
      fail("Unrecognized education level - " + educationLevel);
    }
  }

  /**
   * Tests the structure returned by {@link UsCensusData#getUsSurnames()} and verifies
   * operation of {@link UsCensusData#chooseSurname(Random)}.
   */
  @Test
  public void testChooseSurname() throws Exception {
    verifyNameChooser(
        new NameChooser() {
          @Override
          public String call(final Random rnd) {
            return UsCensusData.chooseSurname(rnd);
          }
        },
        UsCensusData.getUsSurnames());
  }

  /**
   * Tests the structure returned by {@link UsCensusData#getUsFemaleNames()} and verifies
   * operation of {@link UsCensusData#chooseFemaleName(Random)}.
   */
  @Test
  public void testChooseFemaleName() throws Exception {
    verifyNameChooser(
        new NameChooser() {
          @Override
          public String call(final Random rnd) {
            return UsCensusData.chooseFemaleName(rnd);
          }
        },
        UsCensusData.getUsFemaleNames());
  }

  /**
   * Tests the structure returned by {@link UsCensusData#getUsMaleNames()} and verifies
   * operation of {@link UsCensusData#chooseMaleName(Random)}.
   */
  @Test
  public void testChooseMaleName() throws Exception {
    verifyNameChooser(
        new NameChooser() {
          @Override
          public String call(final Random rnd) {
            return UsCensusData.chooseMaleName(rnd);
          }
        },
        UsCensusData.getUsMaleNames());
  }

  /**
   * Verifies a name chooser method against a US Census names map.
   *
   * @param nameChooser the name chooser to verify
   * @param namesMap the map against which {@code nameChooser} is verified
   */
  private void verifyNameChooser(final NameChooser nameChooser, final NavigableMap<Float, List<UsCensusData.CensusName>> namesMap) {
    final int samples = 1000000;

    DiscreteStats<String> nameStats = new DiscreteStats<String>();
    int expectedNameCount = 0;
    float highestCumulativeDistribution = Float.MIN_VALUE;
    for (final List<UsCensusData.CensusName> names : namesMap.values()) {
      for (final UsCensusData.CensusName name : names) {
        nameStats.collect(name.getName());
        expectedNameCount++;
        highestCumulativeDistribution = Math.max(highestCumulativeDistribution, name.getCumulativeDistribution());
      }
    }
    assertThat(nameStats.getObservations().size(), is(equalTo(expectedNameCount)));
    assertThat(highestCumulativeDistribution, is(lessThanOrEqualTo(100.0F)));
    assertThat(highestCumulativeDistribution, is(equalTo(namesMap.lastEntry().getKey())));

    final Random rnd = new Random(1L);

    nameStats = new DiscreteStats<String>();
    for (int i = 0; i < samples; i++) {
      final String name = nameChooser.call(rnd);
      nameStats.collect(name);
    }

    final List<DiscreteStats.Observation<String>> nameStatsObservations = nameStats.getObservations();

    final DiscreteStats.Observation<String> observedMostFrequentName = nameStatsObservations.get(0);
    final UsCensusData.CensusName expectedMostFrequentName = namesMap.firstEntry().getValue().get(0);

    // Assert that at least 80% of the names are generated
    Assert.assertThat(nameStatsObservations.size(),
        is(both(greaterThanOrEqualTo(expectedNameCount * 80 / 100)).and(lessThanOrEqualTo(expectedNameCount))));

    Assert.assertThat(observedMostFrequentName.getValue(), is(equalTo(expectedMostFrequentName.getName())));

    // Assert that the most frequent name appears within 0.1% of its expectation
    final double adjustedObservedDistribution = observedMostFrequentName.getDistribution() * highestCumulativeDistribution / 100;
    Assert.assertThat(adjustedObservedDistribution, is(closeTo(expectedMostFrequentName.getDistribution(), 0.1)));
  }

  private interface NameChooser {
    String call(final Random rnd);
  }
}
