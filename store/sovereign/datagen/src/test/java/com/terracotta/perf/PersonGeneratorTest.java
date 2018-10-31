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

import org.junit.Ignore;
import org.junit.Test;

import com.terracotta.perf.ContinuousStats.DoubleStats;
import com.terracotta.perf.ContinuousStats.LongStats;
import com.terracotta.perf.data.EducationLevel;
import com.terracotta.perf.data.Gender;
import com.terracotta.perf.data.PersonGenerator;
import com.terracotta.perf.data.sources.UsCensusData;

import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;
import static org.junit.Assert.assertThat;

/**
 * @author Clifford W. Johnson
 */
public class PersonGeneratorTest {

  private final PersonGenerator.PersonBuilder<Person> builder = new PersonBuilder(true);

  @Test
  public void testPopulationLite() throws Exception {
    final int recordCount = 3000000;
    final Random rnd = new Random(1L);
    final PersonGenerator<Person> generator = new PersonGenerator<Person>(builder, 1024)
        .includeCollege(false)
        .includeHighSchool(false)
        .includeGivenName(false);

    final long startTime = System.nanoTime();
    for (int i = 0; i < recordCount; i++) {
      assertNotNull(generator.generate(rnd, i));
    }
    final long duration = System.nanoTime() - startTime;
    System.out.format("%nGenerated %,d records in %s%n", recordCount, formatNanos(duration));
  }

  @Ignore("Long running test")
  @Test
  public void testPopulationTiming() throws Exception {
    final int recordCount = 10000000;
    final Random rnd = new Random(1L);
    final PersonGenerator<Person> generator = new PersonGenerator<Person>(builder, 1024);

    final long startTime = System.nanoTime();
    for (int i = 0; i < recordCount; i++) {
      assertNotNull(generator.generate(rnd, i));
    }
    final long duration = System.nanoTime() - startTime;
    System.out.format("%nGenerated %,d records in %s%n", recordCount, formatNanos(duration));
  }

  /**
   * Performs a sample population generation.  The output of this test should be
   * visually inspected for sanity.
   *
   * <h3>Note</h3>
   * Distribution tolerance checks in this unit test are sensitive to the
   * number of samples used.  If the number of samples is changed, some assertions
   * may need adjustment.
   */
  @Test
  public void testPopulation() throws Exception {
    final int recordCount = 3000000;
    final Random rnd = new Random(1L);

    final float expectedBirthDatePercentage = 75.0F;
    final float expectedEducationPercentage = 50.0F;
    final float expectedEyeColorPercentage = 25.0F;
    final float expectedGenderPercentage = 100.0F;
    final float expectedLocationPercentage = 80.0F;
    final float expectedHeightPercentage = 30.0F;
    final float expectedTenureDatePercentage = 90.0F;
    final float expectedRawDataPercentage = 10.0F;
    final PersonGenerator<Person> generator = new PersonGenerator<Person>(builder, 1024)
        .setBirthDatePercentage(expectedBirthDatePercentage)
        .setEducationPercentage(expectedEducationPercentage)
        .setEyeColorPercentage(expectedEyeColorPercentage)
        .setGenderPercentage(expectedGenderPercentage)
        .setLocationPercentage(expectedLocationPercentage)
        .setHeightPercentage(expectedHeightPercentage)
        .setTenureDatePercentage(expectedTenureDatePercentage)
        .setRawDataPercentage(expectedRawDataPercentage);

    boolean wyomingUsed = false;
    final LongStats ageStats = new LongStats();
    final LongStats dobStats = new LongStats();
    final LongStats dojStats = new LongStats();
    final DoubleStats femaleHeightStats = new DoubleStats();
    final DoubleStats maleHeightStats = new DoubleStats();
    final DoubleStats femaleWeightStats = new DoubleStats();
    final DoubleStats maleWeightStats = new DoubleStats();
    final DiscreteStats<String> stateStats = new DiscreteStats<String>();
    final DiscreteStats<Gender> genderStats = new DiscreteStats<Gender>();
    final DiscreteStats<Integer> educationLevelStats = new DiscreteStats<Integer>();
    final DiscreteStats<String> surnameStats = new DiscreteStats<String>();
    final DiscreteStats<String> femaleNameStats = new DiscreteStats<String>();
    final DiscreteStats<String> maleNameStats = new DiscreteStats<String>();
    final DiscreteStats<String> highSchoolStats = new DiscreteStats<String>();
    final DiscreteStats<String> collegeStats = new DiscreteStats<String>();
    final DiscreteStats<Boolean> rawDataStats = new DiscreteStats<Boolean>();
    final long startTime = System.nanoTime();
    for (int i = 0; i < recordCount; i++) {
      final Person person = generator.generate(rnd, i);

      final int age = person.getAge();
      if (age != 0) {
        ageStats.collect(age);
      }
      final Date dateOfBirth = person.getDateOfBirth();
      if (dateOfBirth != null) {
        dobStats.collect(dateOfBirth.getTime());
      }
      final Date dateOfJoining = person.getDateOfJoining();
      if (dateOfJoining != null) {
        dojStats.collect(dateOfJoining.getTime());
      }

      final Gender gender = person.getGender();
      if (gender != null) {
        genderStats.collect(gender);
      }

      if (gender != null) {
        final float height = person.getHeight();
        if (height == height) {
          (gender == Gender.FEMALE ? femaleHeightStats : maleHeightStats).collect(height);
        }
        final double weight = person.getWeight();
        if (weight == weight) {
          (gender == Gender.FEMALE ? femaleWeightStats : maleWeightStats).collect(weight);
        }
      }

      if (person.getAddress() != null && person.getAddress().getState() != null) {
        final String state = person.getAddress().getState();
        wyomingUsed = (wyomingUsed | state.equalsIgnoreCase("Wyoming"));
        stateStats.collect(state);
      }

      final int educationLevel = person.getEducationLevel();
      if (educationLevel != EducationLevel.UNKNOWN) {
        educationLevelStats.collect(educationLevel);
      }

      final String surname = person.getSurname();
      if (surname != null) {
        surnameStats.collect(surname);
      }

      final String givenName = person.getGivenName();
      if (givenName != null && gender != null) {
        switch (gender) {
          case FEMALE:
            femaleNameStats.collect(givenName);
            break;
          case MALE:
            maleNameStats.collect(givenName);
            break;
          default:
            fail("Unexpected gender - " + gender);
        }
      }

      final String highSchoolName = person.getHighSchoolName();
      if (highSchoolName != null) {
        highSchoolStats.collect(highSchoolName);
      }

      final String collegeName = person.getCollegeName();
      if (collegeName != null) {
        collegeStats.collect(collegeName);
      }

      rawDataStats.collect(person.getRawData() != null);
    }
    final long duration = System.nanoTime() - startTime;
    System.out.format("%nGenerated %,d records in %s%n", recordCount, formatNanos(duration));

    final double observedBirthDatePercentage = getPercent(recordCount, ageStats);
    System.out.format("Age stats: [%.2f%%] %s%n", observedBirthDatePercentage, ageStats.compute().toString());
    System.out.format("DateOfBirth stats: [%.2f%%] %s%n", getPercent(recordCount, dobStats), dobStats.compute().toString());
    System.out.format("    min=%s; max=%s%n", new Date(dobStats.getMin()), new Date(dobStats.getMax()));
    assertThat(observedBirthDatePercentage, is(closeTo(expectedBirthDatePercentage, 0.1)));

    final double observedTenurePercentage = getPercent(recordCount, dojStats);
    System.out.format("DateOfJoining stats: [%.2f%%] %s%n", observedTenurePercentage, dojStats.compute().toString());
    System.out.format("    min=%s; max=%s%n", new Date(dojStats.getMin()), new Date(dojStats.getMax()));
    assertThat(observedTenurePercentage, is(closeTo(expectedTenureDatePercentage, 0.1)));

    double observedBodyStatsPercentage = 0.0;
    observedBodyStatsPercentage += checkBodyStats(recordCount, Gender.FEMALE, femaleHeightStats, femaleWeightStats);
    observedBodyStatsPercentage += checkBodyStats(recordCount, Gender.MALE, maleHeightStats, maleWeightStats);
    assertThat(observedBodyStatsPercentage, is(closeTo(expectedHeightPercentage, 0.1)));


    final double observedLocationPercentage = getPercent(recordCount, stateStats);
    final List<DiscreteStats.Observation<String>> stateStatsObservations = stateStats.getObservations();
    System.out.format("State stats: [%.2f%%]%n", observedLocationPercentage);
    for (final DiscreteStats.Observation<String> stateObservation : stateStatsObservations) {
      System.out.format("    %20s %, 10d % 6.2f%%%n", stateObservation.getValue(), stateObservation.getCount(), stateObservation.getDistribution());
    }
    assertThat(observedLocationPercentage, is(closeTo(expectedLocationPercentage, 0.1)));

    assertThat(stateStatsObservations.size(), is(equalTo(UsCensusData.getUsStatesByName().size())));

    final List<UsCensusData.StatePopulationWeight> statePopulationDistribution =
        UsCensusData.getStatePopulationDistribution();
    int totalPopulation = 0;
    for (UsCensusData.StatePopulationWeight statePopulation : statePopulationDistribution) {
      totalPopulation += statePopulation.getValue().getPopulation();
    }

    /*
     * Check each state for the proper frequency distribution (within 0.05%)
     */
    observationLoop:
    for (final DiscreteStats.Observation<String> stateObservation : stateStatsObservations) {
      for (final UsCensusData.StatePopulationWeight weightedState : statePopulationDistribution) {
        if (weightedState.getValue().getName().equals(stateObservation.getValue())) {
          assertThat((double)stateObservation.getDistribution(),
              is(closeTo(weightedState.getValue().getPopulation() * 100.0 / totalPopulation, 0.05)));
          break observationLoop;
        }
      }
      fail("Unrecognized state observed - " + stateObservation.getValue());
    }

    final double observedGenderPercentage = getPercent(recordCount, genderStats);
    System.out.format("Gender stats: [%.2f%%] %s%n", observedGenderPercentage, genderStats.compute().toString());
    assertThat(observedGenderPercentage, is(closeTo(expectedGenderPercentage, 0.1)));

    final double observedEducationPercentage = getPercent(recordCount, educationLevelStats);
    System.out.format("EducationLevel stats:%n");
    for (final DiscreteStats.Observation<Integer> observation : educationLevelStats.getObservations()) {
      System.out.format("    % 3d %, 10d % 6.2f%%%n", observation.getValue(), observation.getCount(), observation.getDistribution());
    }
    assertThat(observedEducationPercentage, is(closeTo(expectedEducationPercentage, 0.1)));

    assertThat(educationLevelStats.getObservations().size(), is(equalTo(UsCensusData.getEducationLevelDistribution().size())));

    final double observedSurnamePercentage = getPercent(recordCount, surnameStats);
    final List<DiscreteStats.Observation<String>> surnameStatsObservations = surnameStats.getObservations();
    System.out.format("Surname stats: [%.2f%%] %d unique of %d samples%n",
        observedSurnamePercentage, surnameStatsObservations.size(), surnameStats.getTotalObservations());
    assertThat(observedSurnamePercentage, is(closeTo(100.0, 0.0001)));
    assertNameObservations(UsCensusData.getUsSurnames(), surnameStatsObservations);
    displayNameObservations(surnameStatsObservations);

    final double observedFemaleNamePercentage = getPercent(recordCount, femaleNameStats);
    final List<DiscreteStats.Observation<String>> femaleNameStatsObservations = femaleNameStats.getObservations();
    System.out.format("Female name stats: [%.2f%%] %d unique of %d samples%n",
        observedFemaleNamePercentage, femaleNameStatsObservations.size(), femaleNameStats.getTotalObservations());
    assertNameObservations(UsCensusData.getUsFemaleNames(), femaleNameStatsObservations);
    assertThat(femaleNameStats.getTotalObservations(),
        is(equalTo((long)genderStats.getObservationMap().get(Gender.FEMALE).getCount())));
    displayNameObservations(femaleNameStatsObservations);

    final double observedMaleNamePercentage = getPercent(recordCount, maleNameStats);
    final List<DiscreteStats.Observation<String>> maleNameStatsObservations = maleNameStats.getObservations();
    System.out.format("Male name stats: [%.2f%%] %d unique of %d samples%n",
        observedMaleNamePercentage, maleNameStatsObservations.size(), maleNameStats.getTotalObservations());
    assertNameObservations(UsCensusData.getUsMaleNames(), maleNameStatsObservations);
    assertThat(maleNameStats.getTotalObservations(),
        is(equalTo((long)genderStats.getObservationMap().get(Gender.MALE).getCount())));
    displayNameObservations(maleNameStatsObservations);

    final double observedHighSchoolPercentage = getPercent(recordCount, highSchoolStats);
    final List<DiscreteStats.Observation<String>> highSchoolStatsObservations = highSchoolStats.getObservations();
    System.out.format("High school stats: [%.2f%%] %d unique of %d samples%n",
        observedHighSchoolPercentage, highSchoolStatsObservations.size(),
        highSchoolStats.getTotalObservations());
    // Can't easily assert distribution

    final double observedCollegePercentage = getPercent(recordCount, collegeStats);
    final List<DiscreteStats.Observation<String>> collegeStatsObservations = collegeStats.getObservations();
    System.out.format("Post-secondary school stats: [%.2f%%] %d unique of %d samples%n",
        observedCollegePercentage, collegeStatsObservations.size(), collegeStats.getTotalObservations());
    // Can't easily assert distribution

    final double observedRawDataPercentage = rawDataStats.getObservationMap().get(true).getDistribution();
    assertThat(observedRawDataPercentage, is(closeTo(expectedRawDataPercentage, 0.1)));

    assertThat(wyomingUsed, is(true));
  }

  private double checkBodyStats(final int recordCount,
                                final Gender gender,
                                final DoubleStats heightStats,
                                final DoubleStats weightStats) {
    final double observedHeightPercentage = getPercent(recordCount, heightStats);
    System.out.format("%s Height stats: [%.2f%%] %s%n", gender, observedHeightPercentage, heightStats.compute().toString());
    System.out.format("%s Weight stats: [%.2f%%] %s%n", gender, getPercent(recordCount, weightStats), weightStats.compute().toString());
    return observedHeightPercentage;
  }

  /**
   * Makes assertions for US Census names.
   *
   * @param nameMap the {@link UsCensusData} name collection representing the expected data
   * @param nameStatsObservations the {@code DiscreteStats.Observation} collection to check
   */
  private void assertNameObservations(final NavigableMap<Float, List<UsCensusData.CensusName>> nameMap,
                                      final List<DiscreteStats.Observation<String>> nameStatsObservations) {

    final UsCensusData.CensusName expectedMostFrequentName = nameMap.firstEntry().getValue().get(0);
    final DiscreteStats.Observation<String> observedMostFrequentName = nameStatsObservations.get(0);
    final float highestCumulativeDistribution = nameMap.lastEntry().getKey();
    final double adjustedObservedDistribution = observedMostFrequentName.getDistribution() * highestCumulativeDistribution / 100;

    System.out.format("    %s: observed=%7.5f%% adjusted=%7.5f%% expected=%7.5f%%%n",
        observedMostFrequentName.getValue(), observedMostFrequentName.getDistribution(),
        adjustedObservedDistribution, expectedMostFrequentName.getDistribution());

    int expectedNameCount = 0;
    for (final List<UsCensusData.CensusName> names : nameMap.values()) {
      expectedNameCount += names.size();
    }

    // Assert that at least 90% of the names are generated; may need adjusting if sample size is changed
    assertThat(nameStatsObservations.size(),
        is(both(greaterThanOrEqualTo(expectedNameCount * 90 / 100)).and(lessThanOrEqualTo(expectedNameCount))));

    assertThat(observedMostFrequentName.getValue(), is(equalTo(expectedMostFrequentName.getName())));

    // Assert that the most frequent name appears within 0.1% of its expectation; may need adjusting if sample size is changed
    assertThat((adjustedObservedDistribution),
        is(closeTo(expectedMostFrequentName.getDistribution(), 0.1)));
  }

  private void displayNameObservations(final List<DiscreteStats.Observation<String>> nameObservations) {
    int i;
    for (i = 0; i < 5; i++) {
      final DiscreteStats.Observation<String> observation = nameObservations.get(i);
      System.out.format("    [%5d] %-20s %, 10d % 7.5f%%%n", i, observation.getValue(), observation.getCount(), observation.getDistribution());
    }
    for (i = nameObservations.size() - 5; i < nameObservations.size(); i++) {
      final DiscreteStats.Observation<String> observation = nameObservations.get(i);
      System.out.format("    [%5d] %-20s %, 10d % 7.5f%%%n", i, observation.getValue(), observation.getCount(), observation.getDistribution());
    }
  }

  private double getPercent(final int recordCount, final ContinuousStats<?> stats) {
    return 100.0 * stats.getCount() / recordCount;
  }

  private double getPercent(final int recordCount, final DiscreteStats<?> stats) {
    return 100.0 * stats.getTotalObservations() / recordCount;
  }

  /**
   * Formats a nanosecond time into a {@code String} of the form
   * {@code <hours> h <minutes> m <seconds>.<fractional_seconds> s}.
   *
   * @param nanos tbe nanosecond value to convert
   *
   * @return the formatted result
   */
  public static CharSequence formatNanos(final long nanos) {
    final int elapsedNanos = (int)(nanos % 1000000000);
    final long elapsedTimeSeconds = nanos / 1000000000;
    final int elapsedSeconds = (int)(elapsedTimeSeconds % 60);
    final long elapsedTimeMinutes = elapsedTimeSeconds / 60;
    final int elapsedMinutes = (int)(elapsedTimeMinutes % 60);
    final long elapsedHours = elapsedTimeMinutes / 60;

    final StringBuilder sb = new StringBuilder(128);
    if (elapsedHours > 0) {
      sb.append(elapsedHours).append(" h ");
    }
    if (elapsedHours > 0 || elapsedMinutes > 0) {
      sb.append(elapsedMinutes).append(" m ");
    }
    sb.append(elapsedSeconds).append('.').append(String.format("%09d", elapsedNanos)).append(" s");

    return sb;
  }
}
