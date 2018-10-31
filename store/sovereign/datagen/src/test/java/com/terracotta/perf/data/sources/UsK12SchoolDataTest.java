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

import org.junit.Test;

import com.terracotta.perf.DiscreteStats;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertNotNull;

/**
 * @author Clifford W. Johnson
 */
public class UsK12SchoolDataTest {

  @Test
  public void testLoading() throws Exception {
    assertNotNull(UsK12SchoolData.getUsHighSchoolsByState());
  }

  /**
   * Ensure that {@link UsK12SchoolData#getWeightedUsHighSchoolsByState()} returns a proper structure
   * and that selection from that collection is consistent with the number of schools available.
   */
  @Test
  public void testGetUsHighSchoolsByState() throws Exception {
    final int samples = 5000000;
    final Random rnd = new Random(1L);

    final Map<UsState, NavigableMap<Float, UsSchool>> weightedSchools = UsK12SchoolData.getWeightedUsHighSchoolsByState();
    assertThat(weightedSchools.size(), is(51));

    final DiscreteStats<String> stateStats = new DiscreteStats<String>();
    final DiscreteStats<UsSchool> schoolStats = new DiscreteStats<UsSchool>();
    for (int i = 0; i < samples; i++) {
      final String state = GoogleGeolocationData.chooseCity(rnd).getState();
      final UsSchool usSchool = UsK12SchoolData.getUsHighSchoolInState(rnd, UsCensusData.getUsStateByName(state));

      assertThat(usSchool.getState().getName(), is(equalTo(state)));

      stateStats.collect(state);
      schoolStats.collect(usSchool);
    }

    final List<DiscreteStats.Observation<String>> stateObservations = stateStats.getObservations();
    assertThat(stateObservations.size(), is(51));

    final List<DiscreteStats.Observation<UsSchool>> schoolObservations = schoolStats.getObservations();
    System.out.format("%s high schools chosen from %d states in %d samples%n",
        schoolObservations.size(), stateObservations.size(), schoolStats.getTotalObservations());

    int expectedSchoolCount = 0;
    for (final List<UsSchool> schools : UsK12SchoolData.getUsHighSchoolsByState().values()) {
      expectedSchoolCount += schools.size();
    }

    // Ensure the sampling hit most of the schools -- if you change the sample count, this test may need to be adjusted
    assertThat(schoolObservations.size(),
        is(both(greaterThanOrEqualTo(expectedSchoolCount * 99 / 100)).and(lessThanOrEqualTo(expectedSchoolCount))));

    final DiscreteStats.Observation<String> mostFrequentState = stateObservations.get(0);
    final DiscreteStats.Observation<String> leastFrequentState = stateObservations.get(stateObservations.size() - 1);
    System.out.format("    Most frequently chosen state = %s [%.2f%%]%n",
        mostFrequentState.getValue(), mostFrequentState.getDistribution());
    System.out.format("    Least frequently chosen state = %s [%.2f%%]%n",
        leastFrequentState.getValue(), leastFrequentState.getDistribution());

    final DiscreteStats.Observation<UsSchool> mostFrequentSchool = schoolObservations.get(0);
    final DiscreteStats.Observation<UsSchool> leastFrequentSchool =
        schoolObservations.get(schoolObservations.size() - 1);
    System.out.format("    Most frequently chosen high school = %s [%.2f%%]%n",
        mostFrequentSchool.getValue(), mostFrequentSchool.getDistribution());
    System.out.format("    Least frequently chosen high school = %s [%.4f%%]%n",
        leastFrequentSchool.getValue(), leastFrequentSchool.getDistribution());
  }
}
