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

import org.hamcrest.Matchers;
import org.junit.Test;

import com.terracotta.perf.DiscreteStats;

import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Basic tests for {@link GoogleGeolocationData}.
 *
 * @author Clifford W. Johnson
 */
public class GoogleGeolocationDataTest {

  @Test
  public void testTryChooseCity() throws Exception {
    final int samples = 500000;
    final Random rnd = new Random(1L);

    final DiscreteStats<String> stateStats = new DiscreteStats<String>();
    final DiscreteStats<GoogleGeolocationData.UsCity> cityStats = new DiscreteStats<GoogleGeolocationData.UsCity>();
    for (int i = 0; i < samples; i++) {
      final GoogleGeolocationData.UsCity usCity = GoogleGeolocationData.chooseCity(rnd);
      assertThat(usCity, is(not(nullValue())));
      stateStats.collect(usCity.getState());
      cityStats.collect(usCity);
    }

    final List<DiscreteStats.Observation<String>> stateObservations = stateStats.getObservations();
    assertThat(stateObservations.size(), is(51));

    final List<DiscreteStats.Observation<GoogleGeolocationData.UsCity>> cityObservations = cityStats.getObservations();
    System.out.format("%d cities chosen from %d states%n", cityObservations.size(), stateObservations.size());

    int expectedCityCount = GoogleGeolocationData.getUsCities().size();

    // Ensure the sampling hit most of the cities -- if you change the sample count, this test may need to be adjusted
    assertThat(cityObservations.size(),
        is(Matchers.both(greaterThanOrEqualTo(expectedCityCount * 99 / 100)).and(lessThanOrEqualTo(expectedCityCount))));

    final DiscreteStats.Observation<String> mostFrequentState = stateObservations.get(0);
    System.out.format("    Most frequently chosen state = %s [%.2f%%]%n",
        mostFrequentState.getValue(), mostFrequentState.getDistribution());
    final DiscreteStats.Observation<String> leastFrequentState = stateObservations.get(stateObservations.size() - 1);
    System.out.format("    Least frequently chosen state = %s [%.2f%%]%n",
        leastFrequentState.getValue(), leastFrequentState.getDistribution());

    final DiscreteStats.Observation<GoogleGeolocationData.UsCity> mostFrequentCity = cityObservations.get(0);
    System.out.format("    Most frequently chosen city = %s [%.2f%%]%n",
        mostFrequentCity.getValue(), mostFrequentCity.getDistribution());
    final DiscreteStats.Observation<GoogleGeolocationData.UsCity> leastFrequentCity =
        cityObservations.get(cityObservations.size() - 1);
    System.out.format("    Least frequently chosen city = %s [%.4f%%]%n",
        leastFrequentCity.getValue(), leastFrequentCity.getDistribution());
  }
}
