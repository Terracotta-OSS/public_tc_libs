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
import com.terracotta.perf.data.EducationLevel;

import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link UsPostSecondaryData}.
 *
 * @author Clifford W. Johnson
 */
public class UsPostSecondaryDataTest {

  @Test
  public void testLoading() throws Exception {
    assertNotNull(UsPostSecondaryData.getUsPostSecondarySchoolsByState());
  }

  /**
   * Ensure that {@link UsPostSecondaryData#getWeightedUsPostSecondarySchoolsByEducationLevel()} returns a proper
   * structure and that selection from that collection is consistent with the number of schools available.
   */
  @Test
  public void testPostSecondaryByEducation() throws Exception {
    final int samples = 5000000;
    final Random rnd = new Random(1L);

    final DiscreteStats<Integer> educationLevelStats = new DiscreteStats<Integer>();
    final DiscreteStats<UsPostSecondarySchool> schoolStats = new DiscreteStats<UsPostSecondarySchool>();
    for (int i = 0; i < samples; i++) {
      int educationLevel;
      do {
        educationLevel = UsCensusData.chooseEducationLevel(rnd);
      } while (educationLevel < EducationLevel.POST_SECONDARY);
      final UsPostSecondarySchool usSchool =
          UsPostSecondaryData.getUsPostSecondarySchoolForEducationLevel(rnd, educationLevel);

      assertThat(usSchool.getHighestDegreeOffered() >= educationLevel, is(true));

      educationLevelStats.collect(educationLevel);
      schoolStats.collect(usSchool);
    }

    final List<DiscreteStats.Observation<Integer>> educationLevelObservations = educationLevelStats.getObservations();

    final List<DiscreteStats.Observation<UsPostSecondarySchool>> schoolObservations = schoolStats.getObservations();
    System.out.format("%s post-secondary schools chosen from %d education levels in %d samples%n",
        schoolObservations.size(), educationLevelObservations.size(), schoolStats.getTotalObservations());

    int expectedSchoolCount = 0;
    for (final List<UsPostSecondarySchool> schools : UsPostSecondaryData.getUsPostSecondarySchoolsByState().values()) {
      expectedSchoolCount += schools.size();
    }

    // Ensure the sampling hit most of the schools -- if you change the sample count, this test may need to be adjusted
    assertThat(schoolObservations.size(),
        is(both(greaterThanOrEqualTo(expectedSchoolCount * 99 / 100)).and(lessThanOrEqualTo(expectedSchoolCount))));

    final DiscreteStats.Observation<Integer> mostFrequentEducationLevel = educationLevelObservations.get(0);
    final DiscreteStats.Observation<Integer> leastFrequentEducationLevel =
        educationLevelObservations.get(educationLevelObservations.size() - 1);
    System.out.format("    Most frequently chosen education level = %d [%.2f%%]%n",
        mostFrequentEducationLevel.getValue(), mostFrequentEducationLevel.getDistribution());
    System.out.format("    Least frequently chosen education level = %d [%.2f%%]%n",
        leastFrequentEducationLevel.getValue(), leastFrequentEducationLevel.getDistribution());

    final DiscreteStats.Observation<UsPostSecondarySchool> mostFrequentSchool = schoolObservations.get(0);
    final DiscreteStats.Observation<UsPostSecondarySchool> leastFrequentSchool =
        schoolObservations.get(schoolObservations.size() - 1);
    System.out.format("    Most frequently chosen post-secondary school = %s [%.2f%%]%n",
        mostFrequentSchool.getValue(), mostFrequentSchool.getDistribution());
    System.out.format("    Least frequently chosen post-secondary school = %s [%.4f%%]%n",
        leastFrequentSchool.getValue(), leastFrequentSchool.getDistribution());
  }
}
