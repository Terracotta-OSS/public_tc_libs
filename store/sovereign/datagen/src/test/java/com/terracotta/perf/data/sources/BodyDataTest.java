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

import com.terracotta.perf.ContinuousStats;
import com.terracotta.perf.data.Gender;

import java.util.Random;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Validates operation of the {@link BodyData} methods.
 *
 * @author Clifford W. Johnson
 */
public class BodyDataTest {

  /**
   * Verifies the {@link BodyData#chooseHeight(Random, Gender)} method for {@code Gender.FEMALE}.
   */
  @Test
  public void testHeightFemale() throws Exception {
    verifyHeight(Gender.FEMALE);
  }

  /**
   * Verifies the {@link BodyData#chooseHeight(Random, Gender)} method for {@code Gender.MALE}.
   */
  @Test
  public void testHeightMale() throws Exception {
    verifyHeight(Gender.MALE);
  }

  /**
   * Verifies the {@link BodyData#chooseBMI(Random, Gender)} method for {@code Gender.FEMALE}.
   */
  @Test
  public void testBmiFemale() throws Exception {
    verifyBMI(Gender.FEMALE);
  }

  /**
   * Verifies the {@link BodyData#chooseBMI(Random, Gender)} method for {@code Gender.MALE}.
   */
  @Test
  public void testBmiMale() throws Exception {
    verifyBMI(Gender.MALE);
  }

  private void verifyHeight(final Gender gender) {
    final int samples = 100000;

    final Random rnd = new Random(1L);
    ContinuousStats.DoubleStats stats = new ContinuousStats.DoubleStats();
    for (int i = 0; i < samples; i++) {
      stats.collect(BodyData.chooseHeight(rnd, gender));
    }

    System.out.format("gender=%s Height stats: %s%n", gender, stats.compute().toString());
    assertThat(stats.getAvg(), is(closeTo(BodyData.getHeightMean(gender), 0.2)));
  }

  private void verifyBMI(final Gender gender) {
    final int samples = 100000;

    final Random rnd = new Random(1L);
    ContinuousStats.DoubleStats stats = new ContinuousStats.DoubleStats();
    for (int i = 0; i < samples; i++) {
      stats.collect(BodyData.chooseBMI(rnd, gender));
    }

    System.out.format("gender=%s BMI stats: %s%n", gender, stats.compute().toString());
    assertThat(stats.getAvg(), is(closeTo(BodyData.getBMIMean(gender), 0.2)));
  }
}
