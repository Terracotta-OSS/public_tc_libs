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

package com.terracottatech.sovereign.time;

import org.junit.Test;

import com.terracottatech.sovereign.VersionLimitStrategy;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Tests for {@link SystemTimeReferenceVersionLimitStrategy}.
 *
 * @author Clifford W. Johnson
 */
public class SystemTimeReferenceVersionLimitStrategyTest {

  @Test
  public void testCtorNullUnit() throws Exception {
    try {
      new SystemTimeReferenceVersionLimitStrategy(5, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testCtorNegativeTime() throws Exception {
    try {
      new SystemTimeReferenceVersionLimitStrategy(-1, TimeUnit.SECONDS);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testCtorSmallTime() throws Exception {
    try {
      new SystemTimeReferenceVersionLimitStrategy(999, TimeUnit.MICROSECONDS);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testType() throws Exception {
    final SystemTimeReferenceVersionLimitStrategy strategy =
        new SystemTimeReferenceVersionLimitStrategy(5, TimeUnit.SECONDS);
    assertThat(strategy.type(), is(equalTo(SystemTimeReference.class)));
  }

  @Test
  public void testGetKeepTimeInMillis() throws Exception {
    final SystemTimeReferenceVersionLimitStrategy strategy =
        new SystemTimeReferenceVersionLimitStrategy(5, TimeUnit.SECONDS);
    assertThat(strategy.getKeepInMillis(), is(equalTo(TimeUnit.SECONDS.toMillis(5))));
  }

  @Test
  public void testTimeReferenceFilterZero() throws Exception {
    final SystemTimeReferenceVersionLimitStrategy strategy =
        new SystemTimeReferenceVersionLimitStrategy(0, TimeUnit.NANOSECONDS);
    final BiFunction<SystemTimeReference, SystemTimeReference, VersionLimitStrategy.Retention> filter =
        strategy.getTimeReferenceFilter();
    assertThat(filter, is(not(nullValue())));

    final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();
    final SystemTimeReference begin = generator.get();
    SystemTimeReference now;
    do {
      now = generator.get();
    } while (now.compareTo(now) < 0);

    assertThat(filter.apply(now, begin), is(VersionLimitStrategy.Retention.FINISH));
  }

  @Test
  public void testTimeReferenceFilterNonZero() throws Exception {
    final SystemTimeReferenceVersionLimitStrategy strategy =
        new SystemTimeReferenceVersionLimitStrategy(1, TimeUnit.MILLISECONDS);
    final BiFunction<SystemTimeReference, SystemTimeReference, VersionLimitStrategy.Retention> filter =
        strategy.getTimeReferenceFilter();
    assertThat(filter, is(not(nullValue())));

    final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();
    final SystemTimeReference begin = generator.get();
    SystemTimeReference now;
    do {
      now = generator.get();
    } while (now.getTime() - begin.getTime() <= 1);

    assertThat(filter.apply(now, now), is(VersionLimitStrategy.Retention.FINISH));
    assertThat(filter.apply(now, begin), is(VersionLimitStrategy.Retention.DROP));
  }
}
