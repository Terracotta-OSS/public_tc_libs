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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * Basic tests for {@link SystemTimeReference SystemTimeReference}.
 *
 * @author Clifford W. Johnson
 */
public class SystemTimeReferenceTest {

  @SuppressWarnings("StatementWithEmptyBody")
  @Test
  public void testNew() throws Exception {
    final SystemTimeReference timeReference = new SystemTimeReference();
    long startTime = System.currentTimeMillis();
    for (long currentTime = System.currentTimeMillis(); currentTime <= startTime; currentTime = System.currentTimeMillis()) {
      // empty
    }
    assertThat(new SystemTimeReference(), greaterThan(timeReference));
  }

  @Test
  public void testGetTime() throws Exception {
    final long beforeTime = System.currentTimeMillis();
    final SystemTimeReference timeReference = new SystemTimeReference();
    final long afterTime = System.currentTimeMillis();

    assertThat(timeReference.getTime(), greaterThanOrEqualTo(beforeTime));
    assertThat(timeReference.getTime(), lessThanOrEqualTo(afterTime));
  }
}
