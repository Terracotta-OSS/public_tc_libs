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
package com.terracottatech.sovereign.btrees.bplustree;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class SingleListBasedAccessTrackerImplTest {
  @Test
  public void testSimple() {
    SingleListBasedAccessTrackerImpl tracker = new SingleListBasedAccessTrackerImpl();
    long lowest = tracker.getLowestActiveRevision(1);
    Assert.assertThat(lowest, is(1L));
    ReadAccessor r1 = tracker.registerRead(new Object(), 10);
    ReadAccessor r2 = tracker.registerRead(new Object(), 5);
    ReadAccessor r3 = tracker.registerRead(new Object(), 20);

    coreTest(tracker, r2, r3);

  }

  static void coreTest(ListBasedAccessTracker tracker, ReadAccessor r2, ReadAccessor r3) {
    long lowest;
    lowest = tracker.getLowestActiveRevision(1);
    Assert.assertThat(lowest, is(1L));
    lowest = tracker.getLowestActiveRevision(100);
    Assert.assertThat(lowest, is(5L));

    tracker.deregisterRead(r3);
    lowest = tracker.getLowestActiveRevision(10L);
    Assert.assertThat(lowest, is(5L));

    tracker.deregisterRead(r2);
    lowest = tracker.getLowestActiveRevision(10L);
    Assert.assertThat(lowest, is(10L));
  }
}