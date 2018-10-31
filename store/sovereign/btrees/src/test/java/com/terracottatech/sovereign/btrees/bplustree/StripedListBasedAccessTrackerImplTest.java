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

import static com.terracottatech.sovereign.btrees.bplustree.SingleListBasedAccessTrackerImplTest.coreTest;
import static org.hamcrest.core.Is.is;

public class StripedListBasedAccessTrackerImplTest {

  @Test
  public void testSimple() {
    ListBasedAccessTracker tracker = new StripedListBasedAccessTrackerImpl();
    long lowest = tracker.getLowestActiveRevision(1);
    Assert.assertThat(lowest, is(1L));
    ReadAccessor r1 = tracker.registerRead(new Object(), 10);
    ReadAccessor r2 = tracker.registerRead(new Object(), 5);
    ReadAccessor r3 = tracker.registerRead(new Object(), 20);
    for (int i = 21; i < 30; i++) {
      tracker.registerRead(new Object(), i);
    }
    coreTest(tracker, r2, r3);

  }
}