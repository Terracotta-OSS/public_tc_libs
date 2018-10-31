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

package com.terracottatech.store.logic;

import java.util.stream.Stream;

/**
 * Test String intervals.
 */
public class TestStringIntervals extends TestIntervals<String> {

  private final Interval<String> CLOSED_START = builder().startClosed("A").build();
  private final Interval<String> CLOSED_END = builder().endClosed("Z").build();
  private final Interval<String> CLOSED = builder().startClosed("A").endClosed("Z").build();
  private final Interval<String> OPEN_TO_CLOSED = builder().startClosed("A").endOpen("Z").build();
  private final Interval<String> CLOSED_TO_OPEN = builder().startOpen("A").endClosed("Z").build();
  private final Interval<String> OPEN = builder().startOpen("A").endOpen("Z").build();
  private final Interval<String> OPEN_DEGENERATE = builder().startOpen("").endOpen("").build();

  private final Interval<String> EMPTY_CLOSED = builder().startClosed("").endClosed("").build();
  private final Interval<String> EMPTY_CLOSED_TO_OPEN = builder().startClosed("").endOpen("").build();
  private final Interval<String> EMPTY_OPEN_TO_CLOSED = builder().startOpen("").endClosed("").build();
  private final Interval<String> EMPTY_NEGATIVE = builder().startOpen("Z").endOpen("A").build();

  @Override
  protected Stream<Interval<String>> getNonEmptyIntervals() {
    return Stream.of(
            infinite,
            CLOSED_START,
            CLOSED_END,
            CLOSED,
            OPEN_TO_CLOSED,
            CLOSED_TO_OPEN,
            OPEN,
            OPEN_DEGENERATE
    );
  }

  @Override
  protected Stream<Interval<String>> getEmptyIntervals() {
    return Stream.of(
            EMPTY_CLOSED,
            EMPTY_CLOSED_TO_OPEN,
            EMPTY_OPEN_TO_CLOSED,
            EMPTY_NEGATIVE
    );
  }

  @Override
  String[] getEndpointValues() {
    return new String[]{"A", "O", "Z"};
  }
}
