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

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

/**
 * Test intervals bounded by special Double values.
 */
public class TestDoubleIntervals extends TestIntervals<Double> {

  private final Interval<Double> CLOSED_START = builder().startClosed(NEGATIVE_INFINITY).build();
  private final Interval<Double> CLOSED_END = builder().endClosed(NaN).build();
  private final Interval<Double> CLOSED = builder().startClosed(NEGATIVE_INFINITY).endClosed(NaN).build();
  private final Interval<Double> OPEN_TO_CLOSED = builder().startClosed(NEGATIVE_INFINITY).endOpen(NaN).build();
  private final Interval<Double> CLOSED_TO_OPEN = builder().startOpen(NEGATIVE_INFINITY).endClosed(NaN).build();
  private final Interval<Double> OPEN = builder().startOpen(NEGATIVE_INFINITY).endOpen(NaN).build();
  private final Interval<Double> OPEN_DEGENERATE = builder().startOpen(POSITIVE_INFINITY).endOpen(POSITIVE_INFINITY).build();

  private final Interval<Double> EMPTY_CLOSED = builder().startClosed(POSITIVE_INFINITY).endClosed(POSITIVE_INFINITY).build();
  private final Interval<Double> EMPTY_CLOSED_TO_OPEN = builder().startClosed(POSITIVE_INFINITY).endOpen(POSITIVE_INFINITY).build();
  private final Interval<Double> EMPTY_OPEN_TO_CLOSED = builder().startOpen(POSITIVE_INFINITY).endClosed(POSITIVE_INFINITY).build();
  private final Interval<Double> EMPTY_NEGATIVE = builder().startOpen(NaN).endOpen(NEGATIVE_INFINITY).build();

  @Override
  protected Stream<Interval<Double>> getNonEmptyIntervals() {
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
  protected Stream<Interval<Double>> getEmptyIntervals() {
    return Stream.of(
            EMPTY_CLOSED,
            EMPTY_CLOSED_TO_OPEN,
            EMPTY_OPEN_TO_CLOSED,
            EMPTY_NEGATIVE
    );
  }

  @Override
  Double[] getEndpointValues() {
    return new Double[]{NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN};
  }
}
