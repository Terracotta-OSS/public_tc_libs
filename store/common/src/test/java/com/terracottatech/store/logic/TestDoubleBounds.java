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

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

/**
 * Test bounds with special Double values.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestDoubleBounds extends TestBounds<Double> {

  @Override
  protected Start<Double>[] getStartsInAscendingOrder() {
    return new Start[]{
            Interval.<Double>negativeInfinity(),

            new Start<>(NEGATIVE_INFINITY, Bound.Inclusion.OPEN),
            new Start<>(NEGATIVE_INFINITY, Bound.Inclusion.CLOSED),

            new Start<>(0.0, Bound.Inclusion.OPEN),
            new Start<>(0.0, Bound.Inclusion.CLOSED),

            new Start<>(POSITIVE_INFINITY, Bound.Inclusion.OPEN),
            new Start<>(POSITIVE_INFINITY, Bound.Inclusion.CLOSED),

            new Start<>(NaN, Bound.Inclusion.OPEN),
            new Start<>(NaN, Bound.Inclusion.CLOSED)
    };
  }

  @Override
  protected End<Double>[] getEndsInAscendingOrder() {
    return new End[]{
            new End<>(NEGATIVE_INFINITY, Bound.Inclusion.CLOSED),
            new End<>(NEGATIVE_INFINITY, Bound.Inclusion.OPEN),

            new End<>(0.0, Bound.Inclusion.CLOSED),
            new End<>(0.0, Bound.Inclusion.OPEN),

            new End<>(POSITIVE_INFINITY, Bound.Inclusion.CLOSED),
            new End<>(POSITIVE_INFINITY, Bound.Inclusion.OPEN),

            new End<>(NaN, Bound.Inclusion.CLOSED),
            new End<>(NaN, Bound.Inclusion.OPEN),

            Interval.<Integer>positiveInfinity(),
    };
  }
}
