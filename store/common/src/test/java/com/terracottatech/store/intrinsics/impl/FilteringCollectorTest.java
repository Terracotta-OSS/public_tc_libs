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

package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Record;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.function.Predicate;
import java.util.stream.Collector;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.function.Collectors.filtering;
import static com.terracottatech.store.function.Collectors.groupingBy;
import static org.mockito.Mockito.mock;

public class FilteringCollectorTest extends CascadingCollectorTest<Long, Predicate<Record<?>>, Record<?>> {

  public FilteringCollectorTest() {
    super(IntrinsicType.COLLECTOR_FILTERING);
  }

  @Override
  Predicate<Record<?>> function() {
    return defineInt("age")
            .intValueOrFail()
            .is(10);
  }

  @Override
  Collector<Record<?>, ?, Long> downstream() {
    return counting();
  }

  @Override
  Collector<Record<?>, ?, Long> getCollector(Predicate<Record<?>> function, Collector<Record<?>, ?, Long> downstream) {
    return filtering(function, downstream);
  }

  @Override
  Predicate<Record<?>> otherFunction() {
    return defineInt("age")
            .intValueOrFail()
            .is(9);
  }

  @SuppressWarnings("unchecked")
  @Override
  Collector<Record<?>, ?, Long> otherDownstream() {
    return (Collector<Record<?>, ?, Long>) groupingBy(mock(IntrinsicFunction.class), counting());
  }
}
