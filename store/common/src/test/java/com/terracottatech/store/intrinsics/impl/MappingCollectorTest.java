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
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.function.Function;
import java.util.stream.Collector;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.function.Collectors.mapping;

public class MappingCollectorTest extends CascadingCollectorTest<Long, Function<Record<?>, Integer>, Integer> {

  public MappingCollectorTest() {
    super(IntrinsicType.COLLECTOR_MAPPING);
  }

  @Override
  Function<Record<?>, Integer> function() {
    return defineInt("age")
            .intValueOrFail()
            .increment()
            .boxed();
  }

  @Override
  Collector<Integer, ?, Long> downstream() {
    return counting();
  }

  @Override
  Collector<Record<?>, ?, Long> getCollector(
          Function<Record<?>, Integer> function, Collector<Integer, ?, Long> downstream) {
    return mapping(function, downstream);
  }

  @Override
  Function<Record<?>, Integer> otherFunction() {
    return defineInt("age")
            .intValueOrFail()
            .decrement()
            .boxed();
  }
}
