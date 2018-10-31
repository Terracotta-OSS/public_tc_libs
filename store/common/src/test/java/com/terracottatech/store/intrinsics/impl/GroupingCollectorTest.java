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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.function.Collectors.counting;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public abstract class GroupingCollectorTest<M extends Map<Integer, Long>>
        extends CascadingCollectorTest<M, Function<Record<?>, Integer>, Record<?>> {

  GroupingCollectorTest(IntrinsicType expectedIntrinsicType) {
    super(expectedIntrinsicType);
  }

  @Override
  Function<Record<?>, Integer> function() {
    return defineInt("age").valueOrFail();
  }

  @Override
  Collector<Record<?>, ?, Long> downstream() {
    return counting();
  }

  @Override
  Function<Record<?>, Integer> otherFunction() {
    return defineInt("height").valueOrFail();
  }

  @Override
  void checkResult(M result) {
    assertThat(result.keySet(), containsInAnyOrder(10));
    assertThat(result.values(), containsInAnyOrder(1L));
  }
}
