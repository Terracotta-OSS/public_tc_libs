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
import org.hamcrest.Matcher;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.internal.function.Functions.partitioningCollector;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PartitioningCollectorTest
        extends CascadingCollectorTest<Map<Boolean, Long>, Predicate<Record<?>>, Record<?>> {

  public PartitioningCollectorTest() {
    super(IntrinsicType.COLLECTOR_PARTITIONING);
  }

  @Override
  Predicate<Record<?>> function() {
    return defineBool("active").isTrue();
  }

  @Override
  Collector<Record<?>, ?, Long> downstream() {
    return counting();
  }

  @Override
  Collector<Record<?>, ?, Map<Boolean, Long>> getCollector(
          Predicate<Record<?>> function, Collector<Record<?>, ?, Long> downstream) {
    return partitioningCollector(function, downstream);
  }

  @Override
  Predicate<Record<?>> otherFunction() {
    return defineBool("active").isFalse();
  }

  @Override
  Matcher<? super Object> containerMatcher() {
    return instanceOf(HashMap.class);
  }

  @Override
  TestRecord<Integer> testRecord() {
    return new TestRecord<>(1, singletonList(cell("active", true)));
  }

  @Override
  void checkResult(Map<Boolean, Long> result) {
    assertThat(result.keySet(), containsInAnyOrder(true, false));
    assertThat(result.get(true), is(1L));
    assertThat(result.get(false), is(0L));
  }
}
