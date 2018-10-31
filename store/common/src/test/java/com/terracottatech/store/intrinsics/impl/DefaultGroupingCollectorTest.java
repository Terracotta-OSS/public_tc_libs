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
import java.util.function.Function;
import java.util.stream.Collector;

import static com.terracottatech.store.internal.function.Functions.groupingByCollector;
import static com.terracottatech.store.internal.function.Functions.groupingByConcurrentCollector;
import static org.hamcrest.Matchers.instanceOf;

public class DefaultGroupingCollectorTest extends GroupingCollectorTest<Map<Integer, Long>> {

  public DefaultGroupingCollectorTest() {
    super(IntrinsicType.COLLECTOR_GROUPING);
  }

  @Override
  Collector<Record<?>, ?, Map<Integer, Long>> getCollector(
          Function<Record<?>, Integer> function, Collector<Record<?>, ?, Long> downstream) {
    return groupingByCollector(function, downstream);
  }

  @Override
  Collector<?, ?, ?> otherCollector() {
    return groupingByConcurrentCollector(function(), downstream());
  }

  @Override
  Matcher<? super Object> containerMatcher() {
    return instanceOf(HashMap.class);
  }
}
