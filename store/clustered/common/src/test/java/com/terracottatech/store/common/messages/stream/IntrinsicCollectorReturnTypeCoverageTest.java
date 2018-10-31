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

package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.Record;
import com.terracottatech.store.TestRecord;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.function.Collectors;
import com.terracottatech.store.intrinsics.IntrinsicCollector;
import com.terracottatech.store.intrinsics.impl.CountingCollector;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.stream.Collector;
import java.util.stream.Stream;

import static com.terracottatech.store.common.messages.stream.ElementValue.ValueType;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;

/**
 * Test that all return values of IntrinsicCollectors can be converted to ElementValues.
 */
public class IntrinsicCollectorReturnTypeCoverageTest
        extends ReturnTypeCoverageTest<IntrinsicCollector<Record<?>, ?, ?>> {

  private static final Record<Integer> RECORD = new TestRecord<>(1, emptyList());

  public IntrinsicCollectorReturnTypeCoverageTest() {
    super(IntrinsicCollector.class, asList(
            CellDefinition.defineInt("bar").value().isGreaterThan(0),
            CellDefinition.defineInt("bar").intValueOr(0),
            CellDefinition.defineInt("bar").valueOr(0).asComparator(),
            CellDefinition.defineLong("bar").longValueOr(0),
            CellDefinition.defineDouble("bar").doubleValueOr(0),

            CountingCollector.counting(),
            new Collector<?, ?, ?>[]{CountingCollector.counting()},
            Collectors.VarianceType.SAMPLE
    ));
  }

  @Test
  public void testCollectorsStatics() {
    testCompleteness(null, Collectors.class);
  }

  @Override
  Object apply(IntrinsicCollector<Record<?>, ?, ?> collector) {
    return Stream.of(RECORD).collect(collector);
  }

  @Override
  Matcher<ValueType> typeMatcher() {
    return anyOf(
            is(ValueType.PRIMITIVE),
            is(ValueType.MAP),
            is(ValueType.CONCURRENT_MAP),
            is(ValueType.INT_SUMMARY_STATISTICS),
            is(ValueType.LONG_SUMMARY_STATISTICS),
            is(ValueType.DOUBLE_SUMMARY_STATISTICS)
    );
  }
}
