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
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicCollector;
import com.terracottatech.store.intrinsics.IntrinsicType;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.internal.function.Functions.countingCollector;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

abstract class CascadingCollectorTest<V, F, R> {

  private final IntrinsicType expectedIntrinsicType;

  CascadingCollectorTest(IntrinsicType expectedIntrinsicType) {
    this.expectedIntrinsicType = expectedIntrinsicType;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testResult() {
    Collector<Record<?>, ?, V> collector = getCollector();
    Object container = collector.supplier().get();
    assertThat(container, containerMatcher());

    BiConsumer<Object, Record<?>> biConsumer = (BiConsumer<Object, Record<?>>)
            collector.accumulator();
    biConsumer.accept(container, testRecord());

    Function<Object, V> finisher = (Function<Object, V>) collector.finisher();
    V result = finisher.apply(container);
    checkResult(result);
  }

  Collector<Record<?>, ?, V> getCollector() {
    return getCollector(function());
  }

  abstract F function();

  @SuppressWarnings("unchecked")
  private Collector<Record<?>, ?, V> getCollector(F function) {
    return getCollector(function, downstream());
  }

  abstract Collector<R, ?, Long> downstream();

  abstract Collector<Record<?>, ?, V> getCollector(F function, Collector<R, ?, Long> downstream);

  Matcher<? super Object> containerMatcher() {
    return instanceOf(Object.class);
  }

  TestRecord<Integer> testRecord() {
    return new TestRecord<>(1, singletonList(cell("age", 10)));
  }

  void checkResult(V result) {
    assertThat(result, is(1L));
  }

  @Test
  public void testIntrinsic() {
    Collector<Record<?>, ?, V> collector = getCollector();
    assertThat(collector, is(instanceOf(Intrinsic.class)));
    Intrinsic intrinsic = (Intrinsic) collector;
    IntrinsicType intrinsicType = intrinsic.getIntrinsicType();
    assertNotNull(intrinsicType);
    assertEquals(expectedIntrinsicType, intrinsicType);
  }

  @Test
  public void testObjectMethods() {

    Collector<Record<?>, ?, V> collector = getCollector();
    assertEquality(collector, collector);

    Collector<Record<?>, ?, V> same = getCollector(
            function()
    );
    assertEquality(collector, same);

    Collector<Record<?>, ?, V> otherFunction = getCollector(
            otherFunction()
    );
    assertInequality(collector, otherFunction);

    @SuppressWarnings("unchecked")
    Collector<Record<?>, ?, V> otherDownstream = getCollector(
            function(),
            otherDownstream()
    );
    assertInequality(collector, otherDownstream);

    Collector<?, ?, ?> otherType = otherCollector();
    assertInequality(collector, otherType);
  }

  abstract F otherFunction();

  @SuppressWarnings("unchecked")
  Collector<R, ?, Long> otherDownstream() {
    return mock(IntrinsicCollector.class);
  }

  Collector<?, ?, ?> otherCollector() {
    return countingCollector();
  }

  private void assertEquality(Object left, Object right) {
    assertThat(right, instanceOf(Intrinsic.class));
    assertEquals(left, right);
    assertEquals(left.toString(), right.toString());
    assertEquals(left.hashCode(), right.hashCode());
  }

  private void assertInequality(Object left, Object right) {
    assertThat(right, instanceOf(Intrinsic.class));
    assertNotEquals(left, right);
    assertNotEquals(left.toString(), right.toString());
  }
}
