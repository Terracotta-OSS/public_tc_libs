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

package com.terracottatech.store.client.stream;

import org.junit.Test;

import java.util.ArrayList;
import java.util.stream.DoubleStream;

import static org.junit.Assert.assertNotNull;

/**
 * Tests closure semantics for {@link DoubleStream} implementations.
 */
public abstract class AbstractDoubleStreamClosureTest extends AbstractBaseStreamClosureTest<Double, DoubleStream> {

  @Override
  protected abstract DoubleStream getStream();

  /* ==========================================================================================
   * DoubleStream methods
   */

  @Test
  public void testAllMatch() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.allMatch(d -> true), IllegalStateException.class);
  }

  @Test
  public void testAnyMatch() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.anyMatch(d -> true), IllegalStateException.class);
  }

  @Test
  public void testAverage() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::average, IllegalStateException.class);
  }

  @Test
  public void testBoxed() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::boxed, IllegalStateException.class);
  }

  @Test
  public void testCollect() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.collect(ArrayList::new, ArrayList::add, ArrayList::addAll), IllegalStateException.class);
  }

  @Test
  public void testCount() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::count, IllegalStateException.class);
  }

  @Test
  public void testDistinct() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::distinct, IllegalStateException.class);
  }

  @Test
  public void testFilter() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.filter(d -> true), IllegalStateException.class);
  }

  @Test
  public void testFindAny() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::findAny, IllegalStateException.class);
  }

  @Test
  public void testFindFirst() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::findFirst, IllegalStateException.class);
  }

  @Test
  public void testFlatMap() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.flatMap(DoubleStream::of), IllegalStateException.class);
  }

  @Test
  public void testForEach() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.forEach(d -> {}), IllegalStateException.class);
  }

  @Test
  public void testForEachOrdered() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.forEachOrdered(d -> {}), IllegalStateException.class);
  }

  @Test
  public void testIterator() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::iterator, IllegalStateException.class);
  }

  @Test
  public void testLimit() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.limit(2), IllegalStateException.class);
  }

  @Test
  public void testMap() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.map(d -> d), IllegalStateException.class);
  }

  @Test
  public void testMapToInt() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.mapToInt(Double::hashCode), IllegalStateException.class);
  }

  @Test
  public void testMapToLong() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.mapToLong(Double::doubleToLongBits), IllegalStateException.class);
  }

  @Test
  public void testMapToObj() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.mapToObj(Double::valueOf), IllegalStateException.class);
  }

  @Test
  public void testMax() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::max, IllegalStateException.class);
  }

  @Test
  public void testMin() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::min, IllegalStateException.class);
  }

  @Test
  public void testNoneMatch() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.noneMatch(d -> true), IllegalStateException.class);
  }

  @Test
  public void testPeek() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.peek(d -> {}), IllegalStateException.class);
  }

  @Test
  public void testReduce1Arg() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.reduce((d1, d2) -> d2), IllegalStateException.class);
  }

  @Test
  public void testReduce2Arg() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.reduce(Double.NaN, (d1, d2) -> d2), IllegalStateException.class);
  }

  @Test
  public void testSkip() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(() -> stream.skip(2), IllegalStateException.class);
  }

  @Test
  public void testSorted() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::sorted, IllegalStateException.class);
  }

  @Test
  public void testSpliterator() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::spliterator, IllegalStateException.class);
  }

  @Test
  public void testSum() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::sum, IllegalStateException.class);
  }

  @Test
  public void testSummaryStatistics() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::summaryStatistics, IllegalStateException.class);
  }

  @Test
  public void testToArray() throws Exception {
    DoubleStream stream = close(getStream());
    assertThrows(stream::toArray, IllegalStateException.class);
  }
}
