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
import java.util.stream.LongStream;

import static org.junit.Assert.assertNotNull;

/**
 * Tests closure semantics for {@link LongStream} implementations.
 */
public abstract class AbstractLongStreamClosureTest extends AbstractBaseStreamClosureTest<Long, LongStream> {

  @Override
  protected abstract LongStream getStream();

  /* ==========================================================================================
   * LongStream methods
   */

  @Test
  public void testAllMatch() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.allMatch(l -> true), IllegalStateException.class);
  }

  @Test
  public void testAnyMatch() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.anyMatch(l -> true), IllegalStateException.class);
  }

  @Test
  public void testAsDoubleStream() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::asDoubleStream, IllegalStateException.class);
  }

  @Test
  public void testAverage() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::average, IllegalStateException.class);
  }

  @Test
  public void testBoxed() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::boxed, IllegalStateException.class);
  }

  @Test
  public void testCollect() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.collect(ArrayList::new, ArrayList::add, ArrayList::addAll), IllegalStateException.class);
  }

  @Test
  public void testCount() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::count, IllegalStateException.class);
  }

  @Test
  public void testDistinct() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::distinct, IllegalStateException.class);
  }

  @Test
  public void testFilter() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.filter(l -> true), IllegalStateException.class);
  }

  @Test
  public void testFindAny() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::findAny, IllegalStateException.class);
  }

  @Test
  public void testFindFirst() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::findFirst, IllegalStateException.class);
  }

  @Test
  public void testFlatMap() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.flatMap(LongStream::of), IllegalStateException.class);
  }

  @Test
  public void testForEach() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.forEach(l -> {}), IllegalStateException.class);
  }

  @Test
  public void testForEachOrdered() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.forEachOrdered(l -> {}), IllegalStateException.class);
  }

  @Test
  public void testIterator() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::iterator, IllegalStateException.class);
  }

  @Test
  public void testLimit() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.limit(2), IllegalStateException.class);
  }

  @Test
  public void testMap() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.map(l -> l), IllegalStateException.class);
  }

  @Test
  public void testMapToDouble() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.mapToDouble(l -> (double)l), IllegalStateException.class);
  }

  @Test
  public void testMapToInt() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.mapToInt(Long::hashCode), IllegalStateException.class);
  }

  @Test
  public void testMapToObj() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.mapToObj(Long::valueOf), IllegalStateException.class);
  }

  @Test
  public void testMax() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::max, IllegalStateException.class);
  }

  @Test
  public void testMin() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::max, IllegalStateException.class);
  }

  @Test
  public void testNoneMatch() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.noneMatch(l -> true), IllegalStateException.class);
  }

  @Test
  public void testPeek() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.peek(l -> {}), IllegalStateException.class);
  }

  @Test
  public void testReduce1Arg() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.reduce((i1, i2) -> i2), IllegalStateException.class);
  }

  @Test
  public void testReduce2Arg() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.reduce(0, (i1, i2) -> i2), IllegalStateException.class);
  }

  @Test
  public void testSkip() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(() -> stream.skip(2), IllegalStateException.class);
  }

  @Test
  public void testSorted() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::sorted, IllegalStateException.class);
  }

  @Test
  public void testSpliterator() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::spliterator, IllegalStateException.class);
  }

  @Test
  public void testSum() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::sum, IllegalStateException.class);
  }

  @Test
  public void testSummaryStatistics() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::summaryStatistics, IllegalStateException.class);
  }

  @Test
  public void testToArray() throws Exception {
    LongStream stream = close(getStream());
    assertThrows(stream::toArray, IllegalStateException.class);
  }

}
