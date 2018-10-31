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
import java.util.Spliterator;
import java.util.stream.BaseStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests closure semantics for {@link Stream} implementations.
 */
public abstract class AbstractStreamClosureTest<T> extends AbstractBaseStreamClosureTest<T, Stream<T>> {

  /* ==========================================================================================
   * Stream methods
   */

  @Test
  public void testAllMatch() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.allMatch(t -> true), IllegalStateException.class);
  }

  @Test
  public void testAnyMatch() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.anyMatch(t -> true), IllegalStateException.class);
  }

  @Test
  public void testCollect1Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.collect(toList()), IllegalStateException.class);
  }

  @Test
  public void testCollect3Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.collect(ArrayList::new, ArrayList::add, ArrayList::addAll), IllegalStateException.class);
  }

  @Test
  public void testCount() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(stream::count, IllegalStateException.class);
  }

  @Test
  public void testDistinct() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(stream::distinct, IllegalStateException.class);
  }

  @Test
  public void testFilter() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.filter(t -> true), IllegalStateException.class);
  }

  @Test
  public void testFindAny() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(stream::findAny, IllegalStateException.class);
  }

  @Test
  public void testFindFirst() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(stream::findFirst, IllegalStateException.class);
  }

  @Test
  public void testFlatMap() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.flatMap(t -> Stream.of(t.toString().split("."))), IllegalStateException.class);
  }

  @Test
  public void testFlatMapToDouble() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.flatMapToDouble(s -> s.toString().chars().mapToDouble(i -> (double)i)), IllegalStateException.class);
  }

  @Test
  public void testFlatMapToInt() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.flatMapToInt(t -> IntStream.of(t.hashCode())), IllegalStateException.class);
  }

  @Test
  public void testFlatMapToLong() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.flatMapToLong(t -> t.toString().chars().mapToLong(i -> (long)i)), IllegalStateException.class);
  }

  @Test
  public void testForEach() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.forEach(t -> {}), IllegalStateException.class);
  }

  @Test
  public void testForEachOrdered() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.forEachOrdered(t -> {}), IllegalStateException.class);
  }

  @Test
  public void testLimit() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.limit(4), IllegalStateException.class);
  }

  @Test
  public void testMap() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.map(t -> t.toString().chars()), IllegalStateException.class);
  }

  @Test
  public void testMapToDouble() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.mapToDouble(t -> (double)t.toString().length()), IllegalStateException.class);
  }

  @Test
  public void testMapToInt() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.mapToInt(t -> t.toString().length()), IllegalStateException.class);
  }

  @Test
  public void testMapToLong() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.mapToLong(t -> t.toString().length()), IllegalStateException.class);
  }

  @Test
  public void testMax() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.max(comparing(Object::toString)), IllegalStateException.class);
  }

  @Test
  public void testMin() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.min(comparing(Object::toString)), IllegalStateException.class);
  }

  @Test
  public void testNoneMatch() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.noneMatch(t -> true), IllegalStateException.class);
  }

  @Test
  public void testPeek() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.peek(t -> {}), IllegalStateException.class);
  }

  @Test
  public void testReduce1Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.reduce((t1, t2) -> t2), IllegalStateException.class);
  }

  @Test
  public void testReduce2Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.reduce(null, (t1, t2) -> t2), IllegalStateException.class);
  }

  @Test
  public void testReduce3Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.reduce(null, (t1, t2) -> t2, (t1, t2) -> t2), IllegalStateException.class);
  }

  @Test
  public void testSkip() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.skip(2), IllegalStateException.class);
  }

  @Test
  public void testSorted0Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(stream::sorted, IllegalStateException.class);
  }

  @Test
  public void testSorted1Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.sorted((t1, t2) -> t1.toString().compareToIgnoreCase(t2.toString())), IllegalStateException.class);
  }

  @Test
  public void testToArray0Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(stream::toArray, IllegalStateException.class);
  }

  @Test
  public void testToArray1Arg() throws Exception {
    Stream<T> stream = close(getStream());
    assertThrows(() -> stream.toArray(Object[]::new), IllegalStateException.class);
  }

  /**
   * Test impact of {@link BaseStream#close()} on {@link BaseStream#spliterator() Spliterator} behavior.
   * This test shows that closure of the stream does <b>not</b> prevent the {@code Spliterator} from
   * consuming the stream.
   */
  @Test
  public void testSpliterator() throws Exception {
    Stream<T> stream = getStream();
    Spliterator<T> spliterator = stream.spliterator();
    stream.close();

    assertTrue(spliterator.tryAdvance(x -> { }));

    spliterator.forEachRemaining(x -> { });   // consume to exhaustion

    assertFalse(spliterator.tryAdvance(x -> { }));
  }

  @Override
  protected abstract Stream<T> getStream();
}
