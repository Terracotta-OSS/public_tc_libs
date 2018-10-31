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

import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.test.data.Animals;

import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP_TO_INT;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.intStream;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests against {@link RemoteIntStream}.
 */
public class RemoteIntStreamTest extends AbstractRemoteStreamTest<String, Integer, IntStream, RemoteIntStream<String>> {

  @SuppressWarnings("unchecked")
  public RemoteIntStreamTest() {
    super(MAP_TO_INT, (Class)RemoteIntStream.class, IntStream.class);    // unchecked
  }

  @Test
  public void testFilter() throws Exception {
    IntPredicate predicate = i -> i != 0;
    tryStream(stream -> {
      assertThrows(() -> stream.filter(null), NullPointerException.class);
      IntStream intStream = stream.filter(predicate);
      assertThat(intStream, is(not(instanceOf(RemoteIntStream.class))));
      assertThat(intStream, is(instanceOf(IntStream.class)));
      assertThat(intStream.count(), is(getExpectedStream().filter(predicate).count()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.INT_FILTER, TerminalOperation.COUNT);
    });
  }

  @Test
  public void testMap() throws Exception {
    IntUnaryOperator operator = i -> i * 2;
    tryStream(stream -> {
      assertThrows(() -> stream.map(null), NullPointerException.class);
      IntStream intStream = stream.map(operator);
      assertThat(intStream, is(not(instanceOf(RemoteIntStream.class))));
      assertThat(intStream, is(instanceOf(IntStream.class)));
      assertThat(intStream.sum(), is(getExpectedStream().map(operator).sum()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.INT_MAP, TerminalOperation.SUM);
    });
  }

  @Test
  public void testMapToObj() throws Exception {
    IntFunction<String> mapper = Integer::toHexString;
    tryStream(stream -> {
      assertThrows(() -> stream.mapToObj(null), NullPointerException.class);
      Stream<String> objStream = stream.mapToObj(mapper);
      assertThat(objStream, is(not(instanceOf(RemoteReferenceStream.class))));
      assertThat(objStream, is(instanceOf(Stream.class)));
      assertThat(objStream.count(), is(getExpectedStream().mapToObj(mapper).count()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.INT_MAP_TO_OBJ, TerminalOperation.COUNT);
    });
  }

  @Test
  public void testMapToDouble() throws Exception {
    IntToDoubleFunction mapper = i -> i * 1.0D;
    tryStream(stream -> {
      assertThrows(() -> stream.mapToDouble(null), NullPointerException.class);
      DoubleStream doubleStream = stream.mapToDouble(mapper);
      assertThat(doubleStream, is(not(instanceOf(RemoteDoubleStream.class))));
      assertThat(doubleStream, is(instanceOf(DoubleStream.class)));
      assertThat(doubleStream.sum(), is(getExpectedStream().mapToDouble(mapper).sum()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.INT_MAP_TO_DOUBLE, TerminalOperation.SUM);
    });
  }

  @Test
  public void testMapToLong() throws Exception {
    IntToLongFunction mapper = i -> (long)(i * 100);
    tryStream(stream -> {
      assertThrows(() -> stream.mapToLong(null), NullPointerException.class);
      LongStream longStream = stream.mapToLong(mapper);
      assertThat(longStream, is(not(instanceOf(RemoteLongStream.class))));
      assertThat(longStream, is(instanceOf(LongStream.class)));
      assertThat(longStream.sum(), is(getExpectedStream().mapToLong(mapper).sum()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.INT_MAP_TO_LONG, TerminalOperation.SUM);
    });
  }

  @Test
  public void testFlatMap() throws Exception {
    IntFunction<IntStream> mapper = i -> IntStream.of(i, i * 2);
    tryStream(stream -> {
      assertThrows(() -> stream.flatMap(null), NullPointerException.class);
      IntStream intStream = stream.flatMap(mapper);
      assertThat(intStream, is(not(instanceOf(RemoteIntStream.class))));
      assertThat(intStream, is(instanceOf(IntStream.class)));
      assertThat(intStream.sum(), is(getExpectedStream().flatMap(mapper).sum()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.INT_FLAT_MAP, TerminalOperation.SUM);
    });
  }

  @Test
  public void testDistinct() throws Exception {
    tryStream(stream -> {
      IntStream intStream = stream.distinct();
      assertThat(intStream, is(instanceOf(RemoteIntStream.class)));
      assertThat(intStream.sum(), is(getExpectedStream().distinct().sum()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.DISTINCT);
      assertTerminalPortableOp(stream, TerminalOperation.SUM);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSorted() throws Exception {
    tryStream(stream -> {
      IntStream intStream = stream.sorted();
      assertThat(intStream, is(instanceOf(RemoteIntStream.class)));
      assertThat(intStream.toArray(), is(getExpectedStream().sorted().toArray()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.TO_ARRAY_0);
    });
  }

  @Test
  public void testPeek() throws Exception {
    IntConsumer consumer = i -> { };
    tryStream(stream -> {
      assertThrows(() -> stream.peek(null), NullPointerException.class);
      IntStream intStream = stream.peek(consumer);
      assertThat(intStream, is(not(instanceOf(RemoteIntStream.class))));
      assertThat(intStream, is(instanceOf(IntStream.class)));
      assertThat(intStream.count(), is(getExpectedStream().peek(consumer).count()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.INT_PEEK, TerminalOperation.COUNT);
    });
  }

  @Test
  public void testLimit() throws Exception {
    int maxSize = 5;
    tryStream(stream -> {
      assertThrows(() -> stream.limit(-1), IllegalArgumentException.class);
      IntStream intStream = stream.limit(maxSize);
      assertThat(intStream, is(instanceOf(RemoteIntStream.class)));
      assertThat(intStream.count(), is(getExpectedStream().limit(maxSize).count()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.LIMIT);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSkip() throws Exception {
    int n = 5;
    tryStream(stream -> {
      assertThrows(() -> stream.skip(-1), IllegalArgumentException.class);
      IntStream intStream = stream.skip(n);
      assertThat(intStream, is(instanceOf(RemoteIntStream.class)));
      assertThat(intStream.count(), is(getExpectedStream().skip(n).count()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.SKIP);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testForEach() throws Exception {
    IntConsumer consumer = i -> { };
    tryStream(stream -> {
      assertThrows(() -> stream.forEach(null), NullPointerException.class);
      stream.forEach(consumer);
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_FOR_EACH);
    });
  }

  @Test
  public void testForEachOrdered() throws Exception {
    IntConsumer consumer = i -> { };
    tryStream(stream -> {
      assertThrows(() -> stream.forEachOrdered(null), NullPointerException.class);
      stream.forEachOrdered(consumer);
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_FOR_EACH_ORDERED);
    });
  }

  @Test
  public void testToArray() throws Exception {
    tryStream(stream -> {
      assertThat(stream.toArray(), intArrayContainsInAnyOrder(getExpectedStream().toArray()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.TO_ARRAY_0);
    });
  }

  @Test
  public void testReduce1Arg() throws Exception {
    IntBinaryOperator operator = Math::max;
    tryStream(stream -> {
      assertThrows(() -> stream.reduce(null), NullPointerException.class);
      assertThat(stream.reduce(operator), is(getExpectedStream().reduce(operator)));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_REDUCE_1);
    });
  }

  @Test
  public void testReduce2Arg() throws Exception {
    int identity = Integer.MIN_VALUE;
    IntBinaryOperator operator = Math::max;
    tryStream(stream -> {
      assertThrows(() -> stream.reduce(identity, null), NullPointerException.class);
      assertThat(stream.reduce(identity, operator), is(getExpectedStream().reduce(identity, operator)));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_REDUCE_2);
    });
  }

  @Test
  public void testCollect() throws Exception {
    Supplier<ArrayList<Integer>> supplier = ArrayList<Integer>::new;
    ObjIntConsumer<ArrayList<Integer>> accumulator = ArrayList<Integer>::add;
    BiConsumer<ArrayList<Integer>, ArrayList<Integer>> combiner = ArrayList<Integer>::addAll;
    tryStream(stream -> {
      assertThrows(() -> stream.collect(null, accumulator, combiner), NullPointerException.class);
      assertThrows(() -> stream.collect(supplier, null, combiner), NullPointerException.class);
      assertThrows(() -> stream.collect(supplier, accumulator, null), NullPointerException.class);
      assertThat(stream.collect(supplier, accumulator, combiner), containsInAnyOrder(getExpectedStream().collect(supplier, accumulator, combiner).toArray(new Integer[0])));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_COLLECT);
    });
  }

  @Test
  public void testSum() throws Exception {
    tryStream(stream -> {
      assertThat(stream.sum(), is(getExpectedStream().sum()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, TerminalOperation.SUM);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testMin() throws Exception {
    tryStream(stream -> {
      assertThat(stream.min(), is(getExpectedStream().min()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, TerminalOperation.MIN_0);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testMax() throws Exception {
    tryStream(stream -> {
      assertThat(stream.max(), is(getExpectedStream().max()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, TerminalOperation.MAX_0);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testCount() throws Exception {
    tryStream(stream -> {
      assertThat(stream.count(), is(getExpectedStream().count()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testAverage() throws Exception {
    tryStream(stream -> {
      assertThat(stream.average(), is(getExpectedStream().average()));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, TerminalOperation.AVERAGE);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSummaryStatistics() throws Exception {
    tryStream(stream -> {
      IntSummaryStatistics summaryStatistics = stream.summaryStatistics();
      assertThat(summaryStatistics, is(instanceOf(IntSummaryStatistics.class)));
      IntSummaryStatistics expectedSummaryStatistics = getExpectedStream().summaryStatistics();
      assertThat(summaryStatistics.getCount(), is(expectedSummaryStatistics.getCount()));
      assertThat(summaryStatistics.getMax(), is(expectedSummaryStatistics.getMax()));
      assertThat(summaryStatistics.getMin(), is(expectedSummaryStatistics.getMin()));
      assertThat(summaryStatistics.getAverage(), is(closeTo(expectedSummaryStatistics.getAverage(), 0.5D)));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, TerminalOperation.SUMMARY_STATISTICS);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testAnyMatch() throws Exception {
    IntPredicate predicate = i -> i == 0;
    tryStream(stream -> {
      assertThrows(() -> stream.anyMatch(null), NullPointerException.class);
      assertThat(stream.anyMatch(predicate), is(getExpectedStream().anyMatch(predicate)));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_ANY_MATCH);
    });
  }

  @Test
  public void testAllMatch() throws Exception {
    IntPredicate predicate = i -> i == 0;
    tryStream(stream -> {
      assertThrows(() -> stream.allMatch(null), NullPointerException.class);
      assertThat(stream.allMatch(predicate), is(getExpectedStream().allMatch(predicate)));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_ALL_MATCH);
    });
  }

  @Test
  public void testNoneMatch() throws Exception {
    IntPredicate predicate = i -> i == 0;
    tryStream(stream -> {
      assertThrows(() -> stream.noneMatch(null), NullPointerException.class);
      assertThat(stream.noneMatch(predicate), is(getExpectedStream().noneMatch(predicate)));
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.INT_NONE_MATCH);
    });
  }

  @Test
  public void testFindFirst() throws Exception {
    tryStream(stream -> {
      assertThat(stream.sorted().findFirst(), is(getExpectedStream().sorted().findFirst()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, TerminalOperation.FIND_FIRST);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testFindAny() throws Exception {
    tryStream(stream -> {
      assertThat(stream.sorted().findAny(), is(getExpectedStream().sorted().findAny()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, TerminalOperation.FIND_ANY);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testAsDoubleStream() throws Exception {
    tryStream(stream -> {
      DoubleStream doubleStream = stream.asDoubleStream();
      assertThat(doubleStream, is(instanceOf(RemoteDoubleStream.class)));
      assertThat(doubleStream.sum(), is(getExpectedStream().asDoubleStream().sum()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.AS_DOUBLE_STREAM);
      assertTerminalPortableOp(stream, TerminalOperation.SUM);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testAsLongStream() throws Exception {
    tryStream(stream -> {
      LongStream longStream = stream.asLongStream();
      assertThat(longStream, is(instanceOf(RemoteLongStream.class)));
      assertThat(longStream.sum(), is(getExpectedStream().asLongStream().sum()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.AS_LONG_STREAM);
      assertTerminalPortableOp(stream, TerminalOperation.SUM);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testBoxed() throws Exception {
    tryStream(stream -> {
      Stream<Integer> boxedStream = stream.boxed();
      assertThat(boxedStream, is(instanceOf(RemoteReferenceStream.class)));
      assertThat(boxedStream.count(), is(getExpectedStream().boxed().count()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.BOXED);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSequential() throws Exception {
    tryStream(stream -> {
      IntStream intStream = stream.sequential();
      assertThat(intStream, is(instanceOf(RemoteIntStream.class)));
      assertThat(intStream.isParallel(), is(false));
      assertThat(intStream.count(), is(getExpectedStream().sequential().count()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.SEQUENTIAL);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testParallel() throws Exception {
    tryStream(stream -> {
      IntStream intStream = stream.parallel();
      assertThat(intStream, is(instanceOf(RemoteIntStream.class)));
      assertThat(intStream.isParallel(), is(true));
      assertThat(intStream.count(), is(getExpectedStream().parallel().count()));
      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.PARALLEL);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testIsParallel() throws Exception {
    tryStream(stream -> {
      assertFalse(stream.isParallel());
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testUnordered() throws Exception {
    tryStream(stream -> {
      IntStream intStream = stream.unordered();
      assertThat(intStream, is(instanceOf(RemoteIntStream.class)));
      assertFalse(stream.getRootStream().isStreamOrdered());

      IntStream sortedIntStream = intStream.sorted();
      assertTrue(stream.getRootStream().isStreamOrdered());

      assertPortableOps(stream, MAP_TO_INT, IntermediateOperation.UNORDERED, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testIterator() throws Exception {
    tryStream(stream -> {
      PrimitiveIterator.OfInt iterator = stream.iterator();
      assertThat(iterator, is(instanceOf(PrimitiveIterator.OfInt.class)));
      assertContainsInAnyOrder(iterator, getExpectedStream().iterator());
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.ITERATOR);
    });
  }

  @Test
  public void testSpliterator() throws Exception {
    tryStream(stream -> {
      Spliterator.OfInt spliterator = stream.spliterator();
      assertThat(spliterator, is(instanceOf(Spliterator.OfInt.class)));
      assertContainsInAnyOrder(spliterator, getExpectedStream().iterator());
      assertPortableOps(stream, MAP_TO_INT);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.SPLITERATOR);
    });
  }


  /**
   * Compares the contents, in any order, of the two {@link Iterator}s provided.
   *
   * @param actualValues an {@code Iterator} over the  actual values
   * @param expectedValues an {@code Iterator} over the expected values
   */
  private void assertContainsInAnyOrder(PrimitiveIterator.OfInt actualValues, PrimitiveIterator.OfInt expectedValues) {
    Spliterator.OfInt actualSpliterator = spliteratorUnknownSize(actualValues, Spliterator.IMMUTABLE);
    assertContainsInAnyOrder(actualSpliterator, expectedValues);
  }

  /**
   * Compares the contents, in any order, of the {@link Spliterator.OfInt} and the
   * {@link PrimitiveIterator.OfInt} provided.
   *
   * @param actualValues an {@code Spliterator} over the  actual values
   * @param expectedValues an {@code Iterator} over the expected values
   */
  private void assertContainsInAnyOrder(Spliterator.OfInt actualValues, PrimitiveIterator.OfInt expectedValues) {
    List<Integer> actual = intStream(actualValues, false).mapToObj(Integer::valueOf).collect(toList());
    Integer[] expected = stream(Spliterators.spliteratorUnknownSize(expectedValues, 0), false)
        .toArray(Integer[]::new);
    assertThat(actual, containsInAnyOrder(expected));
  }


  @Override
  @SuppressWarnings("unchecked")
  protected RemoteIntStream<String> getTestStream() {
    return (RemoteIntStream<String>)animalDataset.getStream()
        .mapToInt(Animals.Schema.STATUS_LEVEL.intValueOr(-1));    // unchecked
  }

  @Override
  protected IntStream getExpectedStream() {
    return Animals.recordStream()
        .mapToInt(Animals.Schema.STATUS_LEVEL.intValueOr(-1));
  }
}
