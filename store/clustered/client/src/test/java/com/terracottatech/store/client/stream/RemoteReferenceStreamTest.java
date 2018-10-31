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

import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.test.data.Animals;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP;
import static java.util.Comparator.nullsFirst;
import static java.util.Comparator.nullsLast;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests against {@link RemoteReferenceStream}.
 */
public class RemoteReferenceStreamTest extends AbstractRemoteStreamTest<String, String, Stream<String>, RemoteReferenceStream<String, String>> {

  @SuppressWarnings("unchecked")
  public RemoteReferenceStreamTest() {
    super(MAP, (Class)RemoteReferenceStream.class, (Class)Stream.class);   // unchecked
  }


  @Test
  public void testFilter() throws Exception {
    Predicate<String> predicate = String::isEmpty;
    tryStream(stream -> {
      assertThrows(() -> stream.filter(null), NullPointerException.class);
      Stream<String> objStream = stream.filter(predicate);
      assertThat(objStream, is(not(instanceOf(RemoteReferenceStream.class))));
      assertThat(objStream, is(instanceOf(Stream.class)));
      assertThat(objStream.count(), is(getExpectedStream().filter(predicate).count()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FILTER, TerminalOperation.COUNT);
    });
  }

  @Test
  public void testMap() throws Exception {
    Function<String, Integer> mapper = String::length;
    tryStream(stream -> {
      assertThrows(() -> stream.map(null), NullPointerException.class);
      Stream<Integer> objStream = stream.map(mapper);
      assertThat(objStream, is(not(instanceOf(RemoteReferenceStream.class))));
      assertThat(objStream, is(instanceOf(Stream.class)));
      assertThat(objStream.count(), is(getExpectedStream().map(mapper).count()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP, TerminalOperation.COUNT);
    });
  }

  @Test
  public void testMapToDouble() throws Exception {
    ToDoubleFunction<String> mapper = s -> (double)s.length();
    tryStream(stream -> {
      assertThrows(() -> stream.mapToDouble(null), NullPointerException.class);
      DoubleStream doubleStream = stream.mapToDouble(mapper);
      assertThat(doubleStream, is(not(instanceOf(RemoteDoubleStream.class))));
      assertThat(doubleStream, is(instanceOf(DoubleStream.class)));
      assertThat(doubleStream.sum(), is(getExpectedStream().mapToDouble(mapper).sum()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP_TO_DOUBLE, TerminalOperation.SUM);
    });
  }

  @Test
  public void testMapToInt() throws Exception {
    ToIntFunction<String> mapper = String::hashCode;
    tryStream(stream -> {
      assertThrows(() -> stream.mapToInt(null), NullPointerException.class);
      IntStream intStream = stream.mapToInt(mapper);
      assertThat(intStream, is(not(instanceOf(RemoteIntStream.class))));
      assertThat(intStream, is(instanceOf(IntStream.class)));
      assertThat(intStream.sum(), is(getExpectedStream().mapToInt(mapper).sum()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP_TO_INT, TerminalOperation.SUM);
    });
  }

  @Test
  public void testMapToLong() throws Exception {
    ToLongFunction<String> mapper = s -> (long)s.hashCode();
    tryStream(stream -> {
      assertThrows(() -> stream.mapToLong(null), NullPointerException.class);
      LongStream longStream = stream.mapToLong(mapper);
      assertThat(longStream, is(not(instanceOf(RemoteLongStream.class))));
      assertThat(longStream, is(instanceOf(LongStream.class)));
      assertThat(longStream.sum(), is(getExpectedStream().mapToLong(mapper).sum()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.MAP_TO_LONG, TerminalOperation.SUM);
    });
  }

  @Test
  public void testFlatMap() throws Exception {
    Function<String, Stream<? extends Character>> mapper = s -> s.chars().mapToObj(c -> (char)c);
    tryStream(stream -> {
      assertThrows(() -> stream.flatMap(null), NullPointerException.class);
      Stream<Character> objStream = stream.flatMap(mapper);
      assertThat(objStream, is(not(instanceOf(RemoteReferenceStream.class))));
      assertThat(objStream, is(instanceOf(Stream.class)));
      assertThat(objStream.count(), is(getExpectedStream().flatMap(mapper).count()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP, TerminalOperation.COUNT);
    });
  }

  @Test
  public void testFlatMapToDouble() throws Exception {
    Function<String, DoubleStream> mapper = s -> DoubleStream.of((double)s.length());
    tryStream(stream -> {
      assertThrows(() -> stream.flatMapToDouble(null), NullPointerException.class);
      DoubleStream doubleStream = stream.flatMapToDouble(mapper);
      assertThat(doubleStream, is(not(instanceOf(RemoteDoubleStream.class))));
      assertThat(doubleStream, is(instanceOf(DoubleStream.class)));
      assertThat(doubleStream.sum(), is(getExpectedStream().flatMapToDouble(mapper).sum()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP_TO_DOUBLE, TerminalOperation.SUM);
    });
  }

  @Test
  public void testFlatMapToInt() throws Exception {
    Function<String, IntStream> mapper = CharSequence::chars;
    tryStream(stream -> {
      assertThrows(() -> stream.flatMapToInt(null), NullPointerException.class);
      IntStream intStream = stream.flatMapToInt(mapper);
      assertThat(intStream, is(not(instanceOf(RemoteIntStream.class))));
      assertThat(intStream, is(instanceOf(IntStream.class)));
      assertThat(intStream.sum(), is(getExpectedStream().flatMapToInt(mapper).sum()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP_TO_INT, TerminalOperation.SUM);
    });
  }

  @Test
  public void testFlatMapToLong() throws Exception {
    Function<String, LongStream> mapper = s -> LongStream.of((long)s.hashCode());
    tryStream(stream -> {
      assertThrows(() -> stream.flatMapToLong(null), NullPointerException.class);
      LongStream longStream = stream.flatMapToLong(mapper);
      assertThat(longStream, is(not(instanceOf(RemoteLongStream.class))));
      assertThat(longStream, is(instanceOf(LongStream.class)));
      assertThat(longStream.sum(), is(getExpectedStream().flatMapToLong(mapper).sum()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.FLAT_MAP_TO_LONG, TerminalOperation.SUM);
    });
  }

  @Test
  public void testDistinct() throws Exception {
    tryStream(stream -> {
      Stream<String> objStream = stream.distinct();
      assertThat(objStream.count(), is(getExpectedStream().distinct().count()));
      assertPortableOps(stream, MAP, IntermediateOperation.DISTINCT);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSorted0Arg() throws Exception {
    tryStream(stream -> {
      Stream<String> objStream = stream.sorted();
      assertThat(objStream, is(instanceOf(RemoteReferenceStream.class)));
      assertThat(objStream.toArray(), is(getExpectedStream().sorted().toArray()));
      assertPortableOps(stream, MAP, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.TO_ARRAY_0);
    });
  }

  @Test
  public void testSorted1Arg() throws Exception {
    // TODO: Convert this to use a portable comparator
    Comparator<String> comparator = nullsFirst(String.CASE_INSENSITIVE_ORDER);
    tryStream(stream -> {
      assertThrows(() -> stream.sorted(null), NullPointerException.class);
      Stream<String> objStream = stream.sorted(comparator);
      assertThat(objStream, is(not(instanceOf(RemoteReferenceStream.class))));
      assertThat(objStream.toArray(), is(getExpectedStream().sorted(comparator).toArray()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.SORTED_1, TerminalOperation.TO_ARRAY_0);
    });
  }

  @Test
  public void testPeek() throws Exception {
    Consumer<String> consumer = i -> { };
    tryStream(stream -> {
      assertThrows(() -> stream.peek(null), NullPointerException.class);
      Stream<String> objStream = stream.peek(consumer);
      assertThat(objStream, is(not(instanceOf(RemoteReferenceStream.class))));
      assertThat(objStream, is(instanceOf(Stream.class)));
      assertThat(objStream.count(), is(getExpectedStream().peek(consumer).count()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, IntermediateOperation.PEEK, TerminalOperation.COUNT);
    });
  }

  @Test
  public void testLimit() throws Exception {
    int maxSize = 5;
    tryStream(stream -> {
      assertThrows(() -> stream.limit(-1), IllegalArgumentException.class);
      Stream<String> objStream = stream.limit(maxSize);
      assertThat(objStream.count(), is(getExpectedStream().limit(maxSize).count()));
      assertPortableOps(stream, MAP, IntermediateOperation.LIMIT);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSkip() throws Exception {
    int n = 5;
    tryStream(stream -> {
      assertThrows(() -> stream.skip(-1), IllegalArgumentException.class);
      Stream<String> objStream = stream.skip(n);
      assertThat(objStream.count(), is(getExpectedStream().skip(n).count()));
      assertPortableOps(stream, MAP, IntermediateOperation.SKIP);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testForEach() throws Exception {
    Consumer<String> consumer = s -> { };
    tryStream(stream -> {
      assertThrows(() -> stream.forEach(null), NullPointerException.class);
      stream.forEach(consumer);
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.FOR_EACH);
    });
  }

  @Test
  public void testForEachOrdered() throws Exception {
    Consumer<String> consumer = s -> { };
    tryStream(stream -> {
      assertThrows(() -> stream.forEachOrdered(null), NullPointerException.class);
      stream.forEachOrdered(consumer);
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.FOR_EACH_ORDERED);
    });
  }

  @Test
  public void testToArray0Arg() throws Exception {
    tryStream(stream -> {
      assertThat(stream.toArray(), arrayContainingInAnyOrder(getExpectedStream().toArray()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.TO_ARRAY_0);
    });
  }

  @Test
  public void testToArray1Arg() throws Exception {
    IntFunction<String[]> generator = String[]::new;
    tryStream(stream -> {
      assertThrows(() -> stream.toArray(null), NullPointerException.class);
      assertThat(stream.toArray(generator), arrayContainingInAnyOrder(getExpectedStream().toArray(generator)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.TO_ARRAY_1);
    });
  }

  @Test
  public void testReduce1Arg() throws Exception {
    BinaryOperator<String> accumulator = (s1, s2) -> nullsFirst(String.CASE_INSENSITIVE_ORDER).compare(s1, s2) > 0 ? s2 : s1;
    tryStream(stream -> {
      assertThrows(() -> stream.reduce(null), NullPointerException.class);
      assertThat(stream.reduce(accumulator), is(getExpectedStream().reduce(accumulator)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.REDUCE_1);
    });
  }

  @Test
  public void testReduce2Arg() throws Exception {
    String identity = "";
    BinaryOperator<String> accumulator = (s1, s2) -> nullsFirst(String.CASE_INSENSITIVE_ORDER).compare(s1, s2) > 0 ? s2 : s1;
    tryStream(stream -> {
      // identity can be null
      assertThrows(() -> stream.reduce(identity, null), NullPointerException.class);
      assertThat(stream.reduce(identity, accumulator), is(getExpectedStream().reduce(identity, accumulator)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.REDUCE_2);
    });
  }

  @Test
  public void testReduce3Arg() throws Exception {
    String identity = "";
    BiFunction<String, String, String> accumulator = (s1, s2) -> nullsFirst(String.CASE_INSENSITIVE_ORDER).compare(s1, s2) > 0 ? s2 : s1;
    BinaryOperator<String> combiner = (s1, s2) -> nullsFirst(String.CASE_INSENSITIVE_ORDER).compare(s1, s2) > 0 ? s2 : s1;
    tryStream(stream -> {
      // identity can be null
      assertThrows(() -> stream.reduce(identity, null, combiner), NullPointerException.class);
      assertThrows(() -> stream.reduce(identity, accumulator, null), NullPointerException.class);
      assertThat(stream.reduce(identity, accumulator, combiner), is(getExpectedStream().reduce(identity, accumulator, combiner)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.REDUCE_3);
    });
  }

  @Test
  public void testCollect1Arg() throws Exception {
    Collector<String, ?, List<String>> collector = toList();
    tryStream(stream -> {
      assertThrows(() -> stream.collect(null), NullPointerException.class);
      assertThat(stream.collect(collector), containsInAnyOrder(getExpectedStream().collect(collector).toArray()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.COLLECT_1);
    });
  }

  @Test
  public void testCollect3Arg() throws Exception {
    Supplier<ArrayList<String>> supplier = ArrayList<String>::new;
    BiConsumer<ArrayList<String>, String> accumulator = ArrayList<String>::add;
    BiConsumer<ArrayList<String>, ArrayList<String>> combiner = ArrayList<String>::addAll;
    tryStream(stream -> {
      assertThrows(() -> stream.collect(null, accumulator, combiner), NullPointerException.class);
      assertThrows(() -> stream.collect(supplier, null, combiner), NullPointerException.class);
      assertThrows(() -> stream.collect(supplier, accumulator, null), NullPointerException.class);
      assertThat(stream.collect(supplier, accumulator, combiner), containsInAnyOrder(getExpectedStream().collect(supplier, accumulator, combiner).toArray()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.COLLECT_3);
    });
  }

  @Test
  public void testMin() throws Exception {
    Comparator<String> comparator = nullsLast(String.CASE_INSENSITIVE_ORDER);
    tryStream(stream -> {
      assertThrows(() -> stream.min(null), NullPointerException.class);
      assertThat(stream.min(comparator), is(getExpectedStream().min(comparator)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.MIN_1);
    });
  }

  @Test
  public void testMax() throws Exception {
    Comparator<String> comparator = nullsFirst(String.CASE_INSENSITIVE_ORDER);
    tryStream(stream -> {
      assertThrows(() -> stream.max(null), NullPointerException.class);
      assertThat(stream.max(comparator), is(getExpectedStream().max(comparator)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.MAX_1);
    });
  }

  @Test
  public void testCount() throws Exception {
    tryStream(stream -> {
      assertThat(stream.count(), is(getExpectedStream().count()));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testAnyMatch() throws Exception {
    Predicate<String> predicate = String::isEmpty;
    tryStream(stream -> {
      assertThrows(() -> stream.anyMatch(null), NullPointerException.class);
      assertThat(stream.anyMatch(predicate), is(getExpectedStream().anyMatch(predicate)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.ANY_MATCH);
    });
  }

  @Test
  public void testAllMatch() throws Exception {
    Predicate<String> predicate = String::isEmpty;
    tryStream(stream -> {
      assertThrows(() -> stream.allMatch(null), NullPointerException.class);
      assertThat(stream.allMatch(predicate), is(getExpectedStream().allMatch(predicate)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.ALL_MATCH);
    });
  }

  @Test
  public void testNoneMatch() throws Exception {
    Predicate<String> predicate = String::isEmpty;
    tryStream(stream -> {
      assertThrows(() -> stream.noneMatch(null), NullPointerException.class);
      assertThat(stream.noneMatch(predicate), is(getExpectedStream().noneMatch(predicate)));
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.NONE_MATCH);
    });
  }

  @Test
  public void testFindFirst() throws Exception {
    tryStream(stream -> {
      assertThat(stream.sorted().findFirst(), is(getExpectedStream().sorted().findFirst()));
      assertPortableOps(stream, MAP, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, TerminalOperation.FIND_FIRST);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testFindAny() throws Exception {
    tryStream(stream -> {
      assertThat(stream.sorted().findAny(), is(getExpectedStream().sorted().findAny()));
      assertPortableOps(stream, MAP, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, TerminalOperation.FIND_ANY);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testSequential() throws Exception {
    tryStream(stream -> {
      Stream<String> objStream = stream.sequential();
      assertThat(objStream, is(instanceOf(RemoteReferenceStream.class)));
      assertThat(objStream.isParallel(), is(false));
      assertThat(objStream.count(), is(getExpectedStream().sequential().count()));
      assertPortableOps(stream, MAP, IntermediateOperation.SEQUENTIAL);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testParallel() throws Exception {
    tryStream(stream -> {
      Stream<String> objStream = stream.parallel();
      assertThat(objStream, is(instanceOf(RemoteReferenceStream.class)));
      assertThat(objStream.isParallel(), is(true));
      assertThat(objStream.count(), is(getExpectedStream().parallel().count()));
      assertPortableOps(stream, MAP, IntermediateOperation.PARALLEL);
      assertTerminalPortableOp(stream, TerminalOperation.COUNT);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testIsParallel() throws Exception {
    tryStream(stream -> {
      assertFalse(stream.isParallel());
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testUnordered() throws Exception {
    tryStream(stream -> {
      Stream<String> objStream = stream.unordered();
      assertThat(objStream, is(instanceOf(RemoteReferenceStream.class)));
      assertFalse(stream.getRootStream().isStreamOrdered());

      Stream<String> sortedObjStream = objStream.sorted();
      assertTrue(stream.getRootStream().isStreamOrdered());

      assertPortableOps(stream, MAP, IntermediateOperation.UNORDERED, IntermediateOperation.SORTED_0);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream);
    });
  }

  @Test
  public void testIterator() throws Exception {
    tryStream(stream -> {
      Iterator<String> iterator = stream.iterator();
      assertThat(iterator, is(instanceOf(Iterator.class)));
      assertContainsInAnyOrder(iterator, getExpectedStream().iterator());
      assertPortableOps(stream, MAP);
      assertTerminalPortableOp(stream, null);
      assertNonPortableOps(stream, TerminalOperation.ITERATOR);
    });
  }

  @Test
  public void testSpliterator() throws Exception {
    tryStream(stream -> {
      Spliterator<String> spliterator = stream.spliterator();
      assertThat(spliterator, is(instanceOf(Spliterator.class)));
      assertContainsInAnyOrder(spliterator, getExpectedStream().iterator());
      assertPortableOps(stream, MAP);
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
  private void assertContainsInAnyOrder(Iterator<String> actualValues, Iterator<String> expectedValues) {
    Spliterator<String> actualSpliterator = spliteratorUnknownSize(actualValues, Spliterator.IMMUTABLE);
    assertContainsInAnyOrder(actualSpliterator, expectedValues);
  }

  /**
   * Compares the contents, in any order, of the {@link Spliterator Spliterator<String>} and the
   * {@link Iterator Iterator<String>} provided.
   *
   * @param actualValues an {@code Spliterator} over the  actual values
   * @param expectedValues an {@code Iterator} over the expected values
   */
  private void assertContainsInAnyOrder(Spliterator<String> actualValues, Iterator<String> expectedValues) {
    List<String> actual = stream(actualValues, false).collect(toList());
    String[] expected = stream(Spliterators.spliteratorUnknownSize(expectedValues, 0), false)
        .toArray(String[]::new);
    assertThat(actual, containsInAnyOrder(expected));
  }


  @Override
  @SuppressWarnings("unchecked")
  protected RemoteReferenceStream<String, String> getTestStream() {
    return (RemoteReferenceStream<String, String>)animalDataset.getStream()
        .map(Animals.Schema.TAXONOMIC_CLASS.valueOr(""));    // unchecked
  }

  @Override
  protected Stream<String> getExpectedStream() {
    return Animals.recordStream()
        .map(Animals.Schema.TAXONOMIC_CLASS.valueOr(""));
  }
}
