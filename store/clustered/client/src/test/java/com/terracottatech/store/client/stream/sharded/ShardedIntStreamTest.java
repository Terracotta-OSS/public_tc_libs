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

package com.terracottatech.store.client.stream.sharded;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.collection.IsArrayContainingInAnyOrder.arrayContainingInAnyOrder;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShardedIntStreamTest {

  @Test
  public void testFilterIsDoneInParallel() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.filter(c -> c >= 3);

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(3, 4));
  }

  @Test
  public void testMapWhenUnorderedIsDoneInParallel() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.map(c -> c * 10);

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(10, 20, 30, 40));
  }

  @Test
  public void testMapWhenOrderedIsMerged() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    IntStream derived = stream.map(c -> c * 10);

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(10, 20, 30, 40));
  }

  @Test
  public void testMapToObjWhenUnorderedIsDoneInParallel() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    Stream<String> derived = stream.mapToObj(Integer::toString);

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder("1", "2", "3", "4"));
  }

  @Test
  public void testMapToObjWhenOrderedIsMerged() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    Stream<String> derived = stream.mapToObj(Integer::toString);

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.collect(toList()), contains("1", "2", "3", "4"));
  }

  @Test
  public void testMapToLongWhenUnorderedIsDoneInParallel() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    LongStream derived = stream.mapToLong(c -> Long.MAX_VALUE - c);

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(Long.MAX_VALUE - 1L, Long.MAX_VALUE - 2L, Long.MAX_VALUE - 3L, Long.MAX_VALUE - 4L));
  }

  @Test
  public void testMapToLongWhenOrderedIsMerged() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    LongStream derived = stream.mapToLong(c -> Long.MAX_VALUE - c);

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(Long.MAX_VALUE - 1L, Long.MAX_VALUE - 2L, Long.MAX_VALUE - 3L, Long.MAX_VALUE - 4L));
  }

  @Test
  public void testMapToDoubleWhenUnorderedIsDoneInParallel() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    DoubleStream derived = stream.mapToDouble(c -> c * 1.5);

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1.5, 3.0, 4.5, 6.0));
  }

  @Test
  public void testMapToDoubleWhenOrderedIsMerged() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    DoubleStream derived = stream.mapToDouble(c -> c * 1.5);

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(1.5, 3.0, 4.5, 6.0));
  }

  @Test
  public void testFlatMapWhenUnorderedIsDoneInParallel() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.flatMap(c -> IntStream.iterate(c, i -> i + c).limit(10));

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(
        1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
        2,  4,  6,  8, 10, 12, 14, 16, 18, 20,
        3,  6,  9, 12, 15, 18, 21, 24, 27, 30,
        4,  8, 12, 16, 20, 24, 28, 32, 36, 40));
  }

  @Test
  public void testFlatMapWhenOrderedIsMerged() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    IntStream derived = stream.flatMap(c -> IntStream.iterate(c, i -> i + c).limit(10));

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(
        1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
        2,  4,  6,  8, 10, 12, 14, 16, 18, 20,
        3,  6,  9, 12, 15, 18, 21, 24, 27, 30,
        4,  8, 12, 16, 20, 24, 28, 32, 36, 40));
  }

  @Test
  public void testDistinctIsMerged() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 1, 3), IntStream.of(3, 2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);
    assertThat(stream, instanceOf(ShardedIntStream.class));

    IntStream derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1, 2, 3, 4));
  }

  @Test
  public void testDistinctWhenOrderedPreservesOrdering() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 1, 3), IntStream.of(3, 2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    IntStream derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(1, 2, 3, 4));
  }

  @Test
  public void testNaturalSortingShardsAndMergesCorrectly() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 1, 3), IntStream.of(3, 2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.sorted();

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), contains(1, 1, 2, 3, 3, 4));
  }

  @Test
  public void testPeekIsShardedAndVisited() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    List<Integer> integers = new ArrayList<>();
    IntStream derived = stream.peek(integers::add);

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(integers, containsInAnyOrder(derived.boxed().toArray(Integer[]::new)));
  }

  @Test
  public void testLimitPullsMinimallyFromShardsAndMerges() {
    List<Integer> consumed = new ArrayList<>();
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3).peek(consumed::add), IntStream.of(2, 4).peek(consumed::add));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.limit(1);

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), either(contains(1)).or(contains(2)));
    assertThat(consumed, either(contains(1)).or(contains(2)));
  }

  @Test
  public void testSkipMergesImmediately() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.skip(1);

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), either(containsInAnyOrder(3, 2, 4)).or(containsInAnyOrder(1, 3, 4)));
  }

  @Test
  public void testForEachIsShardedEvenWhenOrdered() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    doAnswer(RETURNS_SELF).when(a).sorted();
    doAnswer(RETURNS_SELF).when(b).sorted();
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    List<Integer> output = new ArrayList<>();
    IntConsumer add = output::add;
    stream.forEach(add);

    verify(a).forEach(add);
    verify(b).forEach(add);
    assertThat(output, containsInAnyOrder(1, 2, 3, 4));
  }

  @Test
  public void testForEachOrderedIsOrdered() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    List<Integer> output = new ArrayList<>();
    stream.forEachOrdered(output::add);

    assertThat(output, contains(1, 2, 3, 4));
  }

  @Test
  public void testToArrayIsOrdered() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    assertThat(stream(stream.toArray()).boxed().toArray(), arrayContaining(1, 2, 3, 4));
  }

  @Test
  public void testToArrayIsSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    assertThat(stream(stream.toArray()).boxed().toArray(), arrayContainingInAnyOrder(1, 2, 3, 4));
    verify(a).toArray();
    verify(b).toArray();
  }

  @Test
  public void testIdentityReduceIsOrderedCorrectly() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 4)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(3, 2)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    assertThat(stream.reduce(0, (x, y) -> {
      assertThat(x, lessThan(y));
      return y;
    }), is(4));
    verify(a, never()).reduce(anyInt(), any(IntBinaryOperator.class));
    verify(b, never()).reduce(anyInt(), any(IntBinaryOperator.class));
  }

  @Test
  public void testIdentityReduceCanBeSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntBinaryOperator accumulator = Math::addExact;
    assertThat(stream.reduce(0, accumulator), is(10));
    verify(a).reduce(0, accumulator);
    verify(b).reduce(0, accumulator);
  }


  @Test
  public void testReduceIsOrderedCorrectly() {
    IntStream[] sortedShards = new IntStream[2];

    IntStream aOriginal = IntStream.of(1, 3);
    IntStream a = mock(IntStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(IntStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    IntStream bOriginal = IntStream.of(2, 4);
    IntStream b = mock(IntStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(IntStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    assertThat(stream.reduce((x, y) -> {
      assertThat(x, lessThan(y));
      return y;
    }), is(OptionalInt.of(4)));
    verify(sortedShards[0], never()).reduce(any(IntBinaryOperator.class));
    verify(sortedShards[1], never()).reduce(any(IntBinaryOperator.class));
  }

  @Test
  public void testReduceCanBeSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntBinaryOperator accumulator = Math::addExact;
    assertThat(stream.reduce(accumulator), is(OptionalInt.of(10)));
    verify(a).reduce(accumulator);
    verify(b).reduce(accumulator);
  }

  @Test
  public void testCollectIsOrderedCorrectly() {
    IntStream[] sortedShards = new IntStream[2];

    IntStream aOriginal = IntStream.of(1, 3);
    IntStream a = mock(IntStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(IntStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    IntStream bOriginal = IntStream.of(2, 4);
    IntStream b = mock(IntStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(IntStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    assertThat(stream.collect(ArrayList::new, List::add, List::addAll), contains(1, 2, 3, 4));
    verify(sortedShards[0], never()).collect(any(Supplier.class), any(ObjIntConsumer.class), any(BiConsumer.class));
    verify(sortedShards[1], never()).collect(any(Supplier.class), any(ObjIntConsumer.class), any(BiConsumer.class));
  }

  @Test
  public void testCollectCanBeSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    Supplier<List<Integer>> supplier = ArrayList::new;
    ObjIntConsumer<List<Integer>> accumulator = List::add;
    BiConsumer<List<Integer>, List<Integer>> combiner = List::addAll;
    assertThat(stream.collect(supplier, accumulator, combiner), containsInAnyOrder(1, 2, 3, 4));
    verify(a).collect(supplier, accumulator, combiner);
    verify(b).collect(supplier, accumulator, combiner);
  }

  @Test
  public void testSumIsSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    assertThat(stream.sum(), is(10));

    verify(a).sum();
    verify(b).sum();

  }

  @Test
  public void testSummaryStatisticsIsSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntSummaryStatistics statistics = stream.summaryStatistics();
    assertThat(statistics.getCount(), is(4L));
    assertThat(statistics.getMin(), is(1));
    assertThat(statistics.getMax(), is(4));
    assertThat(statistics.getSum(), is(10L));
    assertThat(statistics.getAverage(), is(2.5));

    verify(a).summaryStatistics();
    verify(b).summaryStatistics();
  }

  @Test
  public void testMinIsSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    assertThat(stream.min(), is(OptionalInt.of(1)));

    verify(a).min();
    verify(b).min();
  }

  @Test
  public void testMaxIsSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    assertThat(stream.max(), is(OptionalInt.of(4)));

    verify(a).max();
    verify(b).max();
  }

  @Test
  public void testCountIsSharded() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    assertThat(stream.count(), is(4L));

    verify(a).count();
    verify(b).count();
  }

  @Test
  public void testAnyMatchShortCircuits() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntPredicate predicate = i -> true;
    assertTrue(stream.anyMatch(predicate));

    try {
      verify(a).anyMatch(predicate);
      verify(b, never()).anyMatch(any(IntPredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).anyMatch(any(IntPredicate.class));
        verify(b).anyMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testAnyMatchIsExhaustive() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntPredicate predicate = i -> i == 5;
    assertFalse(stream.anyMatch(predicate));

    verify(a).anyMatch(predicate);
    verify(b).anyMatch(predicate);
  }

  @Test
  public void testAllMatchShortCircuits() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntPredicate predicate = i -> false;
    assertFalse(stream.allMatch(predicate));

    try {
      verify(a).allMatch(predicate);
      verify(b, never()).allMatch(any(IntPredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).allMatch(any(IntPredicate.class));
        verify(b).allMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testAllMatchIsExhaustive() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntPredicate predicate = i -> i >= 0;
    assertTrue(stream.allMatch(predicate));

    verify(a).allMatch(predicate);
    verify(b).allMatch(predicate);
  }

  @Test
  public void testNoneMatchShortCircuits() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntPredicate predicate = i -> true;
    assertFalse(stream.noneMatch(predicate));

    try {
      verify(a).noneMatch(predicate);
      verify(b, never()).noneMatch(any(IntPredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).noneMatch(any(IntPredicate.class));
        verify(b).noneMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testNoneMatchIsExhaustive() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntPredicate predicate = i -> i == 5;
    assertTrue(stream.noneMatch(predicate));

    verify(a).noneMatch(predicate);
    verify(b).noneMatch(predicate);
  }

  @Test
  public void testFindFirstIsOrderedCorrectly() {
    IntStream[] sortedShards = new IntStream[2];

    IntStream aOriginal = IntStream.of(3, 1);
    IntStream a = mock(IntStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(IntStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    IntStream bOriginal = IntStream.of(2, 4);
    IntStream b = mock(IntStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(IntStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();

    assertThat(stream.findFirst(), is(OptionalInt.of(1)));
    verify(sortedShards[0]).findFirst();
    verify(sortedShards[1]).findFirst();
  }

  @Test
  public void testUnorderedFindFirstIsFindAny() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    assertThat(stream.findFirst().getAsInt(), anyOf(is(1), is(2)));

    verify(a, never()).findFirst();
    verify(b, never()).findFirst();

    try {
      verify(a).findAny();
      verify(b, never()).findAny();
    } catch (Throwable e) {
      try {
        verify(a, never()).findAny();
        verify(b).findAny();
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testFindAnyShortCircuits() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    try {
      assertThat(stream.findAny().getAsInt(), anyOf(is(1), is(3)));
      verify(a).findAny();
      verify(b, never()).findAny();
    } catch (Throwable e) {
      try {
        assertThat(stream.findAny().getAsInt(), anyOf(is(2), is(4)));
        verify(a, never()).findAny();
        verify(b).findAny();
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testFindAnyIsExhaustive() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.empty()));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.empty()));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    assertThat(stream.findAny(), is(OptionalInt.empty()));

    verify(a).findAny();
    verify(b).findAny();
  }

  @Test
  public void testAsLongStreamWhenCustomOrderedIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Comparator<Integer> ordering = (x, y) -> Integer.compare(y, x);
    ShardedIntStream stream = new ShardedIntStream(shards.map(s -> s.boxed().sorted(ordering).mapToInt(r -> r)), ordering, ORDERED, source);

    LongStream derived = stream.asLongStream();

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(4L, 3L, 2L, 1L));
  }

  @Test
  public void testAsLongStreamWhenNaturalOrderedIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    LongStream derived = stream.sorted().asLongStream();

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(((ShardedLongStream) derived).getOrdering(), is(naturalOrder()));
    assertThat(derived.boxed().collect(toList()), contains(1L, 2L, 3L, 4L));
  }

  @Test
  public void testAsLongStreamCanBeSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    LongStream derived = stream.asLongStream();

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1L, 2L, 3L, 4L));
  }

  @Test
  public void testAsDoubleStreamWhenCustomOrderedIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Comparator<Integer> ordering = (x, y) -> Integer.compare(y, x);
    ShardedIntStream stream = new ShardedIntStream(shards.map(s -> s.boxed().sorted(ordering).mapToInt(r -> r)), ordering, ORDERED, source);

    DoubleStream derived = stream.asDoubleStream();

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(4.0, 3.0, 2.0, 1.0));
  }

  @Test
  public void testAsDoubleStreamWhenNaturalOrderedIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    DoubleStream derived = stream.sorted().asDoubleStream();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(((ShardedDoubleStream) derived).getOrdering(), is(naturalOrder()));
    assertThat(derived.boxed().collect(toList()), contains(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testAsDoubleStreamCanBeSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    DoubleStream derived = stream.asDoubleStream();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testBoxedIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    Stream<Integer> derived = stream.boxed();

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder(1, 2, 3, 4));
  }

  @Test
  public void testOrderedIteratorIsOrdered() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    List<Integer> assembled = new ArrayList<>();
    for (PrimitiveIterator.OfInt it = stream.iterator(); it.hasNext(); ) {
      assembled.add(it.next());
    }
    assertThat(assembled, contains(1, 2, 3, 4));
  }

  @Test
  public void testUnorderedIteratorIsLazy() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    int seen = stream.iterator().next();

    //Merged iterator implementation is backed by spliterators, hence the mismatching verifications.
    try {
      assertThat(seen, anyOf(is(1), is(3)));
      verify(a).spliterator();
      verify(b, never()).spliterator();
    } catch (Throwable e) {
      try {
        assertThat(seen, anyOf(is(2), is(4)));
        verify(a, never()).spliterator();
        verify(b).spliterator();
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testOrderedSpliteratorIsOrdered() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedIntStream.class));

    List<Integer> assembled = new ArrayList<>();
    Spliterator.OfInt split = stream.spliterator();
    while (split.tryAdvance((IntConsumer) s -> assembled.add(s)));
    assertThat(assembled, contains(1, 2, 3, 4));
  }

  @Test
  public void testUnorderedSpliteratorIsLazy() {
    IntStream a = mock(IntStream.class, delegatesTo(IntStream.of(1, 3)));
    IntStream b = mock(IntStream.class, delegatesTo(IntStream.of(2, 4)));
    Stream<IntStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source);

    int[] seen = new int[1];
    stream.spliterator().tryAdvance((IntConsumer) s -> seen[0] = s);

    try {
      assertThat(seen[0], anyOf(is(1), is(3)));
      verify(a).spliterator();
      verify(b, never()).spliterator();
    } catch (Throwable e) {
      try {
        assertThat(seen[0], anyOf(is(2), is(4)));
        verify(a, never()).spliterator();
        verify(b).spliterator();
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testUnorderedSpliteratorIsSplitInToShardPiecesWhenParallel() {
    IntStream a = IntStream.of(1, 2);
    IntStream b = IntStream.of(3, 4);
    IntStream c = IntStream.of(5, 6);
    IntStream d = IntStream.of(7, 8);
    IntStream e = IntStream.of(9, 10);
    IntStream f = IntStream.of(11, 12);
    IntStream g = IntStream.of(13, 14);
    IntStream h = IntStream.of(15, 16);
    Stream<IntStream> shards = Stream.of(a, b, c, d, e, f, g, h);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    IntStream stream = new ShardedIntStream(shards, null, 0, source).parallel();

    Spliterator.OfInt hSplit = requireNonNull(stream.spliterator());

    Spliterator.OfInt dSplit = requireNonNull(hSplit.trySplit());

    Spliterator.OfInt bSplit = requireNonNull(dSplit.trySplit());
    Spliterator.OfInt fSplit = requireNonNull(hSplit.trySplit());

    Spliterator.OfInt aSplit = requireNonNull(bSplit.trySplit());
    Spliterator.OfInt cSplit = requireNonNull(dSplit.trySplit());
    Spliterator.OfInt eSplit = requireNonNull(fSplit.trySplit());
    Spliterator.OfInt gSplit = requireNonNull(hSplit.trySplit());


    List<Integer> aList = new ArrayList<>();
    aSplit.forEachRemaining((IntConsumer) aList::add);
    assertThat(aList, containsInAnyOrder(1, 2));

    List<Integer> bList = new ArrayList<>();
    bSplit.forEachRemaining((IntConsumer) bList::add);
    assertThat(bList, containsInAnyOrder(3, 4));

    List<Integer> cList = new ArrayList<>();
    cSplit.forEachRemaining((IntConsumer) cList::add);
    assertThat(cList, containsInAnyOrder(5, 6));

    List<Integer> dList = new ArrayList<>();
    dSplit.forEachRemaining((IntConsumer) dList::add);
    assertThat(dList, containsInAnyOrder(7, 8));

    List<Integer> eList = new ArrayList<>();
    eSplit.forEachRemaining((IntConsumer) eList::add);
    assertThat(eList, containsInAnyOrder(9, 10));

    List<Integer> fList = new ArrayList<>();
    fSplit.forEachRemaining((IntConsumer) fList::add);
    assertThat(fList, containsInAnyOrder(11, 12));

    List<Integer> gList = new ArrayList<>();
    gSplit.forEachRemaining((IntConsumer) gList::add);
    assertThat(gList, containsInAnyOrder(13, 14));

    List<Integer> hList = new ArrayList<>();
    hSplit.forEachRemaining((IntConsumer) hList::add);
    assertThat(hList, containsInAnyOrder(15, 16));
  }

  @Test
  public void testSequentialIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.sequential();

    assertThat(derived, instanceOf(ShardedIntStream.class));

    assertFalse(derived.isParallel());
  }

  @Test
  public void testParallelIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.parallel();

    assertThat(derived, instanceOf(ShardedIntStream.class));

    assertTrue(derived.isParallel());
  }

  @Test
  public void testUnorderedIsSharded() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedIntStream stream = new ShardedIntStream(shards, null, 0, source);

    IntStream derived = stream.unordered();

    assertThat(derived, instanceOf(ShardedIntStream.class));
  }

}
