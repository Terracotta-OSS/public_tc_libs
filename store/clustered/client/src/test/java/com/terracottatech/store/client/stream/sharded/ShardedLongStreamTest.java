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
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.ObjLongConsumer;
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShardedLongStreamTest {

  @Test
  public void testFilterIsDoneInParallel() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.filter(c -> c >= 3L);

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(3L, 4L));
  }

  @Test
  public void testMapWhenUnorderedIsDoneInParallel() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.map(c -> c * 10L);

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(10L, 20L, 30L, 40L));
  }

  @Test
  public void testMapWhenOrderedIsMerged() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    LongStream derived = stream.map(c -> c * 10L);

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(10L, 20L, 30L, 40L));
  }

  @Test
  public void testMapToObjWhenUnorderedIsDoneInParallel() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    Stream<String> derived = stream.mapToObj(Long::toString);

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder("1", "2", "3", "4"));
  }

  @Test
  public void testMapToObjWhenOrderedIsMerged() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    Stream<String> derived = stream.mapToObj(Long::toString);

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.collect(toList()), contains("1", "2", "3", "4"));
  }

  @Test
  public void testMapToIntWhenUnorderedIsDoneInParallel() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    IntStream derived = stream.mapToInt(c -> (int) (Integer.MAX_VALUE - c));

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(Integer.MAX_VALUE - 1, Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 3, Integer.MAX_VALUE - 4));
  }

  @Test
  public void testMapToIntWhenOrderedIsMerged() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    IntStream derived = stream.mapToInt(c -> (int) (Integer.MAX_VALUE - c));

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(Integer.MAX_VALUE - 1, Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 3, Integer.MAX_VALUE - 4));
  }

  @Test
  public void testMapToDoubleWhenUnorderedIsDoneInParallel() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    DoubleStream derived = stream.mapToDouble(c -> c * 1.5);

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1.5, 3.0, 4.5, 6.0));
  }

  @Test
  public void testMapToDoubleWhenOrderedIsMerged() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    DoubleStream derived = stream.mapToDouble(c -> c * 1.5);

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(1.5, 3.0, 4.5, 6.0));
  }

  @Test
  public void testFlatMapWhenUnorderedIsDoneInParallel() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.flatMap(c -> LongStream.iterate(c, i -> i + c).limit(10));

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(
        1L,  2L,  3L,  4L,  5L,  6L,  7L,  8L,  9L, 10L,
        2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L,
        3L,  6L,  9L, 12L, 15L, 18L, 21L, 24L, 27L, 30L,
        4L,  8L, 12L, 16L, 20L, 24L, 28L, 32L, 36L, 40L));
  }

  @Test
  public void testFlatMapWhenOrderedIsMerged() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    LongStream derived = stream.flatMap(c -> LongStream.iterate(c, i -> i + c).limit(10));

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(
        1L,  2L,  3L,  4L,  5L,  6L,  7L,  8L,  9L, 10L,
        2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L,
        3L,  6L,  9L, 12L, 15L, 18L, 21L, 24L, 27L, 30L,
        4L,  8L, 12L, 16L, 20L, 24L, 28L, 32L, 36L, 40L));
  }

  @Test
  public void testDistinctIsMerged() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 1L, 3L), LongStream.of(3L, 2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);
    assertThat(stream, instanceOf(ShardedLongStream.class));

    LongStream derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1L, 2L, 3L, 4L));
  }

  @Test
  public void testDistinctWhenOrderedPreservesOrdering() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 1L, 3L), LongStream.of(3L, 2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    LongStream derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(1L, 2L, 3L, 4L));
  }

  @Test
  public void testNaturalSortingShardsAndMergesCorrectly() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 1L, 3L), LongStream.of(3L, 2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.sorted();

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), contains(1L, 1L, 2L, 3L, 3L, 4L));
  }

  @Test
  public void testPeekIsShardedAndVisited() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    List<Long> longs = new ArrayList<>();
    LongStream derived = stream.peek(longs::add);

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(longs, containsInAnyOrder(derived.boxed().toArray(Long[]::new)));
  }

  @Test
  public void testLimitPullsMinimallyFromShardsAndMerges() {
    List<Long> consumed = new ArrayList<>();
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L).peek(consumed::add), LongStream.of(2L, 4L).peek(consumed::add));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.limit(1);

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), either(contains(1L)).or(contains(2L)));
    assertThat(consumed, either(contains(1L)).or(contains(2L)));
  }

  @Test
  public void testSkipMergesImmediately() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.skip(1);

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), either(containsInAnyOrder(3L, 2L, 4L)).or(containsInAnyOrder(1L, 3L, 4L)));
  }

  @Test
  public void testForEachIsShardedEvenWhenOrdered() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    doAnswer(RETURNS_SELF).when(a).sorted();
    doAnswer(RETURNS_SELF).when(b).sorted();
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    List<Long> output = new ArrayList<>();
    LongConsumer add = output::add;
    stream.forEach(add);

    verify(a).forEach(add);
    verify(b).forEach(add);
    assertThat(output, containsInAnyOrder(1L, 2L, 3L, 4L));
  }

  @Test
  public void testForEachOrderedIsOrdered() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    List<Long> output = new ArrayList<>();
    stream.forEachOrdered(output::add);

    assertThat(output, contains(1L, 2L, 3L, 4L));
  }

  @Test
  public void testToArrayIsOrdered() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    assertThat(stream(stream.toArray()).boxed().toArray(), arrayContaining(1L, 2L, 3L, 4L));
  }

  @Test
  public void testToArrayIsSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    assertThat(stream(stream.toArray()).boxed().toArray(), arrayContainingInAnyOrder(1L, 2L, 3L, 4L));
    verify(a).toArray();
    verify(b).toArray();
  }

  @Test
  public void testIdentityReduceIsOrderedCorrectly() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 4L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(3L, 2L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    assertThat(stream.reduce(0L, (x, y) -> {
      assertThat(x, lessThan(y));
      return y;
    }), is(4L));
    verify(a, never()).reduce(anyLong(), any(LongBinaryOperator.class));
    verify(b, never()).reduce(anyLong(), any(LongBinaryOperator.class));
  }

  @Test
  public void testIdentityReduceCanBeSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongBinaryOperator accumulator = Math::addExact;
    assertThat(stream.reduce(0L, accumulator), is(10L));
    verify(a).reduce(0L, accumulator);
    verify(b).reduce(0L, accumulator);
  }


  @Test
  public void testReduceIsOrderedCorrectly() {
    LongStream[] sortedShards = new LongStream[2];

    LongStream aOriginal = LongStream.of(1L, 3L);
    LongStream a = mock(LongStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(LongStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    LongStream bOriginal = LongStream.of(2L, 4L);
    LongStream b = mock(LongStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(LongStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    assertThat(stream.reduce((x, y) -> {
      assertThat(x, lessThan(y));
      return y;
    }), is(OptionalLong.of(4L)));
    verify(sortedShards[0], never()).reduce(any(LongBinaryOperator.class));
    verify(sortedShards[1], never()).reduce(any(LongBinaryOperator.class));
  }

  @Test
  public void testReduceCanBeSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongBinaryOperator accumulator = Math::addExact;
    assertThat(stream.reduce(accumulator), is(OptionalLong.of(10L)));
    verify(a).reduce(accumulator);
    verify(b).reduce(accumulator);
  }

  @Test
  public void testCollectIsOrderedCorrectly() {
    LongStream[] sortedShards = new LongStream[2];

    LongStream aOriginal = LongStream.of(1L, 3L);
    LongStream a = mock(LongStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(LongStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    LongStream bOriginal = LongStream.of(2L, 4L);
    LongStream b = mock(LongStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(LongStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    assertThat(stream.collect(ArrayList::new, List::add, List::addAll), contains(1L, 2L, 3L, 4L));
    verify(sortedShards[0], never()).collect(any(Supplier.class), any(ObjLongConsumer.class), any(BiConsumer.class));
    verify(sortedShards[1], never()).collect(any(Supplier.class), any(ObjLongConsumer.class), any(BiConsumer.class));
  }

  @Test
  public void testCollectCanBeSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    Supplier<List<Long>> supplier = ArrayList::new;
    ObjLongConsumer<List<Long>> accumulator = List::add;
    BiConsumer<List<Long>, List<Long>> combiner = List::addAll;
    assertThat(stream.collect(supplier, accumulator, combiner), containsInAnyOrder(1L, 2L, 3L, 4L));
    verify(a).collect(supplier, accumulator, combiner);
    verify(b).collect(supplier, accumulator, combiner);
  }

  @Test
  public void testSumIsSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1, 3)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2, 4)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    assertThat(stream.sum(), is(10L));

    verify(a).sum();
    verify(b).sum();

  }

  @Test
  public void testSummaryStatisticsIsSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1, 3)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2, 4)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongSummaryStatistics statistics = stream.summaryStatistics();
    assertThat(statistics.getCount(), is(4L));
    assertThat(statistics.getMin(), is(1L));
    assertThat(statistics.getMax(), is(4L));
    assertThat(statistics.getSum(), is(10L));
    assertThat(statistics.getAverage(), is(2.5));

    verify(a).summaryStatistics();
    verify(b).summaryStatistics();
  }

  @Test
  public void testMinIsSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    assertThat(stream.min(), is(OptionalLong.of(1L)));

    verify(a).min();
    verify(b).min();
  }

  @Test
  public void testMaxIsSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    assertThat(stream.max(), is(OptionalLong.of(4L)));

    verify(a).max();
    verify(b).max();
  }

  @Test
  public void testCountIsSharded() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    assertThat(stream.count(), is(4L));

    verify(a).count();
    verify(b).count();
  }

  @Test
  public void testAnyMatchShortCircuits() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongPredicate predicate = i -> true;
    assertTrue(stream.anyMatch(predicate));

    try {
      verify(a).anyMatch(predicate);
      verify(b, never()).anyMatch(any(LongPredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).anyMatch(any(LongPredicate.class));
        verify(b).anyMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testAnyMatchIsExhaustive() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongPredicate predicate = i -> i == 5L;
    assertFalse(stream.anyMatch(predicate));

    verify(a).anyMatch(predicate);
    verify(b).anyMatch(predicate);
  }

  @Test
  public void testAllMatchShortCircuits() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongPredicate predicate = i -> false;
    assertFalse(stream.allMatch(predicate));

    try {
      verify(a).allMatch(predicate);
      verify(b, never()).allMatch(any(LongPredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).allMatch(any(LongPredicate.class));
        verify(b).allMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testAllMatchIsExhaustive() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongPredicate predicate = i -> i >= 0L;
    assertTrue(stream.allMatch(predicate));

    verify(a).allMatch(predicate);
    verify(b).allMatch(predicate);
  }

  @Test
  public void testNoneMatchShortCircuits() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongPredicate predicate = i -> true;
    assertFalse(stream.noneMatch(predicate));

    try {
      verify(a).noneMatch(predicate);
      verify(b, never()).noneMatch(any(LongPredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).noneMatch(any(LongPredicate.class));
        verify(b).noneMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testNoneMatchIsExhaustive() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongPredicate predicate = i -> i == 5L;
    assertTrue(stream.noneMatch(predicate));

    verify(a).noneMatch(predicate);
    verify(b).noneMatch(predicate);
  }

  @Test
  public void testFindFirstIsOrderedCorrectly() {
    LongStream[] sortedShards = new LongStream[2];

    LongStream aOriginal = LongStream.of(3L, 1L);
    LongStream a = mock(LongStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(LongStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    LongStream bOriginal = LongStream.of(2L, 4L);
    LongStream b = mock(LongStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(LongStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();

    assertThat(stream.findFirst(), is(OptionalLong.of(1L)));
    verify(sortedShards[0]).findFirst();
    verify(sortedShards[1]).findFirst();
  }

  @Test
  public void testUnorderedFindFirstIsFindAny() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    assertThat(stream.findFirst().getAsLong(), anyOf(is(1L), is(2L)));

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
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    try {
      assertThat(stream.findAny().getAsLong(), anyOf(is(1L), is(3L)));
      verify(a).findAny();
      verify(b, never()).findAny();
    } catch (Throwable e) {
      try {
        assertThat(stream.findAny().getAsLong(), anyOf(is(2L), is(4L)));
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
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.empty()));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.empty()));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    assertThat(stream.findAny(), is(OptionalLong.empty()));

    verify(a).findAny();
    verify(b).findAny();
  }

  @Test
  public void testAsDoubleStreamWhenCustomOrderedIsSharded() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Comparator<Long> ordering = (x, y) -> Long.compare(y, x);
    ShardedLongStream stream = new ShardedLongStream(shards.map(s -> s.boxed().sorted(ordering).mapToLong(r -> r)), ordering, ORDERED, source);

    DoubleStream derived = stream.asDoubleStream();

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(4.0, 3.0, 2.0, 1.0));
  }

  @Test
  public void testAsDoubleStreamWhenNaturalOrderedIsSharded() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    DoubleStream derived = stream.sorted().asDoubleStream();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(((ShardedDoubleStream) derived).getOrdering(), is(naturalOrder()));
    assertThat(derived.boxed().collect(toList()), contains(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testAsDoubleStreamCanBeSharded() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    DoubleStream derived = stream.asDoubleStream();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testBoxedIsSharded() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    Stream<Long> derived = stream.boxed();

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder(1L, 2L, 3L, 4L));
  }

  @Test
  public void testOrderedIteratorIsOrdered() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    List<Long> assembled = new ArrayList<>();
    for (PrimitiveIterator.OfLong it = stream.iterator(); it.hasNext(); ) {
      assembled.add(it.next());
    }
    assertThat(assembled, contains(1L, 2L, 3L, 4L));
  }

  @Test
  public void testUnorderedIteratorIsLazy() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    long seen = stream.iterator().next();

    //Merged iterator implementation is backed by spliterators, hence the mismatching verifications.
    try {
      assertThat(seen, anyOf(is(1L), is(3L)));
      verify(a).spliterator();
      verify(b, never()).spliterator();
    } catch (Throwable e) {
      try {
        assertThat(seen, anyOf(is(2L), is(4L)));
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
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedLongStream.class));

    List<Long> assembled = new ArrayList<>();
    Spliterator.OfLong split = stream.spliterator();
    while (split.tryAdvance((LongConsumer) s -> assembled.add(s)));
    assertThat(assembled, contains(1L, 2L, 3L, 4L));
  }

  @Test
  public void testUnorderedSpliteratorIsLazy() {
    LongStream a = mock(LongStream.class, delegatesTo(LongStream.of(1L, 3L)));
    LongStream b = mock(LongStream.class, delegatesTo(LongStream.of(2L, 4L)));
    Stream<LongStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source);

    long[] seen = new long[1];
    stream.spliterator().tryAdvance((LongConsumer) s -> seen[0] = s);

    try {
      assertThat(seen[0], anyOf(is(1L), is(3L)));
      verify(a).spliterator();
      verify(b, never()).spliterator();
    } catch (Throwable e) {
      try {
        assertThat(seen[0], anyOf(is(2L), is(4L)));
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
    LongStream a = LongStream.of(1L, 2L);
    LongStream b = LongStream.of(3L, 4L);
    LongStream c = LongStream.of(5L, 6L);
    LongStream d = LongStream.of(7L, 8L);
    LongStream e = LongStream.of(9L, 10L);
    LongStream f = LongStream.of(11L, 12L);
    LongStream g = LongStream.of(13L, 14L);
    LongStream h = LongStream.of(15L, 16L);
    Stream<LongStream> shards = Stream.of(a, b, c, d, e, f, g, h);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    LongStream stream = new ShardedLongStream(shards, null, 0, source).parallel();

    Spliterator.OfLong hSplit = requireNonNull(stream.spliterator());

    Spliterator.OfLong dSplit = requireNonNull(hSplit.trySplit());

    Spliterator.OfLong bSplit = requireNonNull(dSplit.trySplit());
    Spliterator.OfLong fSplit = requireNonNull(hSplit.trySplit());

    Spliterator.OfLong aSplit = requireNonNull(bSplit.trySplit());
    Spliterator.OfLong cSplit = requireNonNull(dSplit.trySplit());
    Spliterator.OfLong eSplit = requireNonNull(fSplit.trySplit());
    Spliterator.OfLong gSplit = requireNonNull(hSplit.trySplit());


    List<Long> aList = new ArrayList<>();
    aSplit.forEachRemaining((LongConsumer) aList::add);
    assertThat(aList, containsInAnyOrder(1L, 2L));

    List<Long> bList = new ArrayList<>();
    bSplit.forEachRemaining((LongConsumer) bList::add);
    assertThat(bList, containsInAnyOrder(3L, 4L));

    List<Long> cList = new ArrayList<>();
    cSplit.forEachRemaining((LongConsumer) cList::add);
    assertThat(cList, containsInAnyOrder(5L, 6L));

    List<Long> dList = new ArrayList<>();
    dSplit.forEachRemaining((LongConsumer) dList::add);
    assertThat(dList, containsInAnyOrder(7L, 8L));

    List<Long> eList = new ArrayList<>();
    eSplit.forEachRemaining((LongConsumer) eList::add);
    assertThat(eList, containsInAnyOrder(9L, 10L));

    List<Long> fList = new ArrayList<>();
    fSplit.forEachRemaining((LongConsumer) fList::add);
    assertThat(fList, containsInAnyOrder(11L, 12L));

    List<Long> gList = new ArrayList<>();
    gSplit.forEachRemaining((LongConsumer) gList::add);
    assertThat(gList, containsInAnyOrder(13L, 14L));

    List<Long> hList = new ArrayList<>();
    hSplit.forEachRemaining((LongConsumer) hList::add);
    assertThat(hList, containsInAnyOrder(15L, 16L));
  }

  @Test
  public void testSequentialIsSharded() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.sequential();

    assertThat(derived, instanceOf(ShardedLongStream.class));

    assertFalse(derived.isParallel());
  }

  @Test
  public void testParallelIsSharded() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.parallel();

    assertThat(derived, instanceOf(ShardedLongStream.class));

    assertTrue(derived.isParallel());
  }

  @Test
  public void testUnorderedIsSharded() {
    Stream<LongStream> shards = Stream.of(LongStream.of(1L, 3L), LongStream.of(2L, 4L));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedLongStream stream = new ShardedLongStream(shards, null, 0, source);

    LongStream derived = stream.unordered();

    assertThat(derived, instanceOf(ShardedLongStream.class));
  }

}
