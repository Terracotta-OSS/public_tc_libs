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
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
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
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShardedDoubleStreamTest {

  @Test
  public void testFilterIsDoneInParallel() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.filter(c -> c >= 3.0);

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(3.0, 4.0));
  }

  @Test
  public void testMapWhenUnorderedIsDoneInParallel() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.map(c -> c * 10.0);

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(10.0, 20.0, 30.0, 40.0));
  }

  @Test
  public void testMapWhenOrderedIsMerged() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    DoubleStream derived = stream.map(c -> c * 10.0);

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(10.0, 20.0, 30.0, 40.0));
  }

  @Test
  public void testMapToObjWhenUnorderedIsDoneInParallel() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    Stream<String> derived = stream.mapToObj(Double::toString);

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder("1.0", "2.0", "3.0", "4.0"));
  }

  @Test
  public void testMapToObjWhenOrderedIsMerged() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    Stream<String> derived = stream.mapToObj(Double::toString);

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.collect(toList()), contains("1.0", "2.0", "3.0", "4.0"));
  }

  @Test
  public void testMapToIntWhenUnorderedIsDoneInParallel() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    IntStream derived = stream.mapToInt(c -> (int) (Integer.MAX_VALUE - c));

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(Integer.MAX_VALUE - 1, Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 3, Integer.MAX_VALUE - 4));
  }

  @Test
  public void testMapToIntWhenOrderedIsMerged() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    IntStream derived = stream.mapToInt(c -> (int) (Integer.MAX_VALUE - c));

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(Integer.MAX_VALUE - 1, Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 3, Integer.MAX_VALUE - 4));
  }

  @Test
  public void testMapToLongWhenUnorderedIsDoneInParallel() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    LongStream derived = stream.mapToLong(c -> Long.MAX_VALUE - (long) c);

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(Long.MAX_VALUE - 1L, Long.MAX_VALUE - 2L, Long.MAX_VALUE - 3L, Long.MAX_VALUE - 4L));
  }

  @Test
  public void testMapToLongWhenOrderedIsMerged() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    LongStream derived = stream.mapToLong(c -> Long.MAX_VALUE - (long) c);

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(Long.MAX_VALUE - 1L, Long.MAX_VALUE - 2L, Long.MAX_VALUE - 3L, Long.MAX_VALUE - 4L));
  }

  @Test
  public void testFlatMapWhenUnorderedIsDoneInParallel() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.flatMap(c -> DoubleStream.iterate(c, i -> i + c).limit(10));

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(
        1.0,  2.0,  3.0,  4.0,  5.0,  6.0,  7.0,  8.0,  9.0, 10.0,
        2.0,  4.0,  6.0,  8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0,
        3.0,  6.0,  9.0, 12.0, 15.0, 18.0, 21.0, 24.0, 27.0, 30.0,
        4.0,  8.0, 12.0, 16.0, 20.0, 24.0, 28.0, 32.0, 36.0, 40.0));
  }

  @Test
  public void testFlatMapWhenOrderedIsMerged() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    DoubleStream derived = stream.flatMap(c -> DoubleStream.iterate(c, i -> i + c).limit(10));

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(
        1.0,  2.0,  3.0,  4.0,  5.0,  6.0,  7.0,  8.0,  9.0, 10.0,
        2.0,  4.0,  6.0,  8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0,
        3.0,  6.0,  9.0, 12.0, 15.0, 18.0, 21.0, 24.0, 27.0, 30.0,
        4.0,  8.0, 12.0, 16.0, 20.0, 24.0, 28.0, 32.0, 36.0, 40.0));
  }

  @Test
  public void testDistinctIsMerged() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 1.0, 3.0), DoubleStream.of(3.0, 2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    DoubleStream derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testDistinctWhenOrderedPreservesOrdering() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 1.0, 3.0), DoubleStream.of(3.0, 2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    DoubleStream derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testNaturalSortingShardsAndMergesCorrectly() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 1.0, 3.0), DoubleStream.of(3.0, 2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.sorted();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), contains(1.0, 1.0, 2.0, 3.0, 3.0, 4.0));
  }

  @Test
  public void testPeekIsShardedAndVisited() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    List<Double> doubles = new ArrayList<>();
    DoubleStream derived = stream.peek(doubles::add);

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(doubles, containsInAnyOrder(derived.boxed().toArray(Double[]::new)));
  }

  @Test
  public void testLimitPullsMinimallyFromShardsAndMerges() {
    List<Double> consumed = new ArrayList<>();
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0).peek(consumed::add), DoubleStream.of(2.0, 4.0).peek(consumed::add));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.limit(1);

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), either(contains(1.0)).or(contains(2.0)));
    assertThat(consumed, either(contains(1.0)).or(contains(2.0)));
  }

  @Test
  public void testSkipMergesImmediately() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.skip(1);

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), either(containsInAnyOrder(3.0, 2.0, 4.0)).or(containsInAnyOrder(1.0, 3.0, 4.0)));
  }

  @Test
  public void testForEachIsShardedEvenWhenOrdered() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 4.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(3.0, 2.0)));
    doAnswer(RETURNS_SELF).when(a).sorted();
    doAnswer(RETURNS_SELF).when(b).sorted();
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    List<Double> output = new ArrayList<>();
    DoubleConsumer add = output::add;
    stream.forEach(add);

    verify(a).forEach(add);
    verify(b).forEach(add);
    assertThat(output, containsInAnyOrder(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testForEachOrderedIsOrdered() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    List<Double> output = new ArrayList<>();
    stream.forEachOrdered(output::add);

    assertThat(output, contains(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testToArrayIsOrdered() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    assertThat(stream(stream.toArray()).boxed().toArray(), arrayContaining(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testToArrayIsSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    assertThat(stream(stream.toArray()).boxed().toArray(), arrayContainingInAnyOrder(1.0, 2.0, 3.0, 4.0));
    verify(a).toArray();
    verify(b).toArray();
  }

  @Test
  public void testIdentityReduceIsOrderedCorrectly() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 4.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(3.0, 2.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    assertThat(stream.reduce(0.0, (x, y) -> {
      assertThat(x, lessThan(y));
      return y;
    }), is(4.0));
    verify(a, never()).reduce(anyDouble(), any(DoubleBinaryOperator.class));
    verify(b, never()).reduce(anyDouble(), any(DoubleBinaryOperator.class));
  }

  @Test
  public void testIdentityReduceCanBeSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleBinaryOperator accumulator = (x, y) -> x + y;
    assertThat(stream.reduce(0.0, accumulator), is(10.0));
    verify(a).reduce(0.0, accumulator);
    verify(b).reduce(0.0, accumulator);
  }


  @Test
  public void testReduceIsOrderedCorrectly() {
    DoubleStream[] sortedShards = new DoubleStream[2];

    DoubleStream aOriginal = DoubleStream.of(1.0, 3.0);
    DoubleStream a = mock(DoubleStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(DoubleStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    DoubleStream bOriginal = DoubleStream.of(2.0, 4.0);
    DoubleStream b = mock(DoubleStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(DoubleStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    assertThat(stream.reduce((x, y) -> {
      assertThat(x, lessThan(y));
      return y;
    }), is(OptionalDouble.of(4.0)));
    verify(sortedShards[0], never()).reduce(any(DoubleBinaryOperator.class));
    verify(sortedShards[1], never()).reduce(any(DoubleBinaryOperator.class));
  }

  @Test
  public void testReduceCanBeSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleBinaryOperator accumulator = (x, y) -> x + y;
    assertThat(stream.reduce(accumulator), is(OptionalDouble.of(10.0)));
    verify(a).reduce(accumulator);
    verify(b).reduce(accumulator);
  }

  @Test
  public void testCollectIsOrderedCorrectly() {
    DoubleStream[] sortedShards = new DoubleStream[2];

    DoubleStream aOriginal = DoubleStream.of(1.0, 3.0);
    DoubleStream a = mock(DoubleStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(DoubleStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    DoubleStream bOriginal = DoubleStream.of(2.0, 4.0);
    DoubleStream b = mock(DoubleStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(DoubleStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    assertThat(stream.collect(ArrayList::new, List::add, List::addAll), contains(1.0, 2.0, 3.0, 4.0));
    verify(sortedShards[0], never()).collect(any(Supplier.class), any(ObjDoubleConsumer.class), any(BiConsumer.class));
    verify(sortedShards[1], never()).collect(any(Supplier.class), any(ObjDoubleConsumer.class), any(BiConsumer.class));
  }

  @Test
  public void testCollectCanBeSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    Supplier<List<Double>> supplier = ArrayList::new;
    ObjDoubleConsumer<List<Double>> accumulator = List::add;
    BiConsumer<List<Double>, List<Double>> combiner = List::addAll;
    assertThat(stream.collect(supplier, accumulator, combiner), containsInAnyOrder(1.0, 2.0, 3.0, 4.0));
    verify(a).collect(supplier, accumulator, combiner);
    verify(b).collect(supplier, accumulator, combiner);
  }

  @Test
  public void testSumIsSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    assertThat(stream.sum(), is(10.0));

    verify(a).sum();
    verify(b).sum();

  }

  @Test
  public void testSummaryStatisticsIsSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleSummaryStatistics statistics = stream.summaryStatistics();
    assertThat(statistics.getCount(), is(4L));
    assertThat(statistics.getMin(), is(1.0));
    assertThat(statistics.getMax(), is(4.0));
    assertThat(statistics.getSum(), is(10.0));
    assertThat(statistics.getAverage(), is(2.5));

    verify(a).summaryStatistics();
    verify(b).summaryStatistics();
  }

  @Test
  public void testMinIsSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    assertThat(stream.min(), is(OptionalDouble.of(1.0)));

    verify(a).min();
    verify(b).min();
  }

  @Test
  public void testMaxIsSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    assertThat(stream.max(), is(OptionalDouble.of(4.0)));

    verify(a).max();
    verify(b).max();
  }

  @Test
  public void testCountIsSharded() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    assertThat(stream.count(), is(4L));

    verify(a).count();
    verify(b).count();
  }

  @Test
  public void testAnyMatchShortCircuits() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoublePredicate predicate = i -> true;
    assertTrue(stream.anyMatch(predicate));

    try {
      verify(a).anyMatch(predicate);
      verify(b, never()).anyMatch(any(DoublePredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).anyMatch(any(DoublePredicate.class));
        verify(b).anyMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testAnyMatchIsExhaustive() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoublePredicate predicate = i -> i == 5.0;
    assertFalse(stream.anyMatch(predicate));

    verify(a).anyMatch(predicate);
    verify(b).anyMatch(predicate);
  }

  @Test
  public void testAllMatchShortCircuits() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoublePredicate predicate = i -> false;
    assertFalse(stream.allMatch(predicate));

    try {
      verify(a).allMatch(predicate);
      verify(b, never()).allMatch(any(DoublePredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).allMatch(any(DoublePredicate.class));
        verify(b).allMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testAllMatchIsExhaustive() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoublePredicate predicate = i -> i >= 0.0;
    assertTrue(stream.allMatch(predicate));

    verify(a).allMatch(predicate);
    verify(b).allMatch(predicate);
  }

  @Test
  public void testNoneMatchShortCircuits() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoublePredicate predicate = i -> true;
    assertFalse(stream.noneMatch(predicate));

    try {
      verify(a).noneMatch(predicate);
      verify(b, never()).noneMatch(any(DoublePredicate.class));
    } catch (Throwable e) {
      try {
        verify(a, never()).noneMatch(any(DoublePredicate.class));
        verify(b).noneMatch(predicate);
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testNoneMatchIsExhaustive() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoublePredicate predicate = i -> i == 5.0;
    assertTrue(stream.noneMatch(predicate));

    verify(a).noneMatch(predicate);
    verify(b).noneMatch(predicate);
  }

  @Test
  public void testFindFirstIsOrderedCorrectly() {
    DoubleStream[] sortedShards = new DoubleStream[2];

    DoubleStream aOriginal = DoubleStream.of(3.0, 1.0);
    DoubleStream a = mock(DoubleStream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(DoubleStream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    DoubleStream bOriginal = DoubleStream.of(2.0, 4.0);
    DoubleStream b = mock(DoubleStream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(DoubleStream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();

    assertThat(stream.findFirst(), is(OptionalDouble.of(1.0)));
    verify(sortedShards[0]).findFirst();
    verify(sortedShards[1]).findFirst();
  }

  @Test
  public void testUnorderedFindFirstIsFindAny() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    assertThat(stream.findFirst().getAsDouble(), anyOf(is(1.0), is(2.0)));

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
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    try {
      assertThat(stream.findAny().getAsDouble(), anyOf(is(1.0), is(3.0)));
      verify(a).findAny();
      verify(b, never()).findAny();
    } catch (Throwable e) {
      try {
        assertThat(stream.findAny().getAsDouble(), anyOf(is(2.0), is(4.0)));
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
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.empty()));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.empty()));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    assertThat(stream.findAny(), is(OptionalDouble.empty()));

    verify(a).findAny();
    verify(b).findAny();
  }

  @Test
  public void testOrderedIteratorIsOrdered() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    List<Double> assembled = new ArrayList<>();
    for (PrimitiveIterator.OfDouble it = stream.iterator(); it.hasNext(); ) {
      assembled.add(it.next());
    }
    assertThat(assembled, contains(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testUnorderedIteratorIsLazy() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    double seen = stream.iterator().next();

    //Merged iterator implementation is backed by spliterators, hence the mismatching verifications.
    try {
      assertThat(seen, anyOf(is(1.0), is(3.0)));
      verify(a).spliterator();
      verify(b, never()).spliterator();
    } catch (Throwable e) {
      try {
        assertThat(seen, anyOf(is(2.0), is(4.0)));
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
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedDoubleStream.class));

    List<Double> assembled = new ArrayList<>();
    Spliterator.OfDouble split = stream.spliterator();
    while (split.tryAdvance((DoubleConsumer) s -> assembled.add(s)));
    assertThat(assembled, contains(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testUnorderedSpliteratorIsLazy() {
    DoubleStream a = mock(DoubleStream.class, delegatesTo(DoubleStream.of(1.0, 3.0)));
    DoubleStream b = mock(DoubleStream.class, delegatesTo(DoubleStream.of(2.0, 4.0)));
    Stream<DoubleStream> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    double[] seen = new double[1];
    stream.spliterator().tryAdvance((DoubleConsumer) s -> seen[0] = s);

    try {
      assertThat(seen[0], anyOf(is(1.0), is(3.0)));
      verify(a).spliterator();
      verify(b, never()).spliterator();
    } catch (Throwable e) {
      try {
        assertThat(seen[0], anyOf(is(2.0), is(4.0)));
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
    DoubleStream a = DoubleStream.of(1.0, 2.0);
    DoubleStream b = DoubleStream.of(3.0, 4.0);
    DoubleStream c = DoubleStream.of(5.0, 6.0);
    DoubleStream d = DoubleStream.of(7.0, 8.0);
    DoubleStream e = DoubleStream.of(9.0, 10.0);
    DoubleStream f = DoubleStream.of(11.0, 12.0);
    DoubleStream g = DoubleStream.of(13.0, 14.0);
    DoubleStream h = DoubleStream.of(15.0, 16.0);
    Stream<DoubleStream> shards = Stream.of(a, b, c, d, e, f, g, h);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    DoubleStream stream = new ShardedDoubleStream(shards, null, 0, source).parallel();

    Spliterator.OfDouble hSplit = requireNonNull(stream.spliterator());

    Spliterator.OfDouble dSplit = requireNonNull(hSplit.trySplit());

    Spliterator.OfDouble bSplit = requireNonNull(dSplit.trySplit());
    Spliterator.OfDouble fSplit = requireNonNull(hSplit.trySplit());

    Spliterator.OfDouble aSplit = requireNonNull(bSplit.trySplit());
    Spliterator.OfDouble cSplit = requireNonNull(dSplit.trySplit());
    Spliterator.OfDouble eSplit = requireNonNull(fSplit.trySplit());
    Spliterator.OfDouble gSplit = requireNonNull(hSplit.trySplit());


    List<Double> aList = new ArrayList<>();
    aSplit.forEachRemaining((DoubleConsumer) aList::add);
    assertThat(aList, containsInAnyOrder(1.0, 2.0));

    List<Double> bList = new ArrayList<>();
    bSplit.forEachRemaining((DoubleConsumer) bList::add);
    assertThat(bList, containsInAnyOrder(3.0, 4.0));

    List<Double> cList = new ArrayList<>();
    cSplit.forEachRemaining((DoubleConsumer) cList::add);
    assertThat(cList, containsInAnyOrder(5.0, 6.0));

    List<Double> dList = new ArrayList<>();
    dSplit.forEachRemaining((DoubleConsumer) dList::add);
    assertThat(dList, containsInAnyOrder(7.0, 8.0));

    List<Double> eList = new ArrayList<>();
    eSplit.forEachRemaining((DoubleConsumer) eList::add);
    assertThat(eList, containsInAnyOrder(9.0, 10.0));

    List<Double> fList = new ArrayList<>();
    fSplit.forEachRemaining((DoubleConsumer) fList::add);
    assertThat(fList, containsInAnyOrder(11.0, 12.0));

    List<Double> gList = new ArrayList<>();
    gSplit.forEachRemaining((DoubleConsumer) gList::add);
    assertThat(gList, containsInAnyOrder(13.0, 14.0));

    List<Double> hList = new ArrayList<>();
    hSplit.forEachRemaining((DoubleConsumer) hList::add);
    assertThat(hList, containsInAnyOrder(15.0, 16.0));
  }

  @Test
  public void testSequentialIsSharded() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.sequential();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));

    assertFalse(derived.isParallel());
  }

  @Test
  public void testParallelIsSharded() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.parallel();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));

    assertTrue(derived.isParallel());
  }

  @Test
  public void testUnorderedIsSharded() {
    Stream<DoubleStream> shards = Stream.of(DoubleStream.of(1.0, 3.0), DoubleStream.of(2.0, 4.0));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedDoubleStream stream = new ShardedDoubleStream(shards, null, 0, source);

    DoubleStream derived = stream.unordered();

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
  }

}
