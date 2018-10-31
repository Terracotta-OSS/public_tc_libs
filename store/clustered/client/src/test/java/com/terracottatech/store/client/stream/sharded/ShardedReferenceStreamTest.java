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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShardedReferenceStreamTest {

  @Test
  public void testFilterIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    @SuppressWarnings("rawtypes")
    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.filter(c -> c.compareTo("c") >= 0);

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder("c", "d"));
  }

  @Test
  public void testMapWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.map(c -> c.concat("foo"));

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder("afoo", "bfoo", "cfoo", "dfoo"));
  }

  @Test
  public void testMapWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    Stream<String> derived = stream.map(c -> c.concat("foo"));

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.collect(toList()), contains("afoo", "bfoo", "cfoo", "dfoo"));
  }

  @Test
  public void testMapToIntWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "ccc"), Stream.of("bb", "dddd"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    IntStream derived = stream.mapToInt(c -> c.length());

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1, 2, 3, 4));
  }

  @Test
  public void testMapToIntWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    IntStream derived = stream.mapToInt(c -> c.length());

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(4, 3, 2, 1));
  }

  @Test
  public void testMapToLongWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    LongStream derived = stream.mapToLong(c -> c.length());

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1L, 2L, 3L, 4L));
  }

  @Test
  public void testMapToLongWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    LongStream derived = stream.mapToLong(c -> c.length());

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(4L, 3L, 2L, 1L));
  }

  @Test
  public void testMapToDoubleWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    DoubleStream derived = stream.mapToDouble(c -> c.length());

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(1.0, 2.0, 3.0, 4.0));
  }

  @Test
  public void testMapToDoubleWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    DoubleStream derived = stream.mapToDouble(c -> c.length());

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(4.0, 3.0, 2.0, 1.0));
  }

  @Test
  public void testFlatMapWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.flatMap(c -> Stream.of(c.split("")));

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), containsInAnyOrder("a", "a", "a", "a", "b", "b", "b", "c", "c", "d"));
  }

  @Test
  public void testFlatMapWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    Stream<String> derived = stream.flatMap(c -> Stream.of(c.split("")));

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.collect(toList()), contains("a", "a", "a", "a", "b", "b", "b", "c", "c", "d"));
  }

  @Test
  public void testFlatMapToIntWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    IntStream derived = stream.flatMapToInt(c -> Stream.of(c.split("")).mapToInt(Object::hashCode));

    assertThat(derived, instanceOf(ShardedIntStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(
        "a".hashCode(), "a".hashCode(), "a".hashCode(), "a".hashCode(),
        "b".hashCode(), "b".hashCode(), "b".hashCode(),
        "c".hashCode(), "c".hashCode(),
        "d".hashCode()));
  }

  @Test
  public void testFlatMapToIntWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    IntStream derived = stream.flatMapToInt(c -> Stream.of(c.split("")).mapToInt(Object::hashCode));

    assertThat(derived, not(instanceOf(ShardedIntStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(
        "a".hashCode(), "a".hashCode(), "a".hashCode(), "a".hashCode(),
        "b".hashCode(), "b".hashCode(), "b".hashCode(),
        "c".hashCode(), "c".hashCode(),
        "d".hashCode()));
  }

  @Test
  public void testFlatMapToLongWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    LongStream derived = stream.flatMapToLong(c -> Stream.of(c.split("")).mapToLong(Object::hashCode));

    assertThat(derived, instanceOf(ShardedLongStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(
        (long) "a".hashCode(), (long) "a".hashCode(), (long) "a".hashCode(), (long) "a".hashCode(),
        (long) "b".hashCode(), (long) "b".hashCode(), (long) "b".hashCode(),
        (long) "c".hashCode(), (long) "c".hashCode(),
        (long) "d".hashCode()));
  }

  @Test
  public void testFlatMapToLongWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    LongStream derived = stream.flatMapToLong(c -> Stream.of(c.split("")).mapToLong(Object::hashCode));

    assertThat(derived, not(instanceOf(ShardedLongStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(
        (long) "a".hashCode(), (long) "a".hashCode(), (long) "a".hashCode(), (long) "a".hashCode(),
        (long) "b".hashCode(), (long) "b".hashCode(), (long) "b".hashCode(),
        (long) "c".hashCode(), (long) "c".hashCode(),
        (long) "d".hashCode()));
  }

  @Test
  public void testFlatMapToDoubleWhenUnorderedIsDoneInParallel() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    DoubleStream derived = stream.flatMapToDouble(c -> Stream.of(c.split("")).mapToDouble(Object::hashCode));

    assertThat(derived, instanceOf(ShardedDoubleStream.class));
    assertThat(derived.boxed().collect(toList()), containsInAnyOrder(
        (double) "a".hashCode(), (double) "a".hashCode(), (double) "a".hashCode(), (double) "a".hashCode(),
        (double) "b".hashCode(), (double) "b".hashCode(), (double) "b".hashCode(),
        (double) "c".hashCode(), (double) "c".hashCode(),
        (double) "d".hashCode()));
  }

  @Test
  public void testFlatMapToDoubleWhenOrderedIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aaaa", "cc"), Stream.of("bbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    DoubleStream derived = stream.flatMapToDouble(c -> Stream.of(c.split("")).mapToDouble(Object::hashCode));

    assertThat(derived, not(instanceOf(ShardedDoubleStream.class)));
    assertThat(derived.boxed().collect(toList()), contains(
        (double) "a".hashCode(), (double) "a".hashCode(), (double) "a".hashCode(), (double) "a".hashCode(),
        (double) "b".hashCode(), (double) "b".hashCode(), (double) "b".hashCode(),
        (double) "c".hashCode(), (double) "c".hashCode(),
        (double) "d".hashCode()));
  }

  @Test
  public void testDistinctIsMerged() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "a", "c"), Stream.of("c", "b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    Stream<String> derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.collect(toList()), containsInAnyOrder("a", "b", "c", "d"));
  }

  @Test
  public void testDistinctWhenOrderedPreservesOrdering() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "a", "c"), Stream.of("c", "b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    Stream<String> derived = stream.distinct();

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.collect(toList()), contains("a", "b", "c", "d"));
  }

  @Test
  public void testNaturalSortingShardsAndMergesCorrectly() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "a", "c"), Stream.of("c", "b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.sorted();

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), contains("a", "a", "b", "c", "c", "d"));
  }

  @Test
  public void testCustomSortingShardsAndMergesCorrectly() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("aa", "aaa"), Stream.of("bbbb", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.sorted(Comparator.comparingInt(String::length));

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.collect(toList()), contains("d", "aa", "aaa", "bbbb"));
  }

  @Test
  public void testPeekIsShardedAndVisited() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    List<String> strings = new ArrayList<>();
    Stream<String> derived = stream.peek(strings::add);

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(strings, containsInAnyOrder(derived.toArray(String[]::new)));
  }

  @Test
  public void testLimitPullsMinimallyFromShardsAndMerges() {
    List<String> consumed = new ArrayList<>();
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c").peek(consumed::add), Stream.of("b", "d").peek(consumed::add));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.limit(1);

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.collect(toList()), either(contains("a")).or(contains("b")));
    assertThat(consumed, either(contains("a")).or(contains("b")));
  }

  @Test
  public void testSkipMergesImmediately() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.skip(1);

    assertThat(derived, not(instanceOf(ShardedReferenceStream.class)));
    assertThat(derived.collect(toList()), either(containsInAnyOrder("c", "b", "d")).or(containsInAnyOrder("a", "c", "d")));
  }

  @Test
  public void testForEachIsShardedEvenWhenOrdered() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    doAnswer(RETURNS_SELF).when(a).sorted();
    doAnswer(RETURNS_SELF).when(b).sorted();
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    List<String> output = new ArrayList<>();
    Consumer<String> add = output::add;
    stream.forEach(add);

    verify(a).forEach(add);
    verify(b).forEach(add);
    assertThat(output, containsInAnyOrder("a", "b", "c", "d"));

  }

  @Test
  public void testForEachOrderedIsOrdered() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    List<String> output = new ArrayList<>();
    stream.forEachOrdered(output::add);

    assertThat(output, contains("a", "b", "c", "d"));
  }

  @Test
  public void testToArrayIsOrdered() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    assertThat(stream.toArray(), arrayContaining("a", "b", "c", "d"));
  }

  @Test
  public void testTypedToArrayIsOrdered() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    assertThat(stream.toArray(String[]::new), arrayContaining("a", "b", "c", "d"));
  }

  @Test
  public void testToArrayIsSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.toArray(), arrayContainingInAnyOrder("a", "b", "c", "d"));
    verify(a).toArray();
    verify(b).toArray();
  }

  @Test
  public void testTypedToArrayIsSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    IntFunction<String[]> generator = String[]::new;
    assertThat(stream.toArray(generator), arrayContainingInAnyOrder("a", "b", "c", "d"));
    verify(a).toArray(generator);
    verify(b).toArray(generator);
  }

  @Test
  public void testIdentityReduceIsOrderedCorrectly() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    assertThat(stream.reduce("", String::concat), is("abcd"));
    verify(a, never()).reduce(anyString(), any(BinaryOperator.class));
    verify(b, never()).reduce(anyString(), any(BinaryOperator.class));
  }

  @Test
  public void testIdentityReduceCanBeSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    BinaryOperator<String> accumulator = String::concat;
    assertThat(stream.reduce("", accumulator).split(""), arrayContainingInAnyOrder("a", "b", "c", "d"));
    verify(a).reduce("", accumulator);
    verify(b).reduce("", accumulator);
  }


  @Test
  public void testReduceIsOrderedCorrectly() {
    Stream[] sortedShards = new Stream[2];

    Stream<String> aOriginal = Stream.of("a", "c");
    Stream<String> a = mock(Stream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(Stream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    Stream<String> bOriginal = Stream.of("b", "d");
    Stream<String> b = mock(Stream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(Stream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    assertThat(stream.reduce(String::concat), is(Optional.of("abcd")));
    verify(sortedShards[0], never()).reduce(any(BinaryOperator.class));
    verify(sortedShards[1], never()).reduce(any(BinaryOperator.class));
  }

  @Test
  public void testReduceCanBeSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    BinaryOperator<String> accumulator = String::concat;
    assertThat(stream.reduce(accumulator).get().split(""), arrayContainingInAnyOrder("a", "b", "c", "d"));
    verify(a).reduce(accumulator);
    verify(b).reduce(accumulator);
  }

  @Test
  public void testCombiningReduceIsOrderedCorrectly() {
    Stream[] sortedShards = new Stream[2];

    Stream<String> aOriginal = Stream.of("a", "c");
    Stream<String> a = mock(Stream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(Stream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    Stream<String> bOriginal = Stream.of("b", "d");
    Stream<String> b = mock(Stream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(Stream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    assertThat(stream.reduce(0, (c, s) -> c + s.length(), (x, y) -> x + y), is(4));
    verify(sortedShards[0], never()).reduce(any(), any(BiFunction.class), any(BinaryOperator.class));
    verify(sortedShards[1], never()).reduce(any(), any(BiFunction.class), any(BinaryOperator.class));
  }

  @Test
  public void testCombiningReduceCanBeSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    BiFunction<Integer, String, Integer> accumulator = (c, s) -> c + s.length();
    BinaryOperator<Integer> combiner = (x, y) -> x + y;
    assertThat(stream.reduce(0, accumulator, combiner), is(4));
    verify(a).reduce(0, accumulator, combiner);
    verify(b).reduce(0, accumulator, combiner);
  }

  @Test
  public void testCollectIsOrderedCorrectly() {
    Stream[] sortedShards = new Stream[2];

    Stream<String> aOriginal = Stream.of("a", "c");
    Stream<String> a = mock(Stream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(Stream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    Stream<String> bOriginal = Stream.of("b", "d");
    Stream<String> b = mock(Stream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(Stream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    assertThat(stream.collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString(), is("abcd"));
    verify(sortedShards[0], never()).collect(any(Supplier.class), any(BiConsumer.class), any(BiConsumer.class));
    verify(sortedShards[1], never()).collect(any(Supplier.class), any(BiConsumer.class), any(BiConsumer.class));
  }

  @Test
  public void testCollectCanBeSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Supplier<StringBuilder> supplier = StringBuilder::new;
    BiConsumer<StringBuilder, String> accumulator = StringBuilder::append;
    BiConsumer<StringBuilder, StringBuilder> combiner = StringBuilder::append;
    assertThat(stream.collect(supplier, accumulator, combiner).toString().split(""), arrayContainingInAnyOrder("a", "b", "c", "d"));
    verify(a).collect(supplier, accumulator, combiner);
    verify(b).collect(supplier, accumulator, combiner);
  }


  @Test
  public void testWrappedCollectIsOrderedCorrectly() {
    Stream[] sortedShards = new Stream[2];

    Stream<String> aOriginal = Stream.of("a", "c");
    Stream<String> a = mock(Stream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(Stream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    Stream<String> bOriginal = Stream.of("b", "d");
    Stream<String> b = mock(Stream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(Stream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    Collector<String, StringBuilder, String> collector = Collector.of(
        StringBuilder::new, StringBuilder::append, StringBuilder::append, StringBuilder::toString);

    assertThat(stream.collect(collector), is("abcd"));
    verify(sortedShards[0], never()).collect(any(Supplier.class), any(BiConsumer.class), any(BiConsumer.class));
    verify(sortedShards[1], never()).collect(any(Supplier.class), any(BiConsumer.class), any(BiConsumer.class));
    verify(sortedShards[0], never()).collect(any(Collector.class));
    verify(sortedShards[1], never()).collect(any(Collector.class));
  }

  @Test
  public void testUnorderedWrappedCollectIsSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Collector<String, StringBuilder, String> collector = Collector.of(
        StringBuilder::new, StringBuilder::append, StringBuilder::append, StringBuilder::toString);

    assertThat(stream.collect(collector).split(""), arrayContainingInAnyOrder("a", "b", "c", "d"));

    verify(a).collect(any(Collector.class));
    verify(b).collect(any(Collector.class));
  }

  @Test
  public void testWrappedCollectCanBeSharded() {
    Stream[] sortedShards = new Stream[2];

    Stream<String> aOriginal = Stream.of("a", "c");
    Stream<String> a = mock(Stream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(Stream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    Stream<String> bOriginal = Stream.of("b", "d");
    Stream<String> b = mock(Stream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(Stream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    Collector<String, int[], Integer> collector = Collector.of(
        () -> new int[1], (c, s) -> c[0] += s.length(), (x, y) -> new int[] {x[0] + y[0]}, x -> x[0], Characteristics.UNORDERED);

    assertThat(stream.collect(collector), is(4));
    verify(sortedShards[0]).collect(any(Collector.class));
    verify(sortedShards[1]).collect(any(Collector.class));
  }

  @Test
  public void testMinIsSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.min(Comparator.naturalOrder()), is(Optional.of("a")));

    verify(a).min(Comparator.naturalOrder());
    verify(b).min(Comparator.naturalOrder());
  }

  @Test
  public void testMaxIsSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.max(Comparator.naturalOrder()), is(Optional.of("d")));

    verify(a).max(Comparator.naturalOrder());
    verify(b).max(Comparator.naturalOrder());
  }

  @Test
  public void testCountIsSharded() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.count(), is(4L));

    verify(a).count();
    verify(b).count();
  }

  @Test
  public void testAnyMatchShortCircuits() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Predicate<String> predicate = "a"::equals;
    assertTrue(stream.anyMatch(predicate));

    verify(a).anyMatch(predicate);
    verify(b, never()).anyMatch(any(Predicate.class));
  }

  @Test
  public void testAnyMatchIsExhaustive() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Predicate<String> predicate = "e"::equals;
    assertFalse(stream.anyMatch(predicate));

    verify(a).anyMatch(predicate);
    verify(b).anyMatch(predicate);
  }

  @Test
  public void testAllMatchShortCircuits() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Predicate<String> predicate = s -> s.compareTo("a") <= 0;
    assertFalse(stream.allMatch(predicate));

    verify(a).allMatch(predicate);
    verify(b, never()).allMatch(any(Predicate.class));
  }

  @Test
  public void testAllMatchIsExhaustive() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Predicate<String> predicate = s -> s.compareTo("a") >= 0;
    assertTrue(stream.allMatch(predicate));

    verify(a).allMatch(predicate);
    verify(b).allMatch(predicate);
  }


  @Test
  public void testNoneMatchShortCircuits() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Predicate<String> predicate = "a"::equals;
    assertFalse(stream.noneMatch(predicate));

    verify(a).noneMatch(predicate);
    verify(b, never()).noneMatch(any(Predicate.class));
  }

  @Test
  public void testNoneMatchIsExhaustive() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Predicate<String> predicate = "e"::equals;
    assertTrue(stream.noneMatch(predicate));

    verify(a).noneMatch(predicate);
    verify(b).noneMatch(predicate);
  }

  @Test
  public void testFindFirstIsOrderedCorrectly() {
    Stream[] sortedShards = new Stream[2];

    Stream<String> aOriginal = Stream.of("c", "a");
    Stream<String> a = mock(Stream.class, delegatesTo(aOriginal));
    doAnswer(invocation -> sortedShards[0] = mock(Stream.class, delegatesTo(aOriginal.sorted()))).when(a).sorted();

    Stream<String> bOriginal = Stream.of("b", "d");
    Stream<String> b = mock(Stream.class, delegatesTo(bOriginal));
    doAnswer(invocation -> sortedShards[1] = mock(Stream.class, delegatesTo(bOriginal.sorted()))).when(b).sorted();

    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();

    assertThat(stream.findFirst(), is(Optional.of("a")));
    verify(sortedShards[0]).findFirst();
    verify(sortedShards[1]).findFirst();
  }

  @Test
  public void testUnorderedFindFirstIsFindAny() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.findFirst().get(), anyOf(is("a"), is("b")));

    verify(a).findAny();
    verify(a, never()).findFirst();
    verify(b, never()).findAny();
    verify(b, never()).findFirst();
  }

  @Test
  public void testFindAnyShortCircuits() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.findAny().get(), anyOf(is("a"), is("b")));

    verify(a).findAny();
    verify(b, never()).findAny();
  }

  @Test
  public void testFindAnyIsExhaustive() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.empty()));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.empty()));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.findAny(), is(Optional.empty()));

    verify(a).findAny();
    verify(b).findAny();
  }

  @Test
  public void testOrderedIteratorIsOrdered() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    List<String> assembled = new ArrayList<>();
    for (Iterator<String> it = stream.iterator(); it.hasNext(); ) {
      assembled.add(it.next());
    }
    assertThat(assembled, contains("a", "b", "c", "d"));
  }

  @Test
  public void testUnorderedIteratorIsLazy() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    assertThat(stream.iterator().next(), anyOf(is("a"), is("b")));

    verify(a).spliterator();
    verify(b, never()).spliterator();
  }

  @Test
  public void testOrderedSpliteratorIsOrdered() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).sorted();
    assertThat(stream, instanceOf(ShardedReferenceStream.class));

    List<String> assembled = new ArrayList<>();
    Spliterator<String> split = stream.spliterator();
    while (split.tryAdvance(s -> assembled.add(s)));
    assertThat(assembled, contains("a", "b", "c", "d"));
  }

  @Test
  public void testUnorderedSpliteratorIsLazy() {
    Stream<String> a = mock(Stream.class, delegatesTo(Stream.of("a", "c")));
    Stream<String> b = mock(Stream.class, delegatesTo(Stream.of("b", "d")));
    Stream<Stream<String>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    String[] first = new String[1];
    stream.spliterator().tryAdvance(s -> first[0] = s);

    assertThat(first[0], anyOf(is("a"), is("b")));

    verify(a).spliterator();
    verify(b, never()).spliterator();
  }

  @Test
  public void testUnorderedSpliteratorIsSplitInToShardPiecesWhenParallel() {
    Stream<String> a = Stream.of("1", "2");
    Stream<String> b = Stream.of("3", "4");
    Stream<String> c = Stream.of("5", "6");
    Stream<String> d = Stream.of("7", "8");
    Stream<String> e = Stream.of("9", "10");
    Stream<String> f = Stream.of("11", "12");
    Stream<String> g = Stream.of("13", "14");
    Stream<String> h = Stream.of("15", "16");
    Stream<Stream<String>> shards = Stream.of(a, b, c, d, e, f, g, h);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    Stream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source).parallel();

    Spliterator<String> hSplit = requireNonNull(stream.spliterator());

    Spliterator<String> dSplit = requireNonNull(hSplit.trySplit());

    Spliterator<String> bSplit = requireNonNull(dSplit.trySplit());
    Spliterator<String> fSplit = requireNonNull(hSplit.trySplit());

    Spliterator<String> aSplit = requireNonNull(bSplit.trySplit());
    Spliterator<String> cSplit = requireNonNull(dSplit.trySplit());
    Spliterator<String> eSplit = requireNonNull(fSplit.trySplit());
    Spliterator<String> gSplit = requireNonNull(hSplit.trySplit());


    List<String> aList = new ArrayList<>();
    aSplit.forEachRemaining(aList::add);
    assertThat(aList, containsInAnyOrder("1", "2"));

    List<String> bList = new ArrayList<>();
    bSplit.forEachRemaining(bList::add);
    assertThat(bList, containsInAnyOrder("3", "4"));

    List<String> cList = new ArrayList<>();
    cSplit.forEachRemaining(cList::add);
    assertThat(cList, containsInAnyOrder("5", "6"));

    List<String> dList = new ArrayList<>();
    dSplit.forEachRemaining(dList::add);
    assertThat(dList, containsInAnyOrder("7", "8"));

    List<String> eList = new ArrayList<>();
    eSplit.forEachRemaining(eList::add);
    assertThat(eList, containsInAnyOrder("9", "10"));

    List<String> fList = new ArrayList<>();
    fSplit.forEachRemaining(fList::add);
    assertThat(fList, containsInAnyOrder("11", "12"));

    List<String> gList = new ArrayList<>();
    gSplit.forEachRemaining(gList::add);
    assertThat(gList, containsInAnyOrder("13", "14"));

    List<String> hList = new ArrayList<>();
    hSplit.forEachRemaining(hList::add);
    assertThat(hList, containsInAnyOrder("15", "16"));

  }

  @Test
  public void testSequentialIsSharded() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.sequential();

    assertThat(derived, instanceOf(ShardedReferenceStream.class));

    assertFalse(derived.isParallel());
  }

  @Test
  public void testParallelIsSharded() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.parallel();

    assertThat(derived, instanceOf(ShardedReferenceStream.class));

    assertTrue(derived.isParallel());
  }

  @Test
  public void testUnorderedIsSharded() {
    Stream<Stream<String>> shards = Stream.of(Stream.of("a", "c"), Stream.of("b", "d"));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedReferenceStream<String> stream = new ShardedReferenceStream<>(shards, null, 0, source);

    Stream<String> derived = stream.unordered();

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
  }
}
