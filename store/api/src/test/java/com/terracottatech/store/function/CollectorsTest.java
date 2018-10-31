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

package com.terracottatech.store.function;

import org.junit.Test;

import java.util.AbstractMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.terracottatech.store.function.Collectors.VarianceType.POPULATION;
import static com.terracottatech.store.function.Collectors.VarianceType.SAMPLE;
import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.function.Collectors.filtering;
import static com.terracottatech.store.function.Collectors.groupingBy;
import static com.terracottatech.store.function.Collectors.groupingByConcurrent;
import static com.terracottatech.store.function.Collectors.mapping;
import static com.terracottatech.store.function.Collectors.partitioningBy;
import static com.terracottatech.store.function.Collectors.varianceOfDouble;
import static com.terracottatech.store.function.Collectors.varianceOfInt;
import static com.terracottatech.store.function.Collectors.varianceOfLong;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class CollectorsTest {

  @SuppressWarnings("unchecked")
  private static <T> T mock(Class<?> classToMock) {
    return org.mockito.Mockito.mock((Class<T>) classToMock);
  }

  @Test
  public void testCompositeSupplier() {
    Object state1 = new Object();
    Object state2 = new Object();
    Supplier<Object> supplier1 = mock(Supplier.class);
    when(supplier1.get()).thenReturn(state1);
    Supplier<Object> supplier2 = mock(Supplier.class);
    when(supplier2.get()).thenReturn(state2);

    BiConsumer<Object, Object> accumulator = mock(BiConsumer.class);
    BinaryOperator<Object> combiner = mock(BinaryOperator.class);
    Function<Object, Object> finisher = mock(Function.class);

    Collector<Object, List<Object>, List<Object>> collector = Collectors.composite(
            Collector.of(supplier1, accumulator, combiner, finisher),
            Collector.of(supplier2, accumulator, combiner, finisher)
    );

    assertThat(collector.supplier().get(), contains(state1, state2));

    verify(supplier1).get();
    verify(supplier2).get();
    verifyZeroInteractions(accumulator);
    verifyZeroInteractions(combiner);
    verifyZeroInteractions(finisher);
  }

  @Test
  public void testCompositeAccumulator() {
    BiConsumer<Object, Object> accumulator1 = mock(BiConsumer.class);
    BiConsumer<Object, Object> accumulator2 = mock(BiConsumer.class);

    Supplier<Object> supplier = mock(Supplier.class);
    BinaryOperator<Object> combiner = mock(BinaryOperator.class);
    Function<Object, Object> finisher = mock(Function.class);

    Collector<Object, List<Object>, List<Object>> collector = Collectors.composite(
            Collector.of(supplier, accumulator1, combiner, finisher),
            Collector.of(supplier, accumulator2, combiner, finisher)
    );

    Object state1 = new Object();
    Object state2 = new Object();
    Object element = new Object();
    List<Object> state = asList(state1, state2);
    collector.accumulator().accept(state, element);

    verify(accumulator1).accept(same(state1), same(element));
    verify(accumulator2).accept(same(state2), same(element));

    verifyZeroInteractions(supplier);
    verifyZeroInteractions(combiner);
    verifyZeroInteractions(finisher);
  }

  @Test
  public void testCompositeCombiner() {
    Object state1A = new Object();
    Object state1B = new Object();
    Object state1AB = new Object();
    BinaryOperator<Object> combiner1 = mock(BinaryOperator.class);
    when(combiner1.apply(state1A, state1B)).thenReturn(state1AB);

    Object state2A = new Object();
    Object state2B = new Object();
    Object state2AB = new Object();
    BinaryOperator<Object> combiner2 = mock(BinaryOperator.class);
    when(combiner2.apply(state2A, state2B)).thenReturn(state2AB);

    Supplier<Object> supplier = mock(Supplier.class);
    BiConsumer<Object, Object> accumulator = mock(BiConsumer.class);
    Function<Object, Object> finisher = mock(Function.class);

    Collector<Object, List<Object>, List<Object>> collector = Collectors.composite(
            Collector.of(supplier, accumulator, combiner1, finisher),
            Collector.of(supplier, accumulator, combiner2, finisher)
    );

    List<Object> stateA = asList(state1A, state2A);
    List<Object> stateB = asList(state1B, state2B);
    assertThat(collector.combiner().apply(stateA, stateB), contains(state1AB, state2AB));

    verify(combiner1).apply(same(state1A), same(state1B));
    verify(combiner2).apply(same(state2A), same(state2B));

    verifyZeroInteractions(supplier);
    verifyZeroInteractions(accumulator);
    verifyZeroInteractions(finisher);
  }

  @Test
  public void testCompositeFinisher() {
    Object state1 = new Object();
    Object finalState1 = new Object();
    Function<Object, Object> finisher1 = mock(Function.class);
    when(finisher1.apply(state1)).thenReturn(finalState1);

    Object state2 = new Object();
    Object finalState2 = new Object();
    Function<Object, Object> finisher2 = mock(Function.class);
    when(finisher2.apply(state2)).thenReturn(finalState2);

    Supplier<Object> supplier = mock(Supplier.class);
    BiConsumer<Object, Object> accumulator = mock(BiConsumer.class);
    BinaryOperator<Object> combiner = mock(BinaryOperator.class);

    Collector<Object, List<Object>, List<Object>> collector = Collectors.composite(
            Collector.of(supplier, accumulator, combiner, finisher1),
            Collector.of(supplier, accumulator, combiner, finisher2)
    );

    List<Object> state = asList(state1, state2);
    assertThat(collector.finisher().apply(state), contains(finalState1, finalState2));

    verify(finisher1).apply(same(state1));
    verify(finisher2).apply(same(state2));

    verifyZeroInteractions(supplier);
    verifyZeroInteractions(accumulator);
    verifyZeroInteractions(combiner);
  }

  @Test
  public void testCompositeCollection() {
    Collector<Integer, ?, List<Object>> collector = Collectors.composite(
            Collectors.counting(),
            Collectors.summarizingInt(Integer::intValue),
            Collectors.partitioningBy(n -> n % 2 == 0, java.util.stream.Collectors.toList())
    );

    List<Object> results = IntStream.range(0, 10).boxed().collect(collector);

    assertThat(results, hasSize(3));

    assertThat(results.get(0), is(10L));
    assertThat(((IntSummaryStatistics) results.get(1)).getCount(), is(10L));
    assertThat(((IntSummaryStatistics) results.get(1)).getMin(), is(0));
    assertThat(((IntSummaryStatistics) results.get(1)).getMax(), is(9));
    assertThat(((IntSummaryStatistics) results.get(1)).getSum(), is(45L));
    assertThat(((IntSummaryStatistics) results.get(1)).getAverage(), is(4.5D));

    @SuppressWarnings("unchecked")
    Map<Boolean, List<Integer>> map = (Map<Boolean, List<Integer>>) results.get(2);
    assertThat(map, allOf(
            hasEntry(Boolean.FALSE, asList(1, 3, 5, 7, 9)),
            hasEntry(Boolean.TRUE, asList(0, 2, 4, 6, 8))
    ));
  }

  @Test
  public void testPopulationVarianceOnEmptyIntStream() {
    assertThat(Stream.<Integer>empty().collect(varianceOfInt(Integer::intValue, POPULATION)), is(Optional.empty()));
  }

  @Test
  public void testPopulationVarianceOnEmptyLongStream() {
    assertThat(Stream.<Long>empty().collect(varianceOfLong(Long::longValue, POPULATION)), is(Optional.empty()));
  }

  @Test
  public void testPopulationVarianceOnEmptyDoubleStream() {
    assertThat(Stream.<Double>empty().collect(varianceOfDouble(Double::doubleValue, POPULATION)), is(Optional.empty()));
  }

  @Test
  public void testSampleVarianceOnEmptyIntStream() {
    assertThat(Stream.<Integer>empty().collect(varianceOfInt(Integer::intValue, SAMPLE)), is(Optional.empty()));
  }

  @Test
  public void testSampleVarianceOnEmptyLongStream() {
    assertThat(Stream.<Long>empty().collect(varianceOfLong(Long::longValue, SAMPLE)), is(Optional.empty()));
  }

  @Test
  public void testSampleVarianceOnEmptyDoubleStream() {
    assertThat(Stream.<Double>empty().collect(varianceOfDouble(Double::doubleValue, SAMPLE)), is(Optional.empty()));
  }

  @Test
  public void testPopulationVarianceOnSingletonIntStream() {
    assertThat(Stream.of(1).collect(varianceOfInt(Integer::intValue, POPULATION)).get(), is(0.0));
  }

  @Test
  public void testPopulationVarianceOnSingletonLongStream() {
    assertThat(Stream.of(1L).collect(varianceOfLong(Long::longValue, POPULATION)).get(), is(0.0));
  }

  @Test
  public void testPopulationVarianceOnSingletonDoubleStream() {
    assertThat(Stream.of(1D).collect(varianceOfDouble(Double::doubleValue, POPULATION)).get(), is(0.0));
  }

  @Test
  public void testSampleVarianceOnSingletonIntStream() {
    assertThat(Stream.of(1).collect(varianceOfInt(Integer::intValue, SAMPLE)), is(Optional.empty()));
  }

  @Test
  public void testSampleVarianceOnSingletonLongStream() {
    assertThat(Stream.of(1L).collect(varianceOfLong(Long::longValue, SAMPLE)), is(Optional.empty()));
  }

  @Test
  public void testSampleVarianceOnSingletonDoubleStream() {
    assertThat(Stream.of(1D).collect(varianceOfDouble(Double::doubleValue, SAMPLE)), is(Optional.empty()));
  }

  @Test
  public void testPopulationVarianceOnIntStream() {
    assertThat(Stream.of(1, 2, 3).collect(varianceOfInt(Integer::intValue, POPULATION)).get(), is(0.6666666666666666));
  }

  @Test
  public void testPopulationVarianceOnLongStream() {
    assertThat(Stream.of(1L, 2L, 3L).collect(varianceOfLong(Long::longValue, POPULATION)).get(), is(0.6666666666666666));
  }

  @Test
  public void testPopulationVarianceOnDoubleStream() {
    assertThat(Stream.of(1d, 2d, 3d).collect(varianceOfDouble(Double::doubleValue, POPULATION)).get(), is(0.6666666666666666));
  }

  @Test
  public void testSampleVarianceOnIntStream() {
    assertThat(Stream.of(1, 2, 3).collect(varianceOfInt(Integer::intValue, SAMPLE)).get(), is(1.0));
  }

  @Test
  public void testSampleVarianceOnLongStream() {
    assertThat(Stream.of(1L, 2L, 3L).collect(varianceOfLong(Long::longValue, SAMPLE)).get(), is(1.0));
  }

  @Test
  public void testSampleVarianceOnDoubleStream() {
    assertThat(Stream.of(1d, 2d, 3d).collect(varianceOfDouble(Double::doubleValue, SAMPLE)).get(), is(1.0));
  }

  @Test
  public void testFilteringWithFullFilter() {
    assertThat(Stream.of(1, 2, 3).collect(filtering(n -> false, counting())), is(0L));
  }

  @Test
  public void testFilteringWithNoFilter() {
    assertThat(Stream.of(1, 2, 3).collect(filtering(n -> true, counting())), is(3L));
  }

  @Test
  public void testFilteringWithEmptyStream() {
    assertThat(Stream.empty().collect(filtering(n -> false, counting())), is(0L));
  }

  @Test
  public void testFilteringWithSimpleFilter() {
    assertThat(Stream.of(1, 2, 3).collect(filtering(n -> (n % 2) == 0, counting())), is(1L));
  }

  @Test
  public void testGroupingEmptyStream() {
    Map<?, ?> map = Stream.empty()
            .collect(groupingBy(mock(Function.class), mock(Collector.class)));
    assertTrue(map.isEmpty());
  }

  @Test
  public void testGroupingStringsByLengthCounting() {
    Map<Integer, Long> map = Stream.of("x", "zz", "y")
            .collect(groupingBy(String::length, counting()));
    assertMapEquals(map, entry(2, 1L), entry(1, 2L));
  }

  @Test
  public void testGroupingStringsByLengthCountingConcurrently() {
    ConcurrentMap<Integer, Long> concurrentMap = Stream.of("x", "zz", "y")
            .collect(groupingByConcurrent(String::length, counting()));
    assertMapEquals(concurrentMap, entry(2, 1L), entry(1, 2L));
  }

  @Test
  public void testPartitioningEmptyStream() {
    Map<Boolean, Long> map = Stream.empty()
            .collect(partitioningBy(mock(Predicate.class), counting()));
    assertMapEquals(map, entry(true, 0L), entry(false, 0L));
  }

  @Test
  public void testPartitioningByCaseCounting() {
    Map<Boolean, Long> map = Stream.of('a', 'A', 'b')
            .collect(partitioningBy(Character::isUpperCase, counting()));
    assertMapEquals(map, entry(true, 1L), entry(false, 2L));
  }

  @Test
  public void testMappingEmpty() {
    Map<?, Long> map = Stream.empty()
            .collect(mapping(mock(Function.class), groupingBy(identity(), counting())));
    assertTrue(map.isEmpty());
  }

  @Test
  public void testMappingToUpperCaseCounting() {
    Map<String, Long> map = Stream.of("aa", "Aa", "b")
            .collect(mapping(String::toUpperCase, groupingBy(identity(), counting())));
    assertMapEquals(map, entry("AA", 2L), entry("B", 1L));
  }

  @Test
  public void testFilteringEmpty() {
    Map<?, Long> map = Stream.empty()
            .collect(filtering(mock(Predicate.class), groupingBy(identity(), counting())));
    assertTrue(map.isEmpty());
  }

  @Test
  public void testFilteringByCaseCounting() {
    Map<Character, Long> map = Stream.of('a', 'A', 'b', 'b')
            .collect(filtering(Character::isLowerCase, groupingBy(identity(), counting())));
    assertMapEquals(map, entry('a', 1L), entry('b', 2L));
  }

  @SuppressWarnings("varargs")
  @SafeVarargs
  private final <K, V> void assertMapEquals(Map<K, V> actual, Map.Entry<K, V>... entries) {
    assertThat(actual.entrySet(), containsInAnyOrder(entries));
  }

  private <K, V> Map.Entry<K, V> entry(K key, V value) {
    return new AbstractMap.SimpleEntry<>(key, value);
  }
}
