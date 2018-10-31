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

package com.terracottatech.sovereign.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.test.data.Animals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests to ensure {@link java.util.stream.Stream Stream} instances created
 * from a {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset}
 * close appropriately.
 * <p/>
 * Due to the <i>interesting</i> resource consumption involved with traversing
 * a secondary index, tests using a secondary index dominate these tests.
 * <p/>
 * JDK-provided {@code Stream} instances to <b>not</b> self-close -- even when
 * the stream is exhausted.  For consistency sake, neither do the Sovereign
 * {@code Stream} implementations,
 *
 * @author Clifford W. Johnson
 */
public class SovereignDatasetStreamClosureTest {

  private SovereignDataset<String> dataset;

  /**
   * One of the resource constraints is the number of concurrent read slots permitted by
   * {@link ABPlusTree} implementation.  Failing to properly close the transaction objects
   * used by the {@code ABPlusTree} will result in a failure once the available slots are
   * exhausted.
   */
  private final int repetitionCount = ABPlusTree.DEFAULT_NUMBER_OF_READ_SLOTS * 2;

  @Before
  public void setup() throws Exception {
    this.dataset = AnimalsDataset.createDataset(16 * 1024 * 1024);
    AnimalsDataset.addIndexes(this.dataset);
  }

  @After
  public void tearDown() throws IOException {
    this.dataset.getStorage().destroyDataSet(this.dataset.getUUID());
  }

  /**
   * Attempts to ensure that a {@link Stream#findAny()} does not leak resources.
   */
  @Test
  public void testCloseAllMatch() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.allMatch(r -> r.get(Animals.Schema.STATUS).isPresent());
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#anyMatch(Predicate)} does not leak resources.
   */
  @Test
  public void testCloseAnyMatch() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.anyMatch(Animals.Schema.STATUS.value().is(Animals.EXTINCT));
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#collect(Collector)} does not leak resources.
   */
  @Test
  public void testCloseCollect1Arg() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.collect(toList());
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#collect(Supplier, BiConsumer, BiConsumer)} does not leak resources.
   */
  @Test
  public void testCloseCollect3Arg() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        @SuppressWarnings("unused")   // Eclipse accommodation  https://bugs.eclipse.org/bugs/show_bug.cgi?id=470826
        List<Record<String>> ignored = mammalStream.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#count()} does not leak resources.
   */
  @Test
  public void testCloseCount() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#distinct()} does not leak resources.
   */
  @Test
  public void testCloseDistinctDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.distinct().collect(toList());
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#distinct()} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseDistinctShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.distinct().findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#filter(Predicate)} does not leak resources.
   */
  @Test
  public void testCloseFilterDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.filter(Animals.Schema.STATUS.value().is(Animals.EXTINCT)).collect(toList());
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#filter(Predicate)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseFilterShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.filter(Animals.Schema.STATUS.value().is(Animals.EXTINCT)).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#findAny()} does not leak resources.
   */
  @Test
  public void testCloseFindAny() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#findFirst()} does not leak resources.
   */
  @Test
  public void testCloseFindFirst() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.findFirst();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMap(Function)} does not leak resources.
   */
  @Test
  public void testCloseFlatMapDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMap(Stream::of).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMap(Function)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseFlatMapShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMap(Stream::of).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMapToDouble(Function)} does not leak resources.
   */
  @Test
  public void testCloseFlatMapToDoubleDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMapToDouble(r -> DoubleStream.of(
            Double.valueOf(r.get(Animals.Schema.OBSERVATIONS).orElse(0L).doubleValue()))).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMapToDouble(Function)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseFlatMapToDoubleShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMapToDouble(r -> DoubleStream.of(
            Double.valueOf(r.get(Animals.Schema.OBSERVATIONS).orElse(0L).doubleValue()))).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMapToInt(Function)} does not leak resources.
   */
  @Test
  public void testCloseFlatMapToIntDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMapToInt(r -> IntStream.of(
            Integer.valueOf(r.get(Animals.Schema.OBSERVATIONS).orElse(0L).intValue()))).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMapToInt(Function)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseFlatMapToIntShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMapToInt(r -> IntStream.of(
            Integer.valueOf(r.get(Animals.Schema.OBSERVATIONS).orElse(0L).intValue()))).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMapToLong(Function)} does not leak resources.
   */
  @Test
  public void testCloseFlatMapToLongDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMapToLong(r -> LongStream.of(r.get(Animals.Schema.OBSERVATIONS).orElse(0L))).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#flatMapToLong(Function)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseFlatMapToLongShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.flatMapToLong(r -> LongStream.of(r.get(Animals.Schema.OBSERVATIONS).orElse(0L))).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#forEach(Consumer)} does not leak resources.
   */
  @Test
  public void testCloseForEach() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      AtomicLong observations = new AtomicLong(0);
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.forEach(r -> observations.addAndGet(r.get(Animals.Schema.OBSERVATIONS).orElse(0L)));
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#forEachOrdered(Consumer)} does not leak resources.
   */
  @Test
  public void testCloseForEachOrdered() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      AtomicLong observations = new AtomicLong(0);
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.forEachOrdered(r -> observations.addAndGet(r.get(Animals.Schema.OBSERVATIONS).orElse(0L)));
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#iterator()} does not leak resources.
   */
  @Test
  public void testCloseIteratorDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        final Iterator<Record<String>> mammals = mammalStream.iterator();
        Record<String> mammal = null;
        while (mammals.hasNext()) {
          mammal = mammals.next();
        }
        assertFalse(mammals.hasNext());
        assertNotNull(mammal);
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#iterator()} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseIteratorWithRemaining() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        int j = 2;
        final Iterator<Record<String>> mammals = mammalStream.iterator();
        Record<String> mammal = null;
        while (mammals.hasNext()) {
          mammal = mammals.next();
          if (j-- == 0) {
            break;
          }
        }
        assertTrue(mammals.hasNext());
        assertNotNull(mammal);
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#limit(long)} does not leak resources.
   */
  @Test
  public void testCloseLimitDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.limit(2).collect(toList());
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#limit(long)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseLimitShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.limit(2).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#map(Function)} does not leak resources.
   */
  @Test
  public void testCloseMapDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.map(Animals.Animal::new).collect(toList());
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#map(Function)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseMapShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.map(Animals.Animal::new).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#map(Function)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseMapShortCircuitWithClose() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream
            .map(Animals.Animal::new).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#mapToDouble(ToDoubleFunction)} does not leak resources.
   */
  @Test
  public void testCloseMapToDoubleDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.mapToDouble(r -> r.get(Animals.Schema.OBSERVATIONS).orElse(0L).doubleValue()).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#mapToDouble(ToDoubleFunction)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseMapToDoubleShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.mapToDouble(r -> r.get(Animals.Schema.OBSERVATIONS).orElse(0L).doubleValue()).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#mapToInt(ToIntFunction)} does not leak resources.
   */
  @Test
  public void testCloseMapToIntDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.mapToInt(r -> r.get(Animals.Schema.OBSERVATIONS).orElse(0L).intValue()).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#mapToInt(ToIntFunction)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseMapToIntShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.mapToInt(r -> r.get(Animals.Schema.OBSERVATIONS).orElse(0L).intValue()).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#mapToInt(ToIntFunction)} does not leak resources.
   */
  @Test
  public void testCloseMapToLongDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.mapToLong(r -> r.get(Animals.Schema.OBSERVATIONS).orElse(0L)).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#mapToInt(ToIntFunction)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseMapToLongShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.mapToLong(r -> r.get(Animals.Schema.OBSERVATIONS).orElse(0L)).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#max(Comparator)} does not leak resources.
   */
  @Test
  public void testCloseMax() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.max(
            (r1, r2) -> r1.get(Animals.Schema.OBSERVATIONS).orElse(Long.MIN_VALUE)
                .compareTo(r2.get(Animals.Schema.OBSERVATIONS).orElse(Long.MIN_VALUE)));
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#min(Comparator)} does not leak resources.
   */
  @Test
  public void testCloseMin() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.min(
            (r1, r2) -> r1.get(Animals.Schema.OBSERVATIONS).orElse(Long.MAX_VALUE)
                .compareTo(r2.get(Animals.Schema.OBSERVATIONS).orElse(Long.MAX_VALUE)));
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#noneMatch(Predicate)} does not leak resources.
   */
  @Test
  public void testCloseNoneMatch() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.noneMatch(r -> r.get(Animals.Schema.STATUS).isPresent());
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#peek(Consumer)} does not leak resources.
   */
  @Test
  public void testClosePeekDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.peek(r -> {}).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#peek(Consumer)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testClosePeekShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.peek(r -> {}).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#reduce(BinaryOperator)} does not leak resources.
   */
  @Test
  public void testCloseReduce1Arg() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.reduce((r1, r2) -> r2);
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#reduce(Object, BinaryOperator)} does not leak resources.
   */
  @Test
  public void testCloseReduce2Arg() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.reduce(null, (r1, r2) -> r2);
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#reduce(Object, BiFunction, BinaryOperator)} does not leak resources.
   */
  @Test
  public void testCloseReduce3Arg() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.reduce(0L, (sum, r) -> sum + r.get(Animals.Schema.OBSERVATIONS).orElse(0L), (l1, l2) -> l1 + l2);
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#sequential()} does not leak resources.
   */
  @Test
  public void testCloseSequentialDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.sequential().count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#sequential()} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseSequentialShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.sequential().findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#skip(long)} does not leak resources.
   */
  @Test
  public void testCloseSkipDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.skip(2).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#skip(long)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseSkipShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.skip(2).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#spliterator()} does not leak resources.
   */
  @Test
  public void testCloseSpliteratorDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.spliterator().forEachRemaining(r -> {});
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#spliterator()} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseSpliteratorShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream
            .spliterator().tryAdvance(r -> {});
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#sorted()} does not leak resources.
   */
  @Test
  public void testCloseSorted0ArgDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.map(Animals.Animal::new)
            .sorted().count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#sorted()} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseSorted0ArgShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.map(Animals.Animal::new)
            .sorted().findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#sorted(Comparator)} does not leak resources.
   */
  @Test
  public void testCloseSorted1ArgDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.map(Animals.Animal::new)
            .sorted(Animals.Animal.COMPARATOR_BY_CLASS).count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#sorted(Comparator)} does not leak resources.
   * This test uses a short-circuit terminal operation and does not fully
   * consume the stream.
   */
  @Test
  public void testCloseSorted1ArgShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.map(Animals.Animal::new)
            .sorted(Animals.Animal.COMPARATOR_BY_CLASS).findAny();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#toArray()} does not leak resources.
   */
  @Test
  public void testCloseToArray0Arg() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.toArray();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#toArray(IntFunction)} does not leak resources.
   */
  @Test
  public void testCloseToArray1Arg() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.toArray(Record[]::new);
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#unordered()} does not leak resources.
   */
  @Test
  public void testCloseUnorderedDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.unordered().count();
      }
    }
  }

  /**
   * Attempts to ensure that a {@link Stream#unordered()} does not leak resources.
   */
  @Test
  public void testCloseUnorderedShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      try (final Stream<Record<String>> mammalStream = getMammalStream()) {
        mammalStream.unordered().findAny();
      }
    }
  }

  /**
   * Ensures that a close of the first {@link Stream} in a chain closes the entire chain.
   */
  @Test
  public void testNestedClosureViaFirst() throws Exception {
    final AtomicBoolean firstStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean secondStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean thirdStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean fourthStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean lastStreamOnClose = new AtomicBoolean(false);

    try (final RecordStream<String> firstStream = this.dataset.records()) {
      firstStream.selfClose(false);
      final Stream<Record<String>> secondStream =
          firstStream.filter(Animals.Schema.TAXONOMIC_CLASS.value().is("mammal"));
      final Stream<Record<String>> thirdStream =
          secondStream.filter(Animals.Schema.STATUS.value().is(Animals.VULNERABLE));
      final Stream<Animals.Animal> fourthStream = thirdStream.map(Animals.Animal::new);
      final Stream<Animals.Animal> lastStream = fourthStream.filter(a -> a.getName().startsWith("w"));

      firstStream.onClose(() -> firstStreamOnClose.set(true));
      secondStream.onClose(() -> secondStreamOnClose.set(true));
      thirdStream.onClose(() -> thirdStreamOnClose.set(true));
      fourthStream.onClose(() -> fourthStreamOnClose.set(true));
      lastStream.onClose(() -> lastStreamOnClose.set(true));

      assertTrue(lastStream.findAny().isPresent());

      assertFalse(lastStreamOnClose.get());
      assertFalse(secondStreamOnClose.get());
      assertFalse(thirdStreamOnClose.get());
      assertFalse(fourthStreamOnClose.get());
      assertFalse(firstStreamOnClose.get());
    } finally {
      assertTrue(lastStreamOnClose.get());
      assertTrue(secondStreamOnClose.get());
      assertTrue(thirdStreamOnClose.get());
      assertTrue(fourthStreamOnClose.get());
      assertTrue(firstStreamOnClose.get());
    }
  }

  /**
   * Ensures that a close of the second {@link Stream} in a chain closes the entire chain.
   */
  @Test
  public void testNestedClosureViaSecond() throws Exception {
    final AtomicBoolean firstStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean secondStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean thirdStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean fourthStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean lastStreamOnClose = new AtomicBoolean(false);

    final RecordStream<String> firstStream = this.dataset.records();
    firstStream.selfClose(false);
    try (final Stream<Record<String>> secondStream =
             firstStream.filter(Animals.Schema.TAXONOMIC_CLASS.value().is("mammal"))) {
      final Stream<Record<String>> thirdStream =
          secondStream.filter(Animals.Schema.STATUS.value().is(Animals.VULNERABLE));
      final Stream<Animals.Animal> fourthStream = thirdStream.map(Animals.Animal::new);
      final Stream<Animals.Animal> lastStream = fourthStream.filter(a -> a.getName().startsWith("w"));

      firstStream.onClose(() -> firstStreamOnClose.set(true));
      secondStream.onClose(() -> secondStreamOnClose.set(true));
      thirdStream.onClose(() -> thirdStreamOnClose.set(true));
      fourthStream.onClose(() -> fourthStreamOnClose.set(true));
      lastStream.onClose(() -> lastStreamOnClose.set(true));

      assertTrue(lastStream.findAny().isPresent());

      assertFalse(lastStreamOnClose.get());
      assertFalse(secondStreamOnClose.get());
      assertFalse(thirdStreamOnClose.get());
      assertFalse(fourthStreamOnClose.get());
      assertFalse(firstStreamOnClose.get());
    } finally {
      assertTrue(lastStreamOnClose.get());
      assertTrue(secondStreamOnClose.get());
      assertTrue(thirdStreamOnClose.get());
      assertTrue(fourthStreamOnClose.get());
      assertTrue(firstStreamOnClose.get());
    }
  }

  /**
   * Ensures that a close of the third {@link Stream} in a chain closes the entire chain.
   */
  @Test
  public void testNestedClosureViaThird() throws Exception {
    final AtomicBoolean firstStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean secondStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean thirdStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean fourthStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean lastStreamOnClose = new AtomicBoolean(false);

    final RecordStream<String> firstStream = this.dataset.records();
    firstStream.selfClose(false);
    final Stream<Record<String>> secondStream =
        firstStream.filter(Animals.Schema.TAXONOMIC_CLASS.value().is("mammal"));
    try (final Stream<Record<String>> thirdStream =
             secondStream.filter(Animals.Schema.STATUS.value().is(Animals.VULNERABLE))) {
      final Stream<Animals.Animal> fourthStream = thirdStream.map(Animals.Animal::new);
      final Stream<Animals.Animal> lastStream = fourthStream.filter(a -> a.getName().startsWith("w"));

      firstStream.onClose(() -> firstStreamOnClose.set(true));
      secondStream.onClose(() -> secondStreamOnClose.set(true));
      thirdStream.onClose(() -> thirdStreamOnClose.set(true));
      fourthStream.onClose(() -> fourthStreamOnClose.set(true));
      lastStream.onClose(() -> lastStreamOnClose.set(true));

      assertTrue(lastStream.findAny().isPresent());

      assertFalse(lastStreamOnClose.get());
      assertFalse(secondStreamOnClose.get());
      assertFalse(thirdStreamOnClose.get());
      assertFalse(fourthStreamOnClose.get());
      assertFalse(firstStreamOnClose.get());
    } finally {
      assertTrue(lastStreamOnClose.get());
      assertTrue(secondStreamOnClose.get());
      assertTrue(thirdStreamOnClose.get());
      assertTrue(fourthStreamOnClose.get());
      assertTrue(firstStreamOnClose.get());
    }
  }

  /**
   * Ensures that a close of the fourth {@link Stream} in a chain closes the entire chain.
   */
  @Test
  public void testNestedClosureViaFourth() throws Exception {
    final AtomicBoolean firstStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean secondStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean thirdStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean fourthStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean lastStreamOnClose = new AtomicBoolean(false);

    final RecordStream<String> firstStream = this.dataset.records();
    firstStream.selfClose(false);
    final Stream<Record<String>> secondStream =
        firstStream.filter(Animals.Schema.TAXONOMIC_CLASS.value().is("mammal"));
    final Stream<Record<String>> thirdStream =
        secondStream.filter(Animals.Schema.STATUS.value().is(Animals.VULNERABLE));
    try (final Stream<Animals.Animal> fourthStream = thirdStream.map(Animals.Animal::new)) {
      final Stream<Animals.Animal> lastStream = fourthStream.filter(a -> a.getName().startsWith("w"));

      firstStream.onClose(() -> firstStreamOnClose.set(true));
      secondStream.onClose(() -> secondStreamOnClose.set(true));
      thirdStream.onClose(() -> thirdStreamOnClose.set(true));
      fourthStream.onClose(() -> fourthStreamOnClose.set(true));
      lastStream.onClose(() -> lastStreamOnClose.set(true));

      assertTrue(lastStream.findAny().isPresent());

      assertFalse(lastStreamOnClose.get());
      assertFalse(secondStreamOnClose.get());
      assertFalse(thirdStreamOnClose.get());
      assertFalse(fourthStreamOnClose.get());
      assertFalse(firstStreamOnClose.get());
    } finally {
      assertTrue(lastStreamOnClose.get());
      assertTrue(secondStreamOnClose.get());
      assertTrue(thirdStreamOnClose.get());
      assertTrue(fourthStreamOnClose.get());
      assertTrue(firstStreamOnClose.get());
    }
  }

  /**
   * Ensures that a close of the last {@link Stream} in a chain closes the entire chain.
   */
  @Test
  public void testNestedClosureViaLast() throws Exception {
    final AtomicBoolean firstStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean secondStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean thirdStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean fourthStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean lastStreamOnClose = new AtomicBoolean(false);

    final RecordStream<String> firstStream = this.dataset.records();
    firstStream.selfClose(false);
    final Stream<Record<String>> secondStream =
        firstStream.filter(Animals.Schema.TAXONOMIC_CLASS.value().is("mammal"));
    final Stream<Record<String>> thirdStream =
        secondStream.filter(Animals.Schema.STATUS.value().is(Animals.VULNERABLE));
    final Stream<Animals.Animal> fourthStream = thirdStream.map(Animals.Animal::new);
    try (final Stream<Animals.Animal> lastStream = fourthStream.filter(a -> a.getName().startsWith("w"))) {

      firstStream.onClose(() -> firstStreamOnClose.set(true));
      secondStream.onClose(() -> secondStreamOnClose.set(true));
      thirdStream.onClose(() -> thirdStreamOnClose.set(true));
      fourthStream.onClose(() -> fourthStreamOnClose.set(true));
      lastStream.onClose(() -> lastStreamOnClose.set(true));

      assertTrue(lastStream.findAny().isPresent());

      assertFalse(lastStreamOnClose.get());
      assertFalse(secondStreamOnClose.get());
      assertFalse(thirdStreamOnClose.get());
      assertFalse(fourthStreamOnClose.get());
      assertFalse(firstStreamOnClose.get());
    } finally {
      assertTrue(lastStreamOnClose.get());
      assertTrue(secondStreamOnClose.get());
      assertTrue(thirdStreamOnClose.get());
      assertTrue(fourthStreamOnClose.get());
      assertTrue(firstStreamOnClose.get());
    }
  }

  /**
   * Ensures that a close of the first {@code Stream} in a chain with a {@code sorted} operation
   * closes the entire chain.
   */
  @Test
  public void testNestedClosureOfSortViaFirst() throws Exception {
    final CellDefinition<String> manager = CellDefinition.define("manager", Type.STRING);
    final Consumer<Record<String>> mutation =
        this.dataset.applyMutation(SovereignDataset.Durability.IMMEDIATE, r -> UpdateOperation.write(manager).<String>value("clifford").apply(r));

    final AtomicBoolean firstStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean lastStreamOnClose = new AtomicBoolean(false);

    try (final RecordStream<String> firstStream = this.dataset.records()) {
      firstStream.selfClose(false);

      final Stream<Record<String>> midStream = firstStream
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .sorted((r1, r2) -> r1.get(OBSERVATIONS).orElse(0L).compareTo(r2.get(OBSERVATIONS).orElse(0L)))
          .skip(5)
          .sorted((r1, r2) -> r1.getKey().compareTo(r2.getKey()))
          .filter(STATUS.exists());

      final Stream<Record<String>> lastStream = midStream.limit(5);

      firstStream.onClose(() -> firstStreamOnClose.set(true));
      lastStream.onClose(() -> lastStreamOnClose.set(true));

      lastStream.forEach(mutation);

      assertFalse(lastStreamOnClose.get());
      assertFalse(firstStreamOnClose.get());

    } finally {
      assertTrue(lastStreamOnClose.get());
      assertTrue(firstStreamOnClose.get());
    }
  }

  /**
   * Ensures that a close of the last {@code Stream} in a chain with a {@code sorted} operation
   * closes the entire chain.
   */
  @Test
  public void testNestedClosureOfSortViaLast() throws Exception {
    final CellDefinition<String> manager = CellDefinition.define("manager", Type.STRING);
    final Consumer<Record<String>> mutation =
        this.dataset.applyMutation(SovereignDataset.Durability.IMMEDIATE, r -> UpdateOperation.write(manager).<String>value("clifford").apply(r));

    final AtomicBoolean firstStreamOnClose = new AtomicBoolean(false);
    final AtomicBoolean lastStreamOnClose = new AtomicBoolean(false);

    final RecordStream<String> firstStream = this.dataset.records();
    firstStream.selfClose(false);

    final Stream<Record<String>> midStream = firstStream
        .filter(TAXONOMIC_CLASS.value().is("mammal"))
        .sorted((r1, r2) -> r1.get(OBSERVATIONS).orElse(0L).compareTo(r2.get(OBSERVATIONS).orElse(0L)))
        .skip(5)
        .sorted((r1, r2) -> r1.getKey().compareTo(r2.getKey()))
        .filter(STATUS.exists());

    try (final Stream<Record<String>> lastStream = midStream.limit(5)) {

      firstStream.onClose(() -> firstStreamOnClose.set(true));
      lastStream.onClose(() -> lastStreamOnClose.set(true));

      lastStream.forEach(mutation);

      assertFalse(lastStreamOnClose.get());
      assertFalse(firstStreamOnClose.get());

    } finally {
      assertTrue(lastStreamOnClose.get());
      assertTrue(firstStreamOnClose.get());
    }
  }

  /**
   * Attempts to ensure that joining two {@code Stream}s using {@link Stream#concat(Stream, Stream)}
   * does not leak resources.  This test fully consumes the joined streams.
   */
  @Test
  public void testCloseConcatDrained() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      final Stream<Record<String>> birdStream = this.dataset.records()
          .filter(Animals.Schema.TAXONOMIC_CLASS.value().is("bird"));
      try (final Stream<Record<String>> mammalsAndBirds = Stream.concat(getMammalStream(), birdStream)) {
        mammalsAndBirds.count();
      }
    }
  }

  /**
   * Attempts to ensure that joining two {@code Stream}s using {@link Stream#concat(Stream, Stream)}
   * does not leak resources.  This test uses a short-circuit terminal operation and does not fully
   * consume both streams.
   */
  @Test
  public void testCloseConcatShortCircuit() throws Exception {
    for (int i = 0; i < repetitionCount; i++) {
      final Stream<Record<String>> birdStream = this.dataset.records()
          .filter(Animals.Schema.TAXONOMIC_CLASS.value().is("bird"));
      try (final Stream<Record<String>> mammalsAndBirds = Stream.concat(getMammalStream(), birdStream)) {
        mammalsAndBirds.filter(Animals.Schema.STATUS.value().is(Animals.ENDANGERED)).findAny();
      }
    }
  }

  /**
   * Gets a test {@link Stream} (of mammals) that makes use of a secondary index.
   *
   * @return a new {@code Stream}
   */
  private Stream<Record<String>> getMammalStream() {
    // This stream expression is expected to use an index on TAXONOMIC_CLASS ...
    // TODO: Add test to ensure use of TAXONOMIC_CLASS index
    return this.dataset.records().filter(Animals.Schema.TAXONOMIC_CLASS.value().is("mammal"));
  }
}
