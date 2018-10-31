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

package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignDataset.Durability;
import com.terracottatech.sovereign.plan.StreamPlan;
import com.terracottatech.store.internal.InternalRecord;
import com.terracottatech.test.data.Animals;
import com.terracottatech.test.data.Animals.Animal;
import com.terracottatech.sovereign.impl.AnimalsDataset;
import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.compute.CellComparison;
import com.terracottatech.store.common.dataset.stream.PackageSupport;
import com.terracottatech.store.common.dataset.stream.PipelineMetaData;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.Operation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.store.common.dataset.stream.WrappedDoubleStream;
import com.terracottatech.store.common.dataset.stream.WrappedIntStream;
import com.terracottatech.store.common.dataset.stream.WrappedLongStream;
import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;
import com.terracottatech.store.common.dataset.stream.WrappedStream;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.spi.store.PersistentRecord;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.function.BuildableToLongFunction;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
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
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.test.data.Animals.ANIMALS;
import static com.terracottatech.test.data.Animals.NEAR_THREATENED;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.sovereign.impl.SovereignDatasetStreamTestSupport.*;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Clifford W. Johnson
 */
// TODO: Replace index selection test implementations with something less "secret"
@SuppressWarnings({ "PointlessBooleanExpression", "ConstantConditions", "ResultOfMethodCallIgnored" })
public class RecordStreamOperationTest {

  private static final Class<?> MANAGED_ACTION_SUPPLIER_CLASS;
  static {
    try {
      // Obtain local reference to private, nested class
      MANAGED_ACTION_SUPPLIER_CLASS =
          Class.forName("com.terracottatech.sovereign.impl.dataset.RecordStreamImpl$ManagedActionSupplier");
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  private static final CellDefinition<String> MANAGER = CellDefinition.define("manager", Type.STRING);

  private SovereignDataset<String> dataset;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    /*
     * Create an Animal dataset with a short record lock timeout; this is required by
     * the locking tests.
     */
    SovereignBuilder<String, SystemTimeReference> builder =
        AnimalsDataset.getBuilder(16 * 1024 * 1024, false)
            .concurrency(1)
            .recordLockTimeout(1L, TimeUnit.SECONDS);
    this.dataset = AnimalsDataset.createDataset(builder);
    AnimalsDataset.addIndexes(this.dataset);
  }

  @After
  public void tearDown() throws Exception {
    dataset.getStorage().destroyDataSet(dataset.getUUID());
  }

  private RecordStreamImpl<String> getTestStream() {
    return (RecordStreamImpl<String>)this.dataset.records();
  }

  @Test
  public void testStreamBifurcation() throws Exception {
    final RecordStreamImpl<String> streamHead = getTestStream();
    streamHead.filter(TAXONOMIC_CLASS.value().is("mammal"));
    try {
      streamHead.filter(TAXONOMIC_CLASS.value().is("reptile"));
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testReuse() throws Exception {
    final RecordStreamImpl<String> streamHead = getTestStream();
    streamHead.count();
    try {
      streamHead.findAny();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testSetTerminalAction() throws Exception {

    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicMarkableReference<PipelineMetaData> passedMetaData = new AtomicMarkableReference<>(null, false);
      final Consumer<PipelineMetaData> firstAction = metaData -> passedMetaData.set(metaData, false);
      /*
       * Using setPipelineConsumer *without* preserving the terminal action set by
       * RecordStreamImpl.newInstance will cause a terminal action failure!
       */
      PackageSupport.setPipelineConsumer(stream.getMetaData(), firstAction);
      assertThat(stream.getTerminalAction(), is(sameInstance(firstAction)));
      assertThat(stream.getMetaData().getPipelineConsumer(), is(sameInstance(firstAction)));
      assertThat(passedMetaData.getReference(), is(nullValue()));

      final Consumer<PipelineMetaData> secondAction = metaData -> passedMetaData.attemptMark(metaData, true);
      stream.appendTerminalAction(secondAction);

      try {
        stream.count();
        fail();
      } catch (IllegalStateException e) {
        assertThat(e.getMessage(), is(equalTo("spliterator not set by Consumer.accept")));
      }
    }
  }

  @Test
  public void testAppendTerminalAction() throws Exception {

    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final Consumer<PipelineMetaData> firstAction = stream.getTerminalAction();
      assertThat(firstAction, is(notNullValue()));

      final AtomicMarkableReference<PipelineMetaData> passedMetaData = new AtomicMarkableReference<>(null, false);
      final Consumer<PipelineMetaData> secondAction = metaData -> passedMetaData.set(metaData, false);
      stream.appendTerminalAction(secondAction);
      assertThat(passedMetaData.getReference(), is(nullValue()));

      final Consumer<PipelineMetaData> thirdAction = metaData -> passedMetaData.attemptMark(metaData, true);
      stream.appendTerminalAction(thirdAction);

      assertThat(stream.count(), is(not(0L)));

      final boolean[] mark = new boolean[1];
      assertThat(passedMetaData.get(mark), is(sameInstance(stream.getMetaData())));
      assertThat(mark[0], is(true));
    }
  }

  @Test
  public void testAllMatch() throws Exception {
    final Predicate<Record<?>> predicate = TAXONOMIC_CLASS.value().is("mammal");
    testOperation(TerminalOperation.ALL_MATCH, stream -> assertFalse(stream.allMatch(predicate)), false, predicate);
  }

  @Test
  public void testAnyMatch() throws Exception {
    final Predicate<Record<?>> predicate = TAXONOMIC_CLASS.value().is("mammal");
    testOperation(TerminalOperation.ANY_MATCH, stream -> assertTrue(stream.anyMatch(predicate)), false, predicate);
  }

  @Test
  public void testCollect1Arg() throws Exception {
    final Collector<Record<String>, ?, List<Record<String>>> collector = toList();
    testOperation(TerminalOperation.COLLECT_1,
        stream -> assertThat(stream.collect(collector), is(iterableWithSize(ANIMALS.size()))),
        false, collector);
  }

  @Test
  public void testCollect3Arg() throws Exception {
    final Supplier<ArrayList<Object>> supplier = ArrayList::new;
    final BiConsumer<ArrayList<Object>, Record<String>> accumulator = ArrayList::add;
    final BiConsumer<ArrayList<Object>, ArrayList<Object>> combiner = ArrayList::addAll;
    testOperation(TerminalOperation.COLLECT_3,
        stream -> assertThat(stream.collect(supplier, accumulator, combiner), is(iterableWithSize(ANIMALS.size()))),
        false, supplier, accumulator, combiner);
  }

  @Test
  public void testCount() throws Exception {
    testOperation(TerminalOperation.COUNT, stream -> assertThat(stream.count(), is((long)ANIMALS.size())), false);
  }

  @Test
  public void testDistinct() throws Exception {
    testOperation(IntermediateOperation.DISTINCT, stream -> {
      final Stream<Record<String>> nextStream = stream.distinct();
      assertThat(nextStream, is(instanceOf(RecordStream.class)));
    }, true);
  }

  @Test
  public void testFilter() throws Exception {
    final Predicate<Record<?>> filterPredicate = TAXONOMIC_CLASS.value().is("mammal");
    testOperation(IntermediateOperation.FILTER,
        stream -> {
          final Stream<Record<String>> nextStream = stream.filter(filterPredicate);
          assertThat(nextStream, is(instanceOf(RecordStream.class)));
        },
        false, filterPredicate);
  }

  @Test
  public void testFindAny() throws Exception {
    testOperation(TerminalOperation.FIND_ANY, stream -> assertThat(stream.findAny().isPresent(), is(true)), false);
  }

  @Test
  public void testFindFirst() throws Exception {
    testOperation(TerminalOperation.FIND_FIRST, stream -> assertThat(stream.findFirst().isPresent(), is(true)), false);
  }

  @Test
  public void testFlatMap() throws Exception {
    final Function<Record<String>, Stream<? extends Record<String>>> mapper = Stream::of;
    testOperation(IntermediateOperation.FLAT_MAP,
        stream -> {
          final Stream<Record<String>> nextStream = stream.flatMap(mapper);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        false, mapper);
  }

  @Test
  public void testFlatMapToDouble() throws Exception {
    final Function<Record<String>, DoubleStream> mapper = r ->
            Stream.of(r).mapToDouble(v -> OBSERVATIONS.longValueOr(0L).applyAsLong(v));
    testOperation(IntermediateOperation.FLAT_MAP_TO_DOUBLE, stream -> stream.flatMapToDouble(mapper), false, mapper);
  }

  @Test
  public void testFlatMapToInt() throws Exception {
    final Function<Record<String>, IntStream> mapper = r ->
        Stream.of(r).mapToInt(v -> (int) (OBSERVATIONS.longValueOr(0).applyAsLong(v)));
    testOperation(IntermediateOperation.FLAT_MAP_TO_INT, stream -> stream.flatMapToInt(mapper), false, mapper);
  }

  @Test
  public void testFlatMapToLong() throws Exception {
    final Function<Record<String>, LongStream> mapper = r ->
        Stream.of(r).mapToLong(OBSERVATIONS.longValueOr(0L));
    testOperation(IntermediateOperation.FLAT_MAP_TO_LONG, stream -> stream.flatMapToLong(mapper), false, mapper);
  }

  @Test
  public void testForEach() throws Exception {
    final Consumer<Record<String>> consumer = s -> { };
    testOperation(TerminalOperation.FOR_EACH, stream -> stream.forEach(consumer), false, consumer);
  }

  @Test
  public void testForEachOrdered() throws Exception {
    final Consumer<Record<String>> consumer = s -> { };
    testOperation(TerminalOperation.FOR_EACH_ORDERED, stream -> stream.forEachOrdered(consumer), false, consumer);
  }

  @Test
  public void testLimit() throws Exception {
    final long maxSize = 5;
    testOperation(IntermediateOperation.LIMIT,
        stream -> {
          final Stream<Record<String>> nextStream = stream.limit(maxSize);
          assertThat(nextStream, is(instanceOf(RecordStream.class)));
        },
        true, maxSize);   // TDB-1350 limit() is now an insertion point
  }

  @Test
  public void testMap() throws Exception {
    final Function<Record<String>, Animal> mapper = Animal::new;
    testOperation(IntermediateOperation.MAP,
        stream -> {
          final Stream<Animal> nextStream = stream.map(mapper);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        false, mapper);
  }

  @Test
  public void testMapToDouble() throws Exception {
    final ToDoubleFunction<Record<?>> mapper = r -> OBSERVATIONS.longValueOr(0L).applyAsLong(r);
    testOperation(IntermediateOperation.MAP_TO_DOUBLE,
        stream -> {
          final DoubleStream nextStream = stream.mapToDouble(mapper);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        false, mapper);
  }

  @Test
  public void testMapToInt() throws Exception {
    final ToIntFunction<Record<String>> mapper = r -> r.get(OBSERVATIONS).orElse(0L).intValue();
    testOperation(IntermediateOperation.MAP_TO_INT,
        stream -> {
          final IntStream nextStream = stream.mapToInt(mapper);
          assertThat(nextStream, is(instanceOf(WrappedIntStream.class)));
        },
        false, mapper);
  }

  @Test
  public void testMapToLong() throws Exception {
    final BuildableToLongFunction<Record<?>> mapper = OBSERVATIONS.longValueOr(0L);
    testOperation(IntermediateOperation.MAP_TO_LONG,
        stream -> {
          final LongStream nextStream = stream.mapToLong(mapper);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        false, mapper);
  }

  @Test
  public void testMax() throws Exception {
    final String expectedRecordName = ANIMALS.stream()
        .max((r1, r2) ->
            ofNullable(r1.getObservations()).orElse(0L)
                .compareTo(ofNullable(r2.getObservations()).orElse(0L)))
        .get().getName();

    final Comparator<Record<String>> comparator =
        (r1, r2) -> r1.get(OBSERVATIONS).orElse(0L).compareTo(r2.get(OBSERVATIONS).orElse(0L));
    testOperation(TerminalOperation.MAX_1,
        stream -> assertThat(stream.max(comparator).get().getKey(), is(equalTo(expectedRecordName))),
        false, comparator);
  }

  @Test
  public void testMin() throws Exception {
    final String expectedRecordName = ANIMALS.stream()
        .min((r1, r2) ->
            ofNullable(r1.getObservations()).orElse(0L)
                .compareTo(ofNullable(r2.getObservations()).orElse(0L)))
        .get().getName();

    final Comparator<Record<String>> comparator =
        (r1, r2) -> r1.get(OBSERVATIONS).orElse(0L).compareTo(r2.get(OBSERVATIONS).orElse(0L));

    testOperation(TerminalOperation.MIN_1,
        stream -> assertThat(stream.min(comparator).get().getKey(), is(equalTo(expectedRecordName))),
        false, comparator);
  }

  @Test
  public void testNoneMatch() throws Exception {
    final Predicate<Record<?>> predicate = TAXONOMIC_CLASS.value().is("mammal");
    testOperation(TerminalOperation.NONE_MATCH, stream -> assertFalse(stream.noneMatch(predicate)), false, predicate);
  }

  @Test
  public void testPeek() throws Exception {
    final Consumer<Record<String>> consumer = s -> { };
    testOperation(IntermediateOperation.PEEK,
        stream -> {
          final Stream<Record<String>> nextStream = stream.peek(consumer);
          assertThat(nextStream, is(instanceOf(RecordStream.class)));
        },
        false, consumer);
  }

  /**
   * Ensures that a {@code ManagedAction} instance is explicitly disallowed as a
   * {@code Stream.peek} argument.
   */
  @Test
  public void testPeekManagedAction() throws Exception {
    final class MockManagedAction
        implements ManagedAction<String>, Consumer<Record<String>> {
      @Override
      public void accept(final Record<String> cells) {
      }

      @Override
      public InternalRecord<String> begin(final InternalRecord<String> record) {
        return record;
      }

      @Override
      public void end(final InternalRecord<String> record) {
      }

      @Override
      public SovereignContainer<String> getContainer() {
        return null;
      }

    }

    final Operation operation = IntermediateOperation.PEEK;
    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      final AtomicBoolean closed = new AtomicBoolean();
      stream.onClose(() -> closed.set(true));

      try {
        stream.peek(new MockManagedAction());
        fail();
      } catch (IllegalArgumentException e) {
        // expected
      }

      assertThat(terminalAction.get(), is(operation.isTerminal()));
      assertThat(closed.get(), is(operation.isTerminal() && WrappedStream.SELF_CLOSE_DEFAULT));

      final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
      assertThat(pipeline.size(), is(0));
    }
  }

  @Test
  public void testReduce1Arg() throws Exception {
    final BinaryOperator<Record<String>> accumulator = (r1, r2) -> r2;
    testOperation(TerminalOperation.REDUCE_1,
        stream -> assertThat(stream.reduce(accumulator).get(), is(notNullValue())),
        false, accumulator);
  }

  @Test
  public void testReduce2Arg() throws Exception {
    final Record<String> identity = null;
    final BinaryOperator<Record<String>> accumulator = (r1, r2) -> r2;
    testOperation(TerminalOperation.REDUCE_2,
        stream -> assertThat(stream.reduce(identity, accumulator), is(notNullValue())),
        false, identity, accumulator);
  }

  @Test
  public void testReduce3Arg() throws Exception {
    final Record<?> identity = null;
    final BiFunction<Record<?>, Record<String>, Record<?>> accumulator = (r1, r2) -> r2;
    final BinaryOperator<Record<?>> combiner = (p1, p2) -> p2;
    testOperation(TerminalOperation.REDUCE_3,
        stream -> assertThat(stream.reduce(identity, accumulator, combiner), is(notNullValue())),
        false, identity, accumulator, combiner);
  }

  @Test
  public void testSkip() throws Exception {
    final long n = 1L;
    testOperation(IntermediateOperation.SKIP,
        stream -> {
          final Stream<Record<String>> nextStream = stream.skip(n);
          assertThat(nextStream, is(instanceOf(RecordStream.class)));
        },
        true,
        n);
  }

  @Test
  public void testSorted0Arg() throws Exception {
    testOperation(IntermediateOperation.SORTED_0, stream -> {
      final Stream<Record<String>> nextStream = stream.sorted();
      assertThat(nextStream, is(instanceOf(RecordStream.class)));
    }, true);
  }

  @Test
  public void testSorted1Arg() throws Exception {
    final Comparator<Record<String>> comparator = (r1, r2) -> r1.getKey().compareTo(r2.getKey());
    testOperation(IntermediateOperation.SORTED_1,
        stream -> {
          final Stream<Record<String>> nextStream = stream.sorted(comparator);
          assertThat(nextStream, is(instanceOf(RecordStream.class)));
        },
        true,
        comparator);
  }

  @Test
  public void testToArray0Arg() throws Exception {
    testOperation(TerminalOperation.TO_ARRAY_0,
        stream -> {
          final Object[] array = stream.toArray();
          assertThat(array, is(arrayWithSize(ANIMALS.size())));
          assertThat(array[0], is(instanceOf(Record.class)));
        }, false);
  }

  @Test
  public void testToArray1Arg() throws Exception {
    final IntFunction<Record<?>[]> generator = Record[]::new;
    testOperation(TerminalOperation.TO_ARRAY_1,
        stream -> {
          final Record<?>[] array = stream.toArray(generator);
          assertThat(array, is(arrayWithSize(ANIMALS.size())));
        },
        false, generator);
  }

  @Test
  public void testIterator() throws Exception {

    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      final AtomicBoolean closed = new AtomicBoolean();
      stream.onClose(() -> closed.set(true));

      final Iterator<Record<String>> streamIterator = stream.iterator();

      assertThat(terminalAction.get(), is(true));
      assertThat(closed.get(), is(false));

      assertTrue(streamIterator.hasNext());

      streamIterator.forEachRemaining(s -> { });

      assertFalse(streamIterator.hasNext());
      assertThat(closed.get(), is(false));

      final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
      assertThat(pipeline.size(), is(1));

      final PipelineOperation pipelineOperation = pipeline.get(0);
      assertThat(pipelineOperation.getOperation(), is(TerminalOperation.ITERATOR));
      assertThat(pipelineOperation.getArguments(), is(empty()));
    }
  }

  @Test
  public void testOnClose() throws Exception {
    final RecordStreamImpl<String> stream = getTestStream();
    final AtomicBoolean terminalAction = new AtomicBoolean();
    stream.appendTerminalAction(metaData -> terminalAction.set(true));

    final AtomicBoolean closed = new AtomicBoolean();
    stream.onClose(() -> closed.set(true));

    final Stream<Record<String>> nextStream = stream.onClose(() -> { });
    assertThat(nextStream, is(instanceOf(RecordStream.class)));
    assertThat(terminalAction.get(), is(false));
    assertThat(closed.get(), is(false));

    final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
    assertThat(pipeline.size(), is(0));
  }

  @Test
  public void testParallel() throws Exception {
    testOperation(IntermediateOperation.PARALLEL, stream -> {
      final Stream<Record<String>> nextStream = stream.parallel();
      assertThat(nextStream, is(instanceOf(RecordStream.class)));
    }, false);
  }

  @Test
  public void testSelfClose() throws Exception {
    testOperation(IntermediateOperation.SELF_CLOSE, stream -> {
      final Stream<Record<String>> nextStream = stream.selfClose(!WrappedStream.SELF_CLOSE_DEFAULT);
      assertThat(nextStream, is(instanceOf(RecordStream.class)));
    }, false);
  }

  @Test
  public void testSequential() throws Exception {
    testOperation(IntermediateOperation.SEQUENTIAL, stream -> {
      final Stream<Record<String>> nextStream = stream.sequential();
      assertThat(nextStream, is(instanceOf(RecordStream.class)));
    }, false);
  }

  @Test
  public void testExplain() throws Exception {
    final Consumer<StreamPlan> consumer = sp -> {};
    testOperation(IntermediateOperation.EXPLAIN,
            stream -> {
              final Stream<Record<String>> nextStream = stream.explain(consumer);
              assertThat(nextStream, is(instanceOf(RecordStream.class)));
            },
            false,
            consumer);
  }

  @Test
  public void testSpliterator() throws Exception {

    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      final AtomicBoolean closed = new AtomicBoolean();
      stream.onClose(() -> closed.set(true));

      final Spliterator<Record<String>> spliterator = stream.spliterator();

      assertThat(terminalAction.get(), is(true));
      assertThat(closed.get(), is(false));

      spliterator.forEachRemaining(s -> { });

      assertThat(closed.get(), is(false));
      assertFalse(spliterator.tryAdvance(s -> { }));

      final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
      assertThat(pipeline.size(), is(1));

      final PipelineOperation pipelineOperation = pipeline.get(0);
      assertThat(pipelineOperation.getOperation(), is(TerminalOperation.SPLITERATOR));
      assertThat(pipelineOperation.getArguments(), is(empty()));
    }
  }

  @Test
  public void testUnordered() throws Exception {
    testOperation(IntermediateOperation.UNORDERED, stream -> {
      final Stream<Record<String>> nextStream = stream.unordered();
      assertThat(nextStream, is(instanceOf(RecordStream.class)));
    }, false);
  }

  @Test
  public void testGetWrappedStream() throws Exception {
    final RecordStreamImpl<String> stream = getTestStream();
    final Stream<Record<String>> observedStream = stream.getNativeStream();
    assertThat(observedStream, is(notNullValue()));
    assertThat(observedStream, is(instanceOf(Stream.class)));
    assertThat(observedStream, is(not(instanceOf(WrappedStream.class))));
  }

  @Test
  public void testGetMetaData() throws Exception {
    final RecordStreamImpl<String> stream = getTestStream();
    final PipelineMetaData metaData = stream.getMetaData();
    assertThat(metaData, is(notNullValue()));
    final BaseStream<?, ?> headStream = metaData.getHeadStream();
    assertThat(headStream, is(notNullValue()));
    assertThat(headStream, is(sameInstance(stream.getNativeStream())));
    assertThat(metaData.isSelfClosing(), is(equalTo(WrappedStream.SELF_CLOSE_DEFAULT)));
    assertThat(metaData.getPipeline(), is(empty()));
    final Consumer<PipelineMetaData> pipelineConsumer = metaData.getPipelineConsumer();
    assertThat(pipelineConsumer, is(notNullValue()));
    assertThat(pipelineConsumer, is(sameInstance(stream.getTerminalAction())));
  }

  @Test
  public void testGetPipeline() throws Exception {
    final RecordStreamImpl<String> stream = getTestStream();
    assertThat(stream.getPipeline(), is(empty()));
  }

  @Test
  public void testGetTerminalAction() throws Exception {
    final RecordStreamImpl<String> stream = getTestStream();
    final Consumer<PipelineMetaData> terminalAction = stream.getTerminalAction();
    assertThat(terminalAction, is(notNullValue()));
    // The first terminal action is both a Consumer and Supplier
    assertThat(terminalAction, is(instanceOf(Supplier.class)));
  }

  @Test
  public void testIsSelfClosing() throws Exception {
    RecordStreamImpl<String> stream = getTestStream();
    assertThat(stream.getMetaData().isSelfClosing(), is(equalTo(WrappedStream.SELF_CLOSE_DEFAULT)));
    stream = (RecordStreamImpl<String>)stream.selfClose(!WrappedStream.SELF_CLOSE_DEFAULT);
    assertThat(stream.getMetaData().isSelfClosing(), is(equalTo(!WrappedStream.SELF_CLOSE_DEFAULT)));
    stream = (RecordStreamImpl<String>)stream.selfClose(WrappedStream.SELF_CLOSE_DEFAULT);
    assertThat(stream.getMetaData().isSelfClosing(), is(equalTo(WrappedStream.SELF_CLOSE_DEFAULT)));
  }

  @Test
  public void testIsParallel() throws Exception {
    RecordStreamImpl<String> stream = getTestStream();
    assertThat(stream.isParallel(), is(false));
    stream = (RecordStreamImpl<String>)stream.parallel();
    assertThat(stream.isParallel(), is(true));
    stream = (RecordStreamImpl<String>)stream.sequential();
    assertThat(stream.isParallel(), is(false));
  }

  /**
   * Ensures that, although the current batch of {@code Spliterator} implementations
   * do not support splitting for parallelism, a parallel stream still "works".
   */
  @Test
  public void testParallelPrimaryIndex() throws Exception {
    final Collector<Animal, ?, Map<String, List<Animal>>> animalByThreadCollector =
        groupingBy(a -> {
          final Thread currentThread = Thread.currentThread();
          return String.format("%s@%s", currentThread.getName(),
              Integer.toHexString(System.identityHashCode(currentThread)));
        });

    final Map<String, List<Animal>> expectedAnimalsByThread =
        ANIMALS.stream().unordered().parallel()
            .collect(animalByThreadCollector);
    final int threadsForJavaCollection = expectedAnimalsByThread.size();
    System.out.format("%s: threads used for Java Collection = %d%n", this.testName.getMethodName(), threadsForJavaCollection);
    final List<Animal> expectedAnimals = new ArrayList<>();
    expectedAnimalsByThread.values().forEach(expectedAnimals::addAll);

    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      final Map<String, List<Animal>> animalsByThread =
          stream.unordered().parallel()
              .map(Animal::new)
              .collect(animalByThreadCollector);
      final int threadsForRecordStream = animalsByThread.size();
      System.out.format("%s: threads used for RecordStream = %d%n", this.testName.getMethodName(), threadsForRecordStream);
      final List<Animal> actualMammals = new ArrayList<>(expectedAnimals.size());
      animalsByThread.values().forEach(actualMammals::addAll);

      assertThat(actualMammals, containsInAnyOrder(expectedAnimals.toArray(new Animal[expectedAnimals.size()])));
      assertThat(assignedIndexCell.get(), is(nullValue()));   // No secondary index currently means primary index
    }
  }

  /**
   * Ensures that, although the current batch of {@code Spliterator} implementations
   * do not support splitting for parallelism, a parallel stream still "works".
   */
  @Test
  public void testParallelSecondaryIndex() throws Exception {
    final Collector<Animal, ?, Map<String, List<Animal>>> animalByThreadCollector =
        groupingBy(a -> {
          final Thread currentThread = Thread.currentThread();
          return String.format("%s@%s", currentThread.getName(),
              Integer.toHexString(System.identityHashCode(currentThread)));
        });

    final Map<String, List<Animal>> expectedMammalsByThread =
        ANIMALS.stream().unordered().parallel()
            .filter(a -> a.getTaxonomicClass().equals("mammal"))
            .collect(animalByThreadCollector);
    final int threadsForJavaCollection = expectedMammalsByThread.size();
    System.out.format("%s: threads used for Java Collection = %d%n", this.testName.getMethodName(), threadsForJavaCollection);
    final List<Animal> expectedMammals = new ArrayList<>();
    expectedMammalsByThread.values().forEach(expectedMammals::addAll);

    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      final Map<String, List<Animal>> mammalsByThread =
          stream.unordered().parallel()
              .filter(TAXONOMIC_CLASS.value().is("mammal"))
              .map(Animal::new)
              .collect(animalByThreadCollector);
      final int threadsForRecordStream = mammalsByThread.size();
      System.out.format("%s: threads used for RecordStream = %d%n", this.testName.getMethodName(), threadsForRecordStream);
      final List<Animal> actualMammals = new ArrayList<>(expectedMammals.size());
      mammalsByThread.values().forEach(actualMammals::addAll);

      assertThat(actualMammals, containsInAnyOrder(expectedMammals.toArray(new Animal[expectedMammals.size()])));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }
  }

  @Test
  public void testSimpleIndexSelection() throws Exception {
    try (final Stream<Record<String>> stream =
             getTestStream().filter(TAXONOMIC_CLASS.value().is("mammal"))) {
       /*
        * Add a terminalAction that gets information from the assigned Spliterator *before* the
        * Spliterator is consumed.
        */
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      stream.spliterator();     // Force terminal action execution without consuming stream

      assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));
    }
  }

  /**
   * This test demonstrates that index selection can be based on {@code filter} operations
   * separated by (at least some) other operations.
   */
  @Test
  public void testComplexIndexSelection() throws Exception {
    try (final Stream<Record<String>> stream = getTestStream()) {
       /*
        * Add a terminalAction that gets information from the assigned Spliterator *before* the
        * Spliterator is consumed.
        */
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      @SuppressWarnings("unchecked")
      final List<PipelineOperation> pipeline =
          ((WrappedStream<Record<String>, Stream<Record<String>>>)stream).getPipeline();
      final Stream<Record<String>> aAnimals = stream.filter(TAXONOMIC_CLASS.value().isGreaterThan("a"));
      /*
       * The two filter operations are separated by a sort operation which should *not*
       * disable selection of the TAXONOMIC_CLASS index.
       */
      final Stream<Record<String>> sortedAAnimals = aAnimals.sorted(
          (r1, r2) -> r1.get(TAXONOMIC_CLASS).orElse("").compareTo(r2.get(TAXONOMIC_CLASS).orElse("")));
      final Stream<Record<String>> aMammals = sortedAAnimals.filter(TAXONOMIC_CLASS.value().is("mammal"));

      final List<Operation> operations = pipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(operations, Matchers.<Operation>contains(
          IntermediateOperation.FILTER, IntermediateOperation.SORTED_1, IntermediateOperation.FILTER));

      aMammals.spliterator();     // Force terminal action execution without consuming stream

      assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));
    }
  }

  @Test
  public void testChainedPredicateIndexSelection() throws Exception {
    final SovereignIndexing datasetIndexing = dataset.getIndexing();
    datasetIndexing.createIndex(OBSERVATIONS, SovereignIndexSettings.btree()).call();

    try (final Stream<Record<String>> stream = this.getTestStream()) {
       /*
        * Add a terminalAction that gets information from the assigned Spliterator *before* the
        * Spliterator is consumed.
        */
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      @SuppressWarnings("unchecked")
      final List<PipelineOperation> pipeline =
          ((WrappedStream<Record<String>, Stream<Record<String>>>)stream).getPipeline();

      final Stream<Record<String>> recordStream = stream
          .filter(OBSERVATIONS.value().isGreaterThan(1L))
          .filter(OBSERVATIONS.value().isLessThan(5L));

      final List<Operation> operations = pipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(operations, Matchers.<Operation>contains(IntermediateOperation.FILTER, IntermediateOperation.FILTER));

      recordStream.spliterator();

      assertThat(assignedIndexCell.get(), is(equalTo(OBSERVATIONS)));
    }
  }

  @Test
  public void testSeveralChainedPredicateIndexSelection() throws Exception {
    final SovereignIndexing datasetIndexing = dataset.getIndexing();
    datasetIndexing.createIndex(OBSERVATIONS, SovereignIndexSettings.btree()).call();

    try (final Stream<Record<String>> stream = this.getTestStream()) {
       /*
        * Add a terminalAction that gets information from the assigned Spliterator *before* the
        * Spliterator is consumed.
        */
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      @SuppressWarnings("unchecked")
      final List<PipelineOperation> pipeline =
          ((WrappedStream<Record<String>, Stream<Record<String>>>)stream).getPipeline();

      final Stream<Record<String>> recordStream = stream
          .filter(OBSERVATIONS.value().isGreaterThan(1L))
          .filter(OBSERVATIONS.value().isLessThan(5L))
          .filter((x)->true)
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .filter(OBSERVATIONS.value().isLessThanOrEqualTo(6L))
          .filter(OBSERVATIONS.value().isGreaterThanOrEqualTo(0L));

      final List<Operation> operations = pipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(operations, Matchers.<Operation>contains(IntermediateOperation.FILTER,
          IntermediateOperation.FILTER, IntermediateOperation.FILTER, IntermediateOperation.FILTER,
          IntermediateOperation.FILTER, IntermediateOperation.FILTER));

      recordStream.spliterator();

      assertThat(assignedIndexCell.get(), is(equalTo(OBSERVATIONS)));
    }
  }

  @Test
  public void testAndConjunctionIndexSelection() throws Exception {
    final SovereignIndexing datasetIndexing = dataset.getIndexing();
    datasetIndexing.createIndex(OBSERVATIONS, SovereignIndexSettings.btree()).call();

    try (final Stream<Record<String>> stream = this.getTestStream()) {
       /*
        * Add a terminalAction that gets information from the assigned Spliterator *before* the
        * Spliterator is consumed.
        */
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      @SuppressWarnings("unchecked")
      final List<PipelineOperation> pipeline =
          ((WrappedStream<Record<String>, Stream<Record<String>>>)stream).getPipeline();

      final Stream<Record<String>> recordStream = stream
          .filter(OBSERVATIONS.value().isGreaterThan(1L).and(OBSERVATIONS.value().isLessThan(5L)));

      final List<Operation> operations = pipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(operations, Matchers.<Operation>contains(IntermediateOperation.FILTER));

      recordStream.spliterator();

      assertThat(assignedIndexCell.get(), is(equalTo(OBSERVATIONS)));
    }
  }

  /**
   * This "test" shows that it is possible for {@code Stream.map} to emit {@code Stream<Record<K>>}
   * which, unfortunately, can't be determined at stream pipeline construction time ...
   */
  @Test
  public void testMapToRecordStream() throws Exception {
    try (final Stream<Record<String>> stream = this.getTestStream()) {
      final Stream<Record<String>> recordStream = stream.map(r -> r);
      assertThat(recordStream, is(not(instanceOf(RecordStream.class))));
      assertThat(recordStream.findAny().get(), is(instanceOf(Record.class)));
    }
  }

  @Test
  public void testManagedAction() throws Exception {
    try (final Stream<Record<String>> stream = this.getTestStream()) {
      /*
       * Add a terminalAction that gets information from the assigned Spliterator *before* the
       * Spliterator is consumed.
       */
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      final AtomicReference<ManagedAction<? extends Comparable<?>>> assignedManagedAction
          = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, assignedManagedAction);

      final Consumer<Record<String>> mutation =
          this.dataset.applyMutation(Durability.IMMEDIATE, r -> UpdateOperation.write(OBSERVATIONS).<String>longResultOf(cells -> 1 + cells.get(OBSERVATIONS).orElse(0L)).apply(r));
      assertThat(mutation, is(instanceOf(ManagedAction.class)));
      stream.filter(TAXONOMIC_CLASS.value().is("reptile")).forEach(mutation);

      @SuppressWarnings("unchecked")
      final List<Operation> operations =
          ((WrappedStream<Record<String>, Stream<Record<String>>>)stream)
              .getPipeline().stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(operations, Matchers.<Operation>contains(IntermediateOperation.FILTER, TerminalOperation.FOR_EACH));

      assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));
      assertThat(assignedManagedAction.get(), is(sameInstance(mutation)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testManagedActionClose() throws Exception {
    final MockAction mockAction = mock(MockAction.class);
    when(mockAction.getContainer()).thenReturn(((SovereignDatasetImpl)this.dataset).getContainer());    // unchecked
    try (final Stream<Record<String>> stream = this.getTestStream()) {
      stream.forEach(mockAction);
    }
    verify(mockAction, atLeastOnce()).close();
  }

  /**
   * This test constructs and executes the following pipeline:
   * <pre>{@code
   * this.dataset.records()
   *   .filter(TAXONOMIC_CLASS.value().is("mammal"))
   *   .sorted((r1, r2) -> r1.get(OBSERVATIONS).orElse(0L).compareTo(r2.get(OBSERVATIONS).orElse(0L)))
   *   .skip(5)
   *   .sorted((r1, r2) -> r1.getKey().compareTo(r2.getKey()))
   *   .filter(hasCell(STATUS))
   *   .limit(5)
   *   .forEach(this.dataset.applyMutation(Durability.IMMEDIATE, RecordFunctions.write(MANAGER, "clifford")))
   * }</pre>
   * and ensures that the placement of the {@code ManagedAction} is placed correctly for lock management
   * purposes.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testManagedActionAfterSort() throws Exception {

    final AtomicInteger closeCount = new AtomicInteger(0);
    final AtomicInteger streamClosed = new AtomicInteger(0);
    final AtomicInteger mammalStreamClosed = new AtomicInteger(0);
    final AtomicInteger sortedByObservationsClosed = new AtomicInteger(0);
    final AtomicInteger skippedClosed = new AtomicInteger(0);
    final AtomicInteger sortedByKeyClosed = new AtomicInteger(0);
    final AtomicInteger mammalsWithStatusClosed = new AtomicInteger(0);
    final AtomicInteger limitedClosed = new AtomicInteger(0);

    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final LocatorGenerator<String> spyOnGenerator =
          setLocatorGeneratorSpy((RecordStreamImpl<String>) stream);

      stream.onClose(() -> streamClosed.set(closeCount.incrementAndGet()));

      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      final AtomicReference<ManagedAction<? extends Comparable<?>>> assignedManagedAction
          = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, assignedManagedAction);

      final Consumer<Record<String>> mutation =
          this.dataset.applyMutation(Durability.IMMEDIATE, r -> UpdateOperation.write(MANAGER).<String>value("clifford").apply(r));

      final Stream<Record<String>> mammalStream =
          stream.filter(TAXONOMIC_CLASS.value().is("mammal"));
      mammalStream.onClose(() -> mammalStreamClosed.set(closeCount.incrementAndGet()));

      final Stream<Record<String>> sortedByObservations =
          mammalStream.sorted((r1, r2) -> r1.get(OBSERVATIONS).orElse(0L).compareTo(r2.get(OBSERVATIONS).orElse(0L)));
      sortedByObservations.onClose(() -> sortedByObservationsClosed.set(closeCount.incrementAndGet()));

      final Stream<Record<String>> skipped =
          sortedByObservations.skip(5);
      skipped.onClose(() -> skippedClosed.set(closeCount.incrementAndGet()));

      final Stream<Record<String>> sortedByKey =
          skipped.sorted((r1, r2) -> r1.getKey().compareTo(r2.getKey()));
      sortedByKey.onClose(() -> sortedByKeyClosed.set(closeCount.incrementAndGet()));

      final Stream<Record<String>> mammalsWithStatus =
          sortedByKey.filter(STATUS.exists());
      mammalsWithStatus.onClose(() -> mammalsWithStatusClosed.set(closeCount.incrementAndGet()));

      final Stream<Record<String>> limited =
          mammalsWithStatus.limit(5);
      limited.onClose(() -> limitedClosed.set(closeCount.incrementAndGet()));

      // Assert that the stream source Spliterator is *not* yet allocated
      verify(spyOnGenerator, never()).createSpliterator(
          any(RecordStreamImpl.class), any(CellComparison.class), any(ManagedAction.class));
      verify(spyOnGenerator, never()).createSpliterator(any(RecordStreamImpl.class), any(CellComparison.class));
      verify(spyOnGenerator, never()).createSpliterator(any(RecordStreamImpl.class));

      limited
          .forEach(mutation);

      // Assert that the stream source Spliterator is allocated *without* the ManagedAction
      verify(spyOnGenerator, times(1)).createSpliterator(
          any(RecordStreamImpl.class), any(CellComparison.class), isNull());
      verify(spyOnGenerator, never()).createSpliterator(any(RecordStreamImpl.class), any(CellComparison.class));
      verify(spyOnGenerator, never()).createSpliterator(any(RecordStreamImpl.class));

      @SuppressWarnings("unchecked") final List<PipelineOperation> pipelineOperations =
          ((WrappedStream<Record<String>, Stream<Record<String>>>)stream).getPipeline();
      final List<Operation> operations = pipelineOperations.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(operations,
          Matchers.<Operation>contains(
              IntermediateOperation.FILTER,     // 0
              IntermediateOperation.SORTED_1,   // 1
              IntermediateOperation.SKIP,       // 2
              IntermediateOperation.SORTED_1,   // 3
              IntermediateOperation.FILTER,     // 4
              IntermediateOperation.LIMIT,      // 5
              TerminalOperation.FOR_EACH));     // 6

      @SuppressWarnings("unchecked")
      final ManagedAction<String> managedAction = (ManagedAction<String>)mutation;
      checkManagedActionSupplier(pipelineOperations, 1, managedAction, false);
      checkManagedActionSupplier(pipelineOperations, 2, managedAction, false);
      checkManagedActionSupplier(pipelineOperations, 3, managedAction, false);
      checkManagedActionSupplier(pipelineOperations, 5, managedAction, true);   // TDB-1350 limit is now an insertion point

      assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));
      assertThat(assignedManagedAction.get(), is(not(sameInstance(managedAction))));
    }

    assertThat(closeCount.get(), is(7));
    assertThat(streamClosed.get(), is(1));
    assertThat(mammalStreamClosed.get(), is(2));
    assertThat(sortedByObservationsClosed.get(), is(3));
    assertThat(skippedClosed.get(), is(4));
    assertThat(sortedByKeyClosed.get(), is(5));
    assertThat(mammalsWithStatusClosed.get(), is(6));
    assertThat(limitedClosed.get(), is(7));

  }

  /**
   * Tests behavior of a {@link ManagedAction} <i>before</i> a {@code sorted} operation.
   * Note that this test shows that a {@code map(ManagedAction)} processes all of the stream
   * elements <i>before</i> {@code sorted} releases its first element.
   */
  @Test
  public void testManagedActionBeforeSort() throws Exception {
    long mammalCount = ANIMALS.stream().filter(a -> a.getTaxonomicClass().equals("mammal")).count();

    try (final Stream<Record<String>> stream = this.dataset.records()) {
      Function<Record<String>, Record<String>> mutation =
          this.dataset.applyMutation(Durability.IMMEDIATE,
              r -> UpdateOperation.write(MANAGER).<String>value("clifford").apply(r),
              (oldRecord, newRecord) -> newRecord);

      @SuppressWarnings({"unchecked", "rawtypes"})
      ProxyManagedAction<String, ?> managedActionProxy = new ProxyManagedAction((ManagedAction<String>)mutation);

      AtomicLong applyCount = new AtomicLong(0);
      List<Record<String>> managedMammals = stream
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .peek(r -> assertTrue(((managedActionProxy.getBeginCount() - managedActionProxy.getEndCount()) == 1)
              && (managedActionProxy.getApplyCount() == managedActionProxy.getEndCount())))
          .map(managedActionProxy)
          .peek(r -> {
            assertTrue(((managedActionProxy.getBeginCount() - managedActionProxy.getEndCount()) == 1)
                && (managedActionProxy.getApplyCount() == managedActionProxy.getBeginCount()));
            applyCount.set(managedActionProxy.getApplyCount());
          })
          .sorted(Comparator.comparing(Record::getKey))
          /*
           * By the time the stream releases records from the sort stage, all elements are consume from the
           * origin Spliterator and all locks are released!  Because the last call to Spliterator.tryAdvance
           * doesn't process an element, the apply count is one less than the begin & end counts once the
           * origin Spliterator is all consumed.  The ManagedAction is fully consumed by the time the
           * sort releases its first element.
           */
          .peek(r -> assertTrue((managedActionProxy.getBeginCount() == managedActionProxy.getEndCount())
              && ((managedActionProxy.getEndCount() - managedActionProxy.getApplyCount()) == 1)
              && applyCount.get() == mammalCount))
          .collect(toList());

      assertThat(managedActionProxy.getApplyCount(), is((long)managedMammals.size()));
      assertThat((long)managedMammals.size(), is(mammalCount));
      assertTrue(managedMammals.stream().allMatch(MANAGER.value().is("clifford")));

      @SuppressWarnings("unchecked") final List<PipelineOperation> pipelineOperations =
          ((WrappedStream<Record<String>, Stream<Record<String>>>)stream).getPipeline();
      final List<Operation> operations = pipelineOperations.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(operations,
          Matchers.<Operation>contains(
              IntermediateOperation.FILTER,     // 0
              IntermediateOperation.PEEK,       // 1
              IntermediateOperation.MAP,        // 2
              IntermediateOperation.PEEK,       // 3
              IntermediateOperation.SORTED_1,   // 4
              IntermediateOperation.PEEK,       // 5
              TerminalOperation.COLLECT_1));    // 6
    }
  }

  /**
   * Ensures a non-filtered, non-mutative stream does not lock.
   */
  @Test
  public void testPipelineLockingBasic() throws Exception {
    AtomicBoolean updated = new AtomicBoolean(false);
    AtomicBoolean timedOut = new AtomicBoolean(false);
    try (RecordStream<String> pipeline = this.getTestStream()) {
      assertThat(pipeline
              .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
              .count(),
          is((long)Animals.ANIMALS.size()));
    }

    assertTrue(updated.get());
    assertFalse(timedOut.get());
  }

  /**
   * Ensures a filtered, non-mutative stream does not lock.
   */
  @Test
  public void testPipelineLockingFilterNoMutation() throws Exception {
    AtomicBoolean updated = new AtomicBoolean(false);
    AtomicBoolean timedOut = new AtomicBoolean(false);
    try (RecordStream<String> pipeline = this.getTestStream()) {
      assertThat(pipeline
              .filter(TAXONOMIC_CLASS.value().is("mammal"))
              .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
              .count(),
          is(Animals.ANIMALS.stream().filter((a) -> a.getTaxonomicClass().equals("mammal")).count()));
    }

    assertTrue(updated.get());
    assertFalse(timedOut.get());
  }

  /**
   * Ensures a non-filtered, mutative stream locks.
   */
  @Test
  public void testPipelineLockingNoFilterMutation() throws Exception {
    AtomicBoolean updated = new AtomicBoolean(false);
    AtomicBoolean timedOut = new AtomicBoolean(false);
    AtomicInteger touches = new AtomicInteger(0);
    try (RecordStream<String> pipeline = this.getTestStream()) {
      pipeline
          .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
          .forEach(dataset.applyMutation(SovereignDataset.Durability.IMMEDIATE, (r) -> {
            CellSet cells = new CellSet(r);
            cells.set(OBSERVATIONS.newCell(1 + cells.get(OBSERVATIONS).orElse(0L)));
            touches.getAndIncrement();
            return cells;
          }));
    }

    assertThat(touches.get(), is(Animals.ANIMALS.size()));
    assertFalse(updated.get());
    assertTrue(timedOut.get());
  }

  /**
   * Ensures a filtered, mutative stream locks.
   */
  @Test
  public void testPipelineLockingFilterMutation() throws Exception {
    AtomicBoolean updated = new AtomicBoolean(false);
    AtomicBoolean timedOut = new AtomicBoolean(false);
    AtomicInteger touches = new AtomicInteger(0);
    try (RecordStream<String> pipeline = this.getTestStream()) {
      pipeline
          .filter(TAXONOMIC_CLASS.value().is("mammal"))
          .peek(lockingConsumer(this.dataset, "ocelot", updated, timedOut))
          .forEach(dataset.applyMutation(SovereignDataset.Durability.IMMEDIATE, (r) -> {
            CellSet cells = new CellSet(r);
            cells.set(OBSERVATIONS.newCell(1 + cells.get(OBSERVATIONS).orElse(0L)));
            touches.getAndIncrement();
            return cells;
          }));
    }

    assertThat(touches.get(), is((int)Animals.ANIMALS.stream().filter((a) -> a.getTaxonomicClass().equals("mammal")).count()));
    assertFalse(updated.get());
    assertTrue(timedOut.get());
  }

  /**
   * Ensures a not-yet-scanned indexed {@code Record} which is <i>deleted</i> is unseen by the pipeline.
   */
  @Test
  public void testDeletedRecordIndexVisibility() throws Exception {
    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");

    String targetMammal = chooseIndexedRecord(TAXONOMIC_CLASS, mammalFilter, 4);

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicBoolean targetMammalSeen = new AtomicBoolean(false);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> mammals = stream.filter(mammalFilter)
          .peek(r -> {
            if (r.getKey().equals(targetMammal)) {
              targetMammalSeen.set(true);
            }
            if (!altered.get()) {
              dataset.delete(Durability.IMMEDIATE, targetMammal);
              altered.set(true);
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertFalse(targetMammalSeen.get());
      assertTrue(mammals.stream().allMatch(animal -> animal.getTaxonomicClass().equals("mammal")));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }

    assertThat(dataset.get(targetMammal), is(nullValue()));
  }

  /**
   * Ensures a not-yet-scanned indexed {@code Record} for which the indexed value is <i>removed</i> is unseen
   * by the pipeline.
   */
  @Test
  public void testUpdatedRecordIndexVisibilityRemoved() throws Exception {
    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");

    String targetMammal = chooseIndexedRecord(TAXONOMIC_CLASS, mammalFilter, 4);

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicBoolean targetMammalSeen = new AtomicBoolean(false);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> mammals = stream.filter(mammalFilter)
          .peek(r -> {
            if (r.getKey().equals(targetMammal)) {
              targetMammalSeen.set(true);
            }
            if (!altered.get()) {
              dataset.applyMutation(Durability.IMMEDIATE, targetMammal, (z) -> true, (z) -> {
                CellSet cells = new CellSet(z);
                cells.get(TAXONOMIC_CLASS).ifPresent(v -> cells.remove(TAXONOMIC_CLASS.newCell(v)));
                return cells;
              });
              altered.set(true);
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertFalse(targetMammalSeen.get());
      assertTrue(mammals.stream().allMatch(animal -> animal.getTaxonomicClass().equals("mammal")));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      List<Record<String>> invertebrates = stream.filter(TAXONOMIC_CLASS.value().isGreaterThan("")).collect(toList());
      assertFalse(invertebrates.stream().anyMatch(r -> r.getKey().equals(targetMammal)));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }
  }

  /**
   * Ensures a not-yet-scanned indexed {@code Record} for which the indexed value is altered to fall <i>outside</i>
   * of the selection filter is unseen by the pipeline.
   */
  @Test
  public void testUpdatedRecordIndexVisibilityRelocatedOut() throws Exception {
    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");

    String targetMammal = chooseIndexedRecord(TAXONOMIC_CLASS, mammalFilter, 4);

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicBoolean targetMammalSeen = new AtomicBoolean(false);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> mammals = stream.filter(mammalFilter)
          .peek(r -> {
            if (r.getKey().equals(targetMammal)) {
              targetMammalSeen.set(true);
            }
            if (!altered.get()) {
              dataset.applyMutation(Durability.IMMEDIATE, targetMammal, (z) -> true, (z) -> {
                CellSet cells = new CellSet(z);
                cells.set(TAXONOMIC_CLASS.newCell("invertebrate"));
                return cells;
              });
              altered.set(true);
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertFalse(targetMammalSeen.get());
      assertTrue(mammals.stream().allMatch(animal -> animal.getTaxonomicClass().equals("mammal")));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      List<Record<String>> invertebrates = stream.filter(TAXONOMIC_CLASS.value().is("invertebrate")).collect(toList());
      assertTrue(invertebrates.stream().anyMatch(r -> r.getKey().equals(targetMammal)));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }
  }

  /**
   * Ensures a not-yet-scanned indexed {@code Record} (originally outside of and <i>before</i> the filter range)
   * is unseen by the pipeline when the indexed value is altered to fall <i>into</i> the filter range.
   */
  @Test
  public void testUpdatedRecordIndexVisibilityRelocatedInFromBefore() throws Exception {
    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");

    String targetAnimal = chooseIndexedRecord(TAXONOMIC_CLASS, TAXONOMIC_CLASS.value().is("bird"), 0);

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicBoolean targetAnimalSeen = new AtomicBoolean(false);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> mammals = stream.filter(mammalFilter)
          .peek(r -> {
            if (r.getKey().equals(targetAnimal)) {
              targetAnimalSeen.set(true);
            }
            if (!altered.get()) {
              dataset.applyMutation(Durability.IMMEDIATE, targetAnimal, (z) -> true, (z) -> {
                CellSet cells = new CellSet(z);
                cells.set(TAXONOMIC_CLASS.newCell("mammal"));
                return cells;
              });
              altered.set(true);
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertFalse(targetAnimalSeen.get());
      assertTrue(mammals.stream().allMatch(animal -> animal.getTaxonomicClass().equals("mammal")));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }
  }

  /**
   * Ensures a not-yet-scanned indexed {@code Record} (originally outside of and <i>after</i> the filter range)
   * is unseen by the pipeline when the indexed value is altered to fall <i>into</i> the filter range.
   */
  @Test
  public void testUpdatedRecordIndexVisibilityRelocatedInFromAfter() throws Exception {
    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");

    String targetAnimal = chooseIndexedRecord(TAXONOMIC_CLASS, TAXONOMIC_CLASS.value().is("reptile"), 0);

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicBoolean targetAnimalSeen = new AtomicBoolean(false);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> mammals = stream.filter(mammalFilter)
          .peek(r -> {
            if (r.getKey().equals(targetAnimal)) {
              targetAnimalSeen.set(true);
            }
            if (!altered.get()) {
              dataset.applyMutation(Durability.IMMEDIATE, targetAnimal, (z) -> true, (z) -> {
                CellSet cells = new CellSet(z);
                cells.set(TAXONOMIC_CLASS.newCell("mammal"));
                return cells;
              });
              altered.set(true);
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertTrue(targetAnimalSeen.get());
      assertTrue(mammals.stream().allMatch(animal -> animal.getTaxonomicClass().equals("mammal")));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
    }
  }

  /**
   * Ensures a not-yet-scanned indexed {@code Record} for which the indexed value is altered to fall <i>later</i>
   * in the selection filter is visited properly.
   */
  @Ignore("Fails: Updated record is visited out of order -- **bad presumption**, retained to document behavior")
  @Test
  public void testPreUpdatedRecordIndexVisibilityRelocatedAgain() throws Exception {
    Predicate<Record<?>> classFilter = TAXONOMIC_CLASS.value().isGreaterThan("");

    String targetAnimal = chooseIndexedRecord(TAXONOMIC_CLASS, classFilter, 4);

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicInteger targetAnimalVisitCount = new AtomicInteger(0);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> animals = stream.filter(classFilter)
          .peek(r -> {
            if (!altered.get()) {
              dataset.applyMutation(Durability.IMMEDIATE, targetAnimal, (z) -> true, (z) -> {
                CellSet cells = new CellSet(z);
                cells.set(TAXONOMIC_CLASS.newCell("xiphosura"));
                return cells;
              });
              altered.set(true);
            }
            if (r.getKey().equals(targetAnimal)) {
              targetAnimalVisitCount.getAndIncrement();
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertThat(targetAnimalVisitCount.get(), is(1));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
      assertOrder(animals, Comparator.comparing(Animal::getTaxonomicClass));
    }
  }

  /**
   * Ensures an already-visited indexed {@code Record} for which the indexed value is altered to fall <i>later</i>
   * in the selection filter is not visited again by the pipeline.
   */
  @Test
  public void testPostUpdatedRecordIndexVisibilityRelocatedAgain() throws Exception {
    Predicate<Record<?>> classFilter = TAXONOMIC_CLASS.value().isGreaterThan("");
    String targetAnimal = chooseIndexedRecord(TAXONOMIC_CLASS, classFilter, 4);

    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicInteger targetAnimalVisitCount = new AtomicInteger(0);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> animals = stream.filter(classFilter)
          .peek(r -> {
            if (r.getKey().equals(targetAnimal)) {
              targetAnimalVisitCount.getAndIncrement();
              if (!altered.get()) {
                dataset.applyMutation(Durability.IMMEDIATE, targetAnimal, (z) -> true, (z) -> {
                  CellSet cells = new CellSet(z);
                  cells.set(TAXONOMIC_CLASS.newCell("xiphosura"));
                  return cells;
                });
                altered.set(true);
              }
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertThat(targetAnimalVisitCount.get(), is(1));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
      assertOrder(animals, Comparator.comparing(Animal::getTaxonomicClass));
    }
  }

  /**
   * Ensures a new {@code Record} (added in a new slot) is not visited by the pipeline.
   */
  @Test
  public void testNewRecordIndexVisibility() throws Exception {
    Predicate<Record<?>> classFilter = TAXONOMIC_CLASS.value().isGreaterThan("");

    String crabKey = "horseshoe crab";
    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicInteger horseshoeCrabVisitCount = new AtomicInteger(0);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> animals = stream.filter(classFilter)
          .peek(r -> {
            if (!altered.get()) {
              dataset.add(Durability.IMMEDIATE, crabKey,
                  TAXONOMIC_CLASS.newCell("xiphosura"),
                  STATUS.newCell(NEAR_THREATENED),
                  OBSERVATIONS.newCell(5L)
              );
              altered.set(true);
            }
            if (r.getKey().equals(crabKey)) {
              horseshoeCrabVisitCount.getAndIncrement();
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertThat(horseshoeCrabVisitCount.get(), is(0));
      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
      assertOrder(animals, Comparator.comparing(Animal::getTaxonomicClass));
    }
  }

  /**
   * Ensures a record added (at the same slot) after a deletion <i>before</i> the slot/record is visited is properly
   * observed by the pipeline.
   * <p>
   * This test requires that the dataset concurrency level is 1 to enable reuse of a storage slot.
   */
  @Ignore("Fails: Added record is visited out of order -- **bad presumption**, retained to document behavior")
  @Test
  public void testPreReplacementRecordIndexVisibility() throws Exception {
    assertThat(((SovereignDatasetImpl)dataset).getConfig().getConcurrency(), is(1));

    Predicate<Record<?>> classFilter = TAXONOMIC_CLASS.value().isGreaterThan("");

    String targetAnimal = chooseIndexedRecord(TAXONOMIC_CLASS, classFilter, 4);
    long targetIndex = getRecordIndex(targetAnimal);

    String crabKey = "horseshoe crab";
    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicInteger targetAnimalVisitCount = new AtomicInteger(0);
      AtomicInteger horseshoeCrabVisitCount = new AtomicInteger(0);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> animals = stream.filter(classFilter)
          .peek(r -> {
            if (r.getKey().equals(targetAnimal)) {
              targetAnimalVisitCount.getAndIncrement();
            } else if (r.getKey().equals(crabKey)) {
              horseshoeCrabVisitCount.getAndIncrement();
            }
            if (!altered.get()) {
              dataset.delete(Durability.IMMEDIATE, targetAnimal);
              dataset.add(Durability.IMMEDIATE, crabKey,
                  TAXONOMIC_CLASS.newCell("xiphosura"),
                  STATUS.newCell(NEAR_THREATENED),
                  OBSERVATIONS.newCell(5L)
              );
              altered.set(true);
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
      assertThat(getRecordIndex(crabKey), is(equalTo(targetIndex)));
      assertThat(targetAnimalVisitCount.get(), is(0));
      assertThat(horseshoeCrabVisitCount.get(), is(1));
      assertOrder(animals, Comparator.comparing(Animal::getTaxonomicClass));
    }
  }

  /**
   * Ensures a record, not fitting the filter criteria, added (at the same slot) after a deletion <i>before</i> the
   * slot/record is not visited by the pipeline.
   * <p>
   * This test requires that the dataset concurrency level is 1 to enable reuse of a storage slot.
   */
  @Test
  public void testPreReplacementRecordIndexVisibilityOutOfIndex() throws Exception {
    assertThat(((SovereignDatasetImpl)dataset).getConfig().getConcurrency(), is(1));

    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");

    String targetMammal = chooseIndexedRecord(TAXONOMIC_CLASS, mammalFilter, 4);
    long targetIndex = getRecordIndex(targetMammal);

    String crabKey = "horseshoe crab";
    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicInteger targetAnimalVisitCount = new AtomicInteger(0);
      AtomicInteger horseshoeCrabVisitCount = new AtomicInteger(0);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> animals = stream.filter(mammalFilter)
          .peek(r -> {
            if (r.getKey().equals(targetMammal)) {
              targetAnimalVisitCount.getAndIncrement();
            } else if (r.getKey().equals(crabKey)) {
              horseshoeCrabVisitCount.getAndIncrement();
            }
            if (!altered.get()) {
              dataset.delete(Durability.IMMEDIATE, targetMammal);
              dataset.add(Durability.IMMEDIATE, crabKey,
                  TAXONOMIC_CLASS.newCell("xiphosura"),
                  STATUS.newCell(NEAR_THREATENED),
                  OBSERVATIONS.newCell(5L)
              );
              altered.set(true);
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
      assertThat(getRecordIndex(crabKey), is(equalTo(targetIndex)));
      assertThat(targetAnimalVisitCount.get(), is(0));
      assertThat(horseshoeCrabVisitCount.get(), is(0));
      assertOrder(animals, Comparator.comparing(Animal::getTaxonomicClass));
    }
  }

  /**
   * Ensures a record added (at the same slot) after a deletion <i>after</i> the slot/record is visited is properly
   * observed by the pipeline.
   * <p>
   * This test requires that the dataset concurrency level is 1 to enable reuse of a storage slot.
   */
  @Test
  public void testPostReplacementRecordIndexVisibility() throws Exception {
    assertThat(((SovereignDatasetImpl)dataset).getConfig().getConcurrency(), is(1));

    Predicate<Record<?>> classFilter = TAXONOMIC_CLASS.value().isGreaterThan("");

    String targetAnimal = chooseIndexedRecord(TAXONOMIC_CLASS, classFilter, 4);
    long targetIndex = getRecordIndex(targetAnimal);

    String crabKey = "horseshoe crab";
    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      AtomicInteger targetAnimalVisitCount = new AtomicInteger(0);
      AtomicInteger horseshoeCrabVisitCount = new AtomicInteger(0);
      AtomicBoolean altered = new AtomicBoolean(false);
      List<Animal> animals = stream.filter(classFilter)
          .peek(r -> {
            if (r.getKey().equals(targetAnimal)) {
              targetAnimalVisitCount.getAndIncrement();
              if (!altered.get()) {
                dataset.delete(Durability.IMMEDIATE, targetAnimal);
                dataset.add(Durability.IMMEDIATE, crabKey,
                    TAXONOMIC_CLASS.newCell("xiphosura"),
                    STATUS.newCell(NEAR_THREATENED),
                    OBSERVATIONS.newCell(5L)
                );
                altered.set(true);
              }
            } else if (r.getKey().equals(crabKey)) {
              horseshoeCrabVisitCount.getAndIncrement();
            }
          })
          .map(Animal::new)
          .collect(toList());

      assertThat(assignedIndexCell.get(), is(TAXONOMIC_CLASS));
      assertThat(getRecordIndex(crabKey), is(equalTo(targetIndex)));
      assertThat(targetAnimalVisitCount.get(), is(1));
      assertThat(horseshoeCrabVisitCount.get(), is(0));
      assertOrder(animals, Comparator.comparing(Animal::getTaxonomicClass));
    }
  }

  /**
   * Asserts that the collection elements are ordered according to the {@link Comparator} provided.
   * @param collection the {@code Collection} to check
   * @param elementComparator the {@code Comparator} for the element pairs
   */
  private <T> void assertOrder(Collection<T> collection, Comparator<T> elementComparator) {
    T prev = null;
    for (T element : collection) {
      if (prev != null && elementComparator.compare(prev, element) > 0) {
        throw new AssertionError("Elements visited out of order:\n\t[prev]=" + prev + "\n\t[next]=" + element);
      }
      prev = element;
    }
  }

  /**
   * Gets the {@link PersistentMemoryLocator} index for the record with the key provided.
   * @param key identifies the record to obtain
   * @return the {@code PersistentMemoryLocator} index value for the record; -1 if the record is not found
   */
  private long getRecordIndex(String key) {
    PersistentRecord<?, ?> persistentRecord = (PersistentRecord<?, ?>)dataset.get(key);
    return (persistentRecord == null ? -1 : ((PersistentMemoryLocator)persistentRecord.getLocation()).index());
  }

  /**
   * Returns the key of the {@code skipCount} + 1 record from the test dataset using the index-based filter provided.
   * @param expectedIndex the {@code CellDefinition} of the index expected to be selected for {@code indexFilter}
   * @param indexFilter the {@code Predicate} used to designate the index to use
   * @param skipCount the number of records to skip before choosing the key
   * @return the key of the chosen record
   */
  private String chooseIndexedRecord(CellDefinition<?> expectedIndex, Predicate<Record<?>> indexFilter, int skipCount) {
    String chosenKey;
    try (RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
      SpliteratorUtility.setStreamInfoAction(this.dataset, stream, assignedIndexCell, null);

      chosenKey = stream.filter(indexFilter)
          .skip(skipCount)
          .findFirst()
          .orElseThrow(() -> new AssertionError("Failed to select a record"))
          .getKey();

      assertThat(assignedIndexCell.get(), is(expectedIndex));
    }
    return chosenKey;
  }

  /**
   * Checks the indicated {@code PipelineOperation} to ensure it is a {@code ManagedAction} insertion
   * point and that the {@code ManagedAction} is set or not (as indicated).
   *
   * @param pipelineOperations the {@code List} of {@code PipelineOperation}s to examine
   * @param index the index of the {@code PipelineOperation} in {@code pipelineOperations} to examine
   * @param managedAction a reference to the {@code ManagedAction} instance to check
   * @param isSet if {@code true}, the meta data for the {@code PipelineOperation} must be a
   *              {@code ManagedActionSupplier} that returns {@code managedAction}; if {@code false},
   *              the {@code ManagedActionSupplier} must <b>not</b> return {@code managedAction}
   * @param <K> the key type for {@code managedAction}
   */
  private <K extends Comparable<K>> void checkManagedActionSupplier(
    final List<PipelineOperation> pipelineOperations, final int index,
    final ManagedAction<K> managedAction, final boolean isSet) {
    final PipelineOperation.OperationMetaData operationMetaData = pipelineOperations.get(index).getOperationMetaData();
    assertThat(operationMetaData, is(instanceOf(MANAGED_ACTION_SUPPLIER_CLASS)));
    if (isSet) {
      assertThat(((Supplier)operationMetaData).get(), is(sameInstance(managedAction)));
    } else {
      assertThat(((Supplier)operationMetaData).get(), is(not(sameInstance(managedAction))));
    }
  }

  /**
   * Replaces the {@link LocatorGenerator} used by a {@code RecordStream} with a Mockito spy-wrapped version.
   *
   * @param stream the {@code RecordStream} to modify
   * @param <K> the key type of the {@code SovereignDataset} emitting {@code stream}
   *
   * @return the spy-wrapped {@code LocatorGenerator}
   */
  private <K extends Comparable<K>>
  LocatorGenerator<K> setLocatorGeneratorSpy(final RecordStreamImpl<K> stream) {
    try {
      final Field generatorField = RecordStreamImpl.class.getDeclaredField("generator");
      generatorField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final LocatorGenerator<K> spyGenerator = spy((LocatorGenerator<K>)generatorField.get(stream));
      generatorField.set(stream, spyGenerator);
      return spyGenerator;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new AssertionError("Unable to set spy(RecordComputeStream.generator)", e);
    }
  }

  /**
   * Provides a test holding the common checks made for most {@code Stream} operations.
   *
   * @param operation the {@code Operation} enum constant identifying the {@code Stream} operation being tested
   * @param operationTest a {@code Consumer} providing the testing unique to the {@code Stream} operation
   * @param isManagedActionInsertionPoint indicates whether or not {@code operation} should be marked as an
   *                                      insertion point for a {@link ManagedAction}
   * @param operationArguments the arguments to the operation being test that must be retained in
   *                           {@code PipelineMetaData}
   */
  private void testOperation(final Operation operation,
                             final Consumer<RecordStreamImpl<String>> operationTest,
                             final boolean isManagedActionInsertionPoint,
                             final Object... operationArguments) {


    try (final RecordStreamImpl<String> stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      final AtomicBoolean closed = new AtomicBoolean();
      stream.onClose(() -> closed.set(true));

      operationTest.accept(stream);

      assertThat(terminalAction.get(), is(operation.isTerminal()));
      assertThat(closed.get(), is(operation.isTerminal() && WrappedStream.SELF_CLOSE_DEFAULT));

      final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
      assertThat(pipeline.size(), is(1));

      final PipelineOperation pipelineOperation = pipeline.get(0);
      assertThat(pipelineOperation.getOperation(), is(operation));

      final PipelineOperation.OperationMetaData operationMetaData = pipelineOperation.getOperationMetaData();
      if (isManagedActionInsertionPoint) {
        assertThat(operationMetaData, is(instanceOf(MANAGED_ACTION_SUPPLIER_CLASS)));
      } else {
        assertThat(operationMetaData, is(nullValue()));
      }

      final List<Object> capturedArguments = pipelineOperation.getArguments();
      assertThat(capturedArguments.size(), is(equalTo(operationArguments.length)));
      if (operationArguments.length != 0) {
        for (int i = 0; i < operationArguments.length; i++) {
          assertThat(capturedArguments.get(i), is(sameInstance(operationArguments[i])));
        }
      }
    }
  }

  /**
   * A {@link ManagedAction}/{@link Function} implementation that counts use of the
   * {@code begin}, {@code apply}, and {@code end} methods.
   */
  @SuppressWarnings("try")
  private static final class ProxyManagedAction<K extends Comparable<K>, M extends ManagedAction<K> & Function<Record<K>, Record<K>> & AutoCloseable>
      implements ManagedAction<K>, Function<Record<K>, Record<K>>, AutoCloseable {
    private final M delegate;

    private long beginCount = 0;
    private long applyCount = 0;
    private long endCount = 0;

    /**
     * Create a new {@code ProxyManagedAction} wrapping an existing {@link ManagedAction}.
     * @param delegate the {@code ManagedAction & Function} to wrap
     */
    private ProxyManagedAction(M delegate) {
      this.delegate = delegate;
    }

    @Override
    public InternalRecord<K> begin(InternalRecord<K> record) {
      beginCount++;
      return delegate.begin(record);
    }

    @Override
    public void end(InternalRecord<K> record) {
      delegate.end(record);
      endCount++;
    }

    @Override
    public SovereignContainer<K> getContainer() {
      return delegate.getContainer();
    }

    @Override
    public Record<K> apply(Record<K> record) {
      applyCount++;
      return delegate.apply(record);
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }

    public long getBeginCount() {
      return beginCount;
    }

    public long getApplyCount() {
      return applyCount;
    }

    public long getEndCount() {
      return endCount;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ProxyManagedAction{");
      sb.append("beginCount=").append(beginCount);
      sb.append(", applyCount=").append(applyCount);
      sb.append(", endCount=").append(endCount);
      sb.append('}');
      return sb.toString();
    }
  }

  @SuppressWarnings("try")
  private interface MockAction
      extends ManagedAction<String>, Consumer<Record<String>>, AutoCloseable {
  }
}
