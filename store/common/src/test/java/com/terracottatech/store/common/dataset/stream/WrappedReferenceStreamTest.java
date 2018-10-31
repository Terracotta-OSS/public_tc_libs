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

package com.terracottatech.store.common.dataset.stream;

import org.junit.Before;
import org.junit.Test;

import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.Operation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicMarkableReference;
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
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
@SuppressWarnings({ "PointlessBooleanExpression", "ConstantConditions" })
public class WrappedReferenceStreamTest {

  private static final int ITEM_COUNT = 50;

  private Stream<String> wrappedStream;

  @Before
  public void setUp() throws Exception {
    this.wrappedStream = IntStream.rangeClosed(1, ITEM_COUNT).mapToObj(Integer::toString);
  }

  private WrappedReferenceStream<String> getTestStream() {
    return new WrappedReferenceStream<>(this.wrappedStream);
  }

  @Test
  public void testInstantiation() throws Exception {
    final WrappedReferenceStream<String> stream = getTestStream();
    final PipelineMetaData metaData = stream.getMetaData();
    assertNotNull(metaData);

    assertThat(metaData.getHeadStream(), is(sameInstance(this.wrappedStream)));
    assertThat(metaData.isSelfClosing(), is(WrappedStream.SELF_CLOSE_DEFAULT));
    assertThat(metaData.getPipeline(), is(empty()));

    // Ensure original pipeline consumer is the "self-eliminating" default
    final Consumer<PipelineMetaData> pipelineConsumer = metaData.getPipelineConsumer();
    assertThat(pipelineConsumer, is(notNullValue()));
    assertThat(pipelineConsumer.andThen(pipelineConsumer), is(sameInstance(pipelineConsumer)));

    assertThat(stream.getNativeStream(), is(sameInstance(this.wrappedStream)));
    assertThat(stream.getPipeline(), is(empty()));
    assertThat(stream.getTerminalAction(), is(sameInstance(pipelineConsumer)));
  }

  @Test
  public void testStreamBifurcation() throws Exception {
    final WrappedReferenceStream<String> streamHead = getTestStream();
    streamHead.filter(s -> s.endsWith("4"));
    try {
      streamHead.filter(s -> s.endsWith("1"));
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testReuse() throws Exception {
    final WrappedReferenceStream<String> streamHead = getTestStream();
    streamHead.count();
    try {
      streamHead.findAny();
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testTerminalAction() throws Exception {
    final WrappedReferenceStream<String> stream = getTestStream();

    final AtomicMarkableReference<PipelineMetaData> passedMetaData = new AtomicMarkableReference<>(null, false);
    final Consumer<PipelineMetaData> firstAction = metaData -> passedMetaData.set(metaData, false);
    stream.appendTerminalAction(firstAction);
    assertThat(stream.getTerminalAction(), is(sameInstance(firstAction)));
    assertThat(stream.getMetaData().getPipelineConsumer(), is(sameInstance(firstAction)));
    assertThat(passedMetaData.getReference(), is(nullValue()));

    final Consumer<PipelineMetaData> secondAction = metaData -> passedMetaData.attemptMark(metaData, true);
    stream.appendTerminalAction(secondAction);

    assertThat(stream.count(), is((long) ITEM_COUNT));

    final boolean[] mark = new boolean[1];
    assertThat(passedMetaData.get(mark), is(sameInstance(stream.getMetaData())));
    assertThat(mark[0], is(true));
  }

  @Test
  public void testAllMatch() throws Exception {
    final Predicate<String> predicate = s -> s.endsWith("4");
    testOperation(TerminalOperation.ALL_MATCH, stream -> assertFalse(stream.allMatch(predicate)), predicate);
  }

  @Test
  public void testAnyMatch() throws Exception {
    final Predicate<String> predicate = s -> s.endsWith("4");
    testOperation(TerminalOperation.ANY_MATCH, stream -> assertTrue(stream.anyMatch(predicate)), predicate);
  }

  @Test
  public void testCollect1Arg() throws Exception {
    final Collector<String, ?, List<String>> collector = toList();
    testOperation(TerminalOperation.COLLECT_1,
        stream -> assertThat(stream.collect(collector), is(iterableWithSize(ITEM_COUNT))),
        collector);
  }

  @Test
  public void testCollect3Arg() throws Exception {
    final Supplier<ArrayList<Object>> supplier = ArrayList::new;
    final BiConsumer<ArrayList<Object>, String> accumulator = ArrayList::add;
    final BiConsumer<ArrayList<Object>, ArrayList<Object>> combiner = ArrayList::addAll;
    testOperation(TerminalOperation.COLLECT_3,
        stream -> assertThat(stream.collect(supplier, accumulator, combiner), is(iterableWithSize(ITEM_COUNT))),
        supplier, accumulator, combiner);
  }

  @Test
  public void testCount() throws Exception {
    testOperation(TerminalOperation.COUNT, stream -> assertThat(stream.count(), is((long)ITEM_COUNT)));
  }

  @Test
  public void testDistinct() throws Exception {
    testOperation(IntermediateOperation.DISTINCT, stream -> {
      final Stream<String> nextStream = stream.distinct();
      assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
    });
  }

  @Test
  public void testFilter() throws Exception {
    final Predicate<String> filterPredicate = s -> s.endsWith("4");
    testOperation(IntermediateOperation.FILTER,
        stream -> {
          final Stream<String> nextStream = stream.filter(filterPredicate);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        filterPredicate);
  }

  @Test
  public void testFindAny() throws Exception {
    testOperation(TerminalOperation.FIND_ANY, stream -> assertThat(stream.findAny().isPresent(), is(true)));
  }

  @Test
  public void testFindFirst() throws Exception {
    testOperation(TerminalOperation.FIND_FIRST, stream -> assertThat(stream.findFirst().isPresent(), is(true)));
  }

  @Test
  public void testFlatMap() throws Exception {
    final Function<String, Stream<? extends Character>> mapper = s -> s.chars().mapToObj(c -> (char)c);
    testOperation(IntermediateOperation.FLAT_MAP,
        stream -> {
          final Stream<Character> nextStream = stream.flatMap(mapper);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        mapper);
  }

  @Test
  public void testFlatMapToDouble() throws Exception {
    final Function<String, DoubleStream> mapper = s -> s.chars().asDoubleStream();
    testOperation(IntermediateOperation.FLAT_MAP_TO_DOUBLE,
        stream -> {
          final DoubleStream nextStream = stream.flatMapToDouble(mapper);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        mapper);
  }

  @Test
  public void testFlatMapToInt() throws Exception {
    final Function<String, IntStream> mapper = CharSequence::chars;
    testOperation(IntermediateOperation.FLAT_MAP_TO_INT,
        stream -> {
          final IntStream nextStream = stream.flatMapToInt(mapper);
          assertThat(nextStream, is(instanceOf(WrappedIntStream.class)));
        },
        mapper);
  }

  @Test
  public void testFlatMapToLong() throws Exception {
    final Function<String, LongStream> mapper = s -> s.chars().asLongStream();
    testOperation(IntermediateOperation.FLAT_MAP_TO_LONG,
        stream -> {
          final LongStream nextStream = stream.flatMapToLong(mapper);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        mapper);
  }

  @Test
  public void testForEach() throws Exception {
    final Consumer<String> consumer = s -> { };
    testOperation(TerminalOperation.FOR_EACH, stream -> stream.forEach(consumer), consumer);
  }

  @Test
  public void testForEachOrdered() throws Exception {
    final Consumer<String> consumer = s -> { };
    testOperation(TerminalOperation.FOR_EACH_ORDERED, stream -> stream.forEachOrdered(consumer), consumer);
  }

  @Test
  public void testLimit() throws Exception {
    final long maxSize = 5;
    testOperation(IntermediateOperation.LIMIT,
        stream -> {
          final Stream<String> nextStream = stream.limit(maxSize);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        maxSize);
  }

  @Test
  public void testMap() throws Exception {
    final Function<String, String> mapper = s -> s.substring(1, Math.max(1, s.length() - 1));
    testOperation(IntermediateOperation.MAP,
        stream -> {
          final Stream<String> nextStream = stream.map(mapper);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToDouble() throws Exception {
    final ToDoubleFunction<String> mapper = s -> Integer.valueOf(s.length()).doubleValue();
    testOperation(IntermediateOperation.MAP_TO_DOUBLE,
        stream -> {
          final DoubleStream nextStream = stream.mapToDouble(mapper);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToInt() throws Exception {
    final ToIntFunction<String> mapper = String::length;
    testOperation(IntermediateOperation.MAP_TO_INT,
        stream -> {
          final IntStream nextStream = stream.mapToInt(mapper);
          assertThat(nextStream, is(instanceOf(WrappedIntStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToLong() throws Exception {
    final ToLongFunction<String> mapper = s -> (long)s.length();
    testOperation(IntermediateOperation.MAP_TO_LONG,
        stream -> {
          final LongStream nextStream = stream.mapToLong(mapper);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        mapper);
  }

  @Test
  public void testMax() throws Exception {
    final Comparator<String> comparator = (s1, s2) -> Integer.valueOf(s1).compareTo(Integer.valueOf(s2));
    testOperation(TerminalOperation.MAX_1,
        stream -> assertThat(stream.max(comparator).get(), is(Integer.toString(ITEM_COUNT))),
        comparator);
  }

  @Test
  public void testMin() throws Exception {
    final Comparator<String> comparator = (s1, s2) -> Integer.valueOf(s1).compareTo(Integer.valueOf(s2));
    testOperation(TerminalOperation.MIN_1,
        stream -> assertThat(stream.min(comparator).get(), is(Integer.toString(1))),
        comparator);
  }

  @Test
  public void testNoneMatch() throws Exception {
    final Predicate<String> predicate = s -> s.endsWith("4");
    testOperation(TerminalOperation.NONE_MATCH, stream -> assertFalse(stream.noneMatch(predicate)), predicate);
  }

  @Test
  public void testPeek() throws Exception {
    final Consumer<String> consumer = s -> { };
    testOperation(IntermediateOperation.PEEK,
        stream -> {
          final Stream<String> nextStream = stream.peek(consumer);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        consumer);
  }

  @Test
  public void testReduce1Arg() throws Exception {
    final BinaryOperator<String> accumulator = (s1, s2) -> s2;
    testOperation(TerminalOperation.REDUCE_1,
        stream -> assertThat(stream.reduce(accumulator).get(), is(equalTo(Integer.toString(ITEM_COUNT)))),
        accumulator);
  }

  @Test
  public void testReduce2Arg() throws Exception {
    final String identity = "";
    final BinaryOperator<String> accumulator = (s1, s2) -> s2;
    testOperation(TerminalOperation.REDUCE_2,
        stream -> assertThat(stream.reduce(identity, accumulator), is(equalTo(Integer.toString(ITEM_COUNT)))),
        identity, accumulator);
  }

  @Test
  public void testReduce3Arg() throws Exception {
    final String identity = "";
    final BiFunction<String, String, String> accumulator = (s1, s2) -> s2;
    final BinaryOperator<String> combiner = (p1, p2) -> p2;
    testOperation(TerminalOperation.REDUCE_3,
        stream -> assertThat(stream.reduce(identity, accumulator, combiner), is(equalTo(Integer.toString(ITEM_COUNT)))),
        identity, accumulator, combiner);
  }

  @Test
  public void testSkip() throws Exception {
    final long n = 1L;
    testOperation(IntermediateOperation.SKIP,
        stream -> {
          final Stream<String> nextStream = stream.skip(n);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        n);
  }

  @Test
  public void testSorted0Arg() throws Exception {
    testOperation(IntermediateOperation.SORTED_0, stream -> {
      final Stream<String> nextStream = stream.sorted();
      assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
    });
  }

  @Test
  public void testSorted1Arg() throws Exception {
    final Comparator<String> comparator = Comparator.<String>reverseOrder();
    testOperation(IntermediateOperation.SORTED_1,
        stream -> {
          final Stream<String> nextStream = stream.sorted(comparator);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        comparator);
  }

  @Test
  public void testToArray0Arg() throws Exception {
    testOperation(TerminalOperation.TO_ARRAY_0, stream -> {
      final Object[] array = stream.toArray();
      assertThat(array, is(arrayWithSize(ITEM_COUNT)));
      assertThat(array[0], is(instanceOf(String.class)));
    });
  }

  @Test
  public void testToArray1Arg() throws Exception {
    final IntFunction<String[]> generator = String[]::new;
    testOperation(TerminalOperation.TO_ARRAY_1,
        stream -> {
          final String[] array = stream.toArray(generator);
          assertThat(array, is(arrayWithSize(ITEM_COUNT)));
          assertThat(array[0], is(instanceOf(String.class)));
        },
        generator);
  }

  @Test
  public void testIterator() throws Exception {
    final AtomicBoolean closed = new AtomicBoolean();
    try (final WrappedReferenceStream<String> stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      stream.onClose(() -> closed.set(true));

      final Iterator<String> streamIterator = stream.iterator();

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
    assertThat(closed.get(), is(true));
  }

  @SuppressWarnings("UnusedAssignment")
  @Test(timeout = 30000L)
  public void testIteratorAutoClose() throws Exception {
    final ReferenceQueue<WrappedReferenceStream<String>> queue = new ReferenceQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();
    WrappedReferenceStream<String> stream = getTestStream();
    Iterator<String> iterator = stream
        .selfClose(true)
        .onClose(() -> closed.set(true))
        .iterator();
    final PhantomReference<WrappedReferenceStream<String>> ref = new PhantomReference<>(stream, queue);
    this.wrappedStream = null;
    stream = null;

    assertNotNull(iterator.next());
    assertThat(closed.get(), is(false));

    iterator = null;   // unusedAssignment - explicit null of reference for garbage collection

    /*
     * Await finalization of the iterator and stream.
     */
    Reference<? extends WrappedReferenceStream<String>> queuedRef;
    while ((queuedRef = queue.poll()) == null) {
      System.gc();
      Thread.sleep(100L);
    }
    assertThat(queuedRef, is(sameInstance(ref)));
    queuedRef.clear();

    assertThat(closed.get(), is(true));
  }

  @Test
  public void testOnClose() throws Exception {
    final WrappedReferenceStream<String> stream = getTestStream();
    final AtomicBoolean terminalAction = new AtomicBoolean();
    stream.appendTerminalAction(metaData -> terminalAction.set(true));

    final AtomicBoolean closed = new AtomicBoolean();
    stream.onClose(() -> closed.set(true));

    final Stream<String> nextStream = stream.onClose(() -> { });
    assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
    assertThat(terminalAction.get(), is(false));
    assertThat(closed.get(), is(false));

    final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
    assertThat(pipeline.size(), is(0));
  }

  @Test
  public void testParallel() throws Exception {
    testOperation(IntermediateOperation.PARALLEL, stream -> {
      final Stream<String> nextStream = stream.parallel();
      assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
    });
  }

  @Test
  public void testSelfClose() throws Exception {
    testOperation(IntermediateOperation.SELF_CLOSE, stream -> {
      final Stream<String> nextStream = stream.selfClose(!WrappedStream.SELF_CLOSE_DEFAULT);
      assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
    });
  }

  @Test
  public void testSequential() throws Exception {
    testOperation(IntermediateOperation.SEQUENTIAL, stream -> {
      final Stream<String> nextStream = stream.sequential();
      assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
    });
  }

  @Test
  public void testSpliterator() throws Exception {
    final AtomicBoolean closed = new AtomicBoolean();
    try (final WrappedReferenceStream<String> stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      stream.onClose(() -> closed.set(true));

      final Spliterator<String> spliterator = stream.spliterator();

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
    assertThat(closed.get(), is(true));
  }

  @SuppressWarnings("UnusedAssignment")
  @Test(timeout = 30000L)
  public void testSpliteratorAutoClose() throws Exception {
    final ReferenceQueue<WrappedReferenceStream<String>> queue = new ReferenceQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();
    WrappedReferenceStream<String> stream = getTestStream();
    Spliterator<String> spliterator = stream
        .selfClose(true)
        .onClose(() -> closed.set(true))
        .spliterator();
    final PhantomReference<WrappedReferenceStream<String>> ref = new PhantomReference<>(stream, queue);
    this.wrappedStream = null;
    stream = null;

    assertTrue(spliterator.tryAdvance(s -> { }));
    assertThat(closed.get(), is(false));

    spliterator = null;   // unusedAssignment - explicit null of reference for garbage collection

    /*
     * Await finalization of the spliterator and stream.
     */
    Reference<? extends WrappedReferenceStream<String>> queuedRef;
    while ((queuedRef = queue.poll()) == null) {
      System.gc();
      Thread.sleep(100L);
    }
    assertThat(queuedRef, is(sameInstance(ref)));
    queuedRef.clear();

    assertThat(closed.get(), is(true));
  }

  @SuppressWarnings("UnusedAssignment")
  @Test(timeout = 30000L)
  public void testWrappedSpliteratorSelfClose() throws Exception {
    final ReferenceQueue<WrappedReferenceStream<String>> queue = new ReferenceQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();
    WrappedReferenceStream<String> testStream = getTestStream();
    Spliterator<String> spliterator = testStream
        .selfClose(true)
        .onClose(() -> closed.set(true))
        .spliterator();
    final PhantomReference<WrappedReferenceStream<String>> ref = new PhantomReference<>(testStream, queue);
    this.wrappedStream = null;
    testStream = null;

    Stream<String> stream = StreamSupport.stream(spliterator, false);
    spliterator = null;     // unusedAssignment - explicit null of reference for garbage collection
    assertThat(stream.findAny().isPresent(), is(true));
    assertThat(closed.get(), is(false));
    stream = null;          // unusedAssignment - explicit null of reference for garbage collection

    /*
     * Await finalization of the spliterator and stream.
     */
    Reference<? extends WrappedReferenceStream<String>> queuedRef;
    while ((queuedRef = queue.poll()) == null) {
      System.gc();
      Thread.sleep(100L);
    }
    assertThat(queuedRef, is(sameInstance(ref)));
    queuedRef.clear();

    assertThat(closed.get(), is(true));
  }

  @Test
  public void testUnordered() throws Exception {
    testOperation(IntermediateOperation.UNORDERED, stream -> {
      final Stream<String> nextStream = stream.unordered();
      assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
    });
  }

  /**
   * Provides a test holding the common checks made for most {@code Stream} operations.
   *
   * @param operation the {@code Operation} enum constant identifying the {@code Stream} operation being tested
   * @param operationTest a {@code Consumer} providing the testing unique to the {@code Stream} operation
   * @param operationArguments the arguments to the operation being test that must be retained in
   *                           {@code PipelineMetaData}
   */
  private void testOperation(final Operation operation,
                             final Consumer<WrappedReferenceStream<String>> operationTest,
                             final Object... operationArguments) {
    final WrappedReferenceStream<String> stream = getTestStream();
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

    final List<Object> capturedArguments = pipelineOperation.getArguments();
    assertThat(capturedArguments.size(), is(equalTo(operationArguments.length)));
    if (operationArguments.length != 0) {
      for (int i = 0; i < operationArguments.length; i++) {
        assertThat(capturedArguments.get(i), is(sameInstance(operationArguments[i])));
      }
    }
  }
}
