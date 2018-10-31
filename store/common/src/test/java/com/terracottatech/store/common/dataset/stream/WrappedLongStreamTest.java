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
import java.util.List;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
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
public class WrappedLongStreamTest {

  private static final int ITEM_COUNT = 50;

  private LongStream wrappedStream;
  private double average;
  private long sum;

  @Before
  public void setUp() throws Exception {
    this.wrappedStream = LongStream.rangeClosed(1, ITEM_COUNT);
    this.sum = (1 + ITEM_COUNT) * ITEM_COUNT / 2;
    this.average = sum / (ITEM_COUNT * 1.0D);
  }

  private WrappedLongStream getTestStream() {
    return new WrappedLongStream(this.wrappedStream);
  }

  @Test
  public void testInstantiation() throws Exception {
    final WrappedLongStream stream = getTestStream();
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
    final WrappedLongStream streamHead = getTestStream();
    streamHead.filter(i -> i % 4 == 0);
    try {
      streamHead.filter(i -> i % 10 == 1);
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testReuse() throws Exception {
    final WrappedLongStream streamHead = getTestStream();
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
    final WrappedLongStream stream = getTestStream();

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
    final LongPredicate predicate = i -> i % 4 == 0;
    testOperation(TerminalOperation.LONG_ALL_MATCH, stream -> assertFalse(stream.allMatch(predicate)), predicate);
  }

  @Test
  public void testAnyMatch() throws Exception {
    final LongPredicate predicate = i -> i % 4 == 0;
    testOperation(TerminalOperation.LONG_ANY_MATCH, stream -> assertTrue(stream.anyMatch(predicate)), predicate);
  }

  @Test
  public void testAsDoubleStream() throws Exception {
    testOperation(IntermediateOperation.AS_DOUBLE_STREAM,
        stream -> {
          final DoubleStream nextStream = stream.asDoubleStream();
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        });
  }

  @Test
  public void testAverage() throws Exception {
    testOperation(TerminalOperation.AVERAGE,
        stream -> {
          final OptionalDouble optionalAverage = stream.average();
          assertThat(optionalAverage.isPresent(), is(true));
          final double observedAverage = optionalAverage.getAsDouble();
          assertThat(observedAverage, is(closeTo(this.average, 2 * Math.max(Math.ulp(this.average), Math.ulp(observedAverage)))));
        });
  }

  @Test
  public void testBoxed() throws Exception {
    testOperation(IntermediateOperation.BOXED,
        stream -> {
          final Stream<Long> nextStream = stream.boxed();
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        });
  }

  @Test
  public void testCollect() throws Exception {
    final Supplier<ArrayList<Object>> supplier = ArrayList::new;
    final ObjLongConsumer<ArrayList<Object>> accumulator = ArrayList::add;
    final BiConsumer<ArrayList<Object>, ArrayList<Object>> combiner = ArrayList::addAll;
    testOperation(TerminalOperation.LONG_COLLECT,
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
      final LongStream nextStream = stream.distinct();
      assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
    });
  }

  @Test
  public void testFilter() throws Exception {
    final LongPredicate filterPredicate = i -> i % 4 == 0;
    testOperation(IntermediateOperation.LONG_FILTER,
        stream -> {
          final LongStream nextStream = stream.filter(filterPredicate);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
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
    final LongFunction<LongStream> mapper = i -> LongStream.rangeClosed(1, i);
    testOperation(IntermediateOperation.LONG_FLAT_MAP,
        stream -> {
          final LongStream nextStream = stream.flatMap(mapper);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        mapper);
  }

  @Test
  public void testForEach() throws Exception {
    final LongConsumer consumer = i -> { };
    testOperation(TerminalOperation.LONG_FOR_EACH, stream -> stream.forEach(consumer), consumer);
  }

  @Test
  public void testForEachOrdered() throws Exception {
    final LongConsumer consumer = i -> { };
    testOperation(TerminalOperation.LONG_FOR_EACH_ORDERED, stream -> stream.forEachOrdered(consumer), consumer);
  }

  @Test
  public void testLimit() throws Exception {
    final long maxSize = 5;
    testOperation(IntermediateOperation.LIMIT,
        stream -> {
          final LongStream nextStream = stream.limit(maxSize);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        maxSize);
  }

  @Test
  public void testMap() throws Exception {
    final LongUnaryOperator mapper = i -> i * 2;
    testOperation(IntermediateOperation.LONG_MAP,
        stream -> {
          final LongStream nextStream = stream.map(mapper);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToDouble() throws Exception {
    final LongToDoubleFunction mapper = i -> i * 2.0D;
    testOperation(IntermediateOperation.LONG_MAP_TO_DOUBLE,
        stream -> {
          final DoubleStream nextStream = stream.mapToDouble(mapper);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToInt() throws Exception {
    final LongToIntFunction mapper = i -> (int) i;
    testOperation(IntermediateOperation.LONG_MAP_TO_INT,
        stream -> {
          final IntStream nextStream = stream.mapToInt(mapper);
          assertThat(nextStream, is(instanceOf(WrappedIntStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToObj() throws Exception {
    final LongFunction<String> mapper = Long::toString;
    testOperation(IntermediateOperation.LONG_MAP_TO_OBJ,
        stream -> {
          final Stream<String> nextStream = stream.mapToObj(mapper);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        mapper);
  }

  @Test
  public void testMax() throws Exception {
    testOperation(TerminalOperation.MAX_0, stream -> assertThat(stream.max().getAsLong(), is(equalTo((long)ITEM_COUNT))));
  }

  @Test
  public void testMin() throws Exception {
    testOperation(TerminalOperation.MIN_0, stream -> assertThat(stream.min().getAsLong(), is(equalTo(1L))));
  }

  @Test
  public void testNoneMatch() throws Exception {
    final LongPredicate predicate = i -> i % 4 == 0;
    testOperation(TerminalOperation.LONG_NONE_MATCH, stream -> assertFalse(stream.noneMatch(predicate)), predicate);
  }

  @Test
  public void testPeek() throws Exception {
    final LongConsumer consumer = i -> { };
    testOperation(IntermediateOperation.LONG_PEEK,
        stream -> {
          final LongStream nextStream = stream.peek(consumer);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        consumer);
  }

  @Test
  public void testReduce1Arg() throws Exception {
    final LongBinaryOperator op = (i1, i2) -> i2;
    testOperation(TerminalOperation.LONG_REDUCE_1,
        stream -> assertThat(stream.reduce(op).getAsLong(), is((long)ITEM_COUNT)),
        op);
  }

  @Test
  public void testReduce2Arg() throws Exception {
    final long identity = 0L;
    final LongBinaryOperator op = (i1, i2) -> i2;
    testOperation(TerminalOperation.LONG_REDUCE_2,
        stream -> assertThat(stream.reduce(identity, op), is((long)ITEM_COUNT)),
        identity, op);
  }

  @Test
  public void testSkip() throws Exception {
    final long n = 1L;
    testOperation(IntermediateOperation.SKIP,
        stream -> {
          final LongStream nextStream = stream.skip(n);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        n);
  }

  @Test
  public void testSorted() throws Exception {
    testOperation(IntermediateOperation.SORTED_0, stream -> {
      final LongStream nextStream = stream.sorted();
      assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
    });
  }

  @Test
  public void testSum() throws Exception {
    testOperation(TerminalOperation.SUM, stream -> assertThat(stream.sum(), is(equalTo(this.sum))));
  }

  @Test
  public void testSummaryStatistics() throws Exception {
    testOperation(TerminalOperation.SUMMARY_STATISTICS, stream -> assertThat(stream.summaryStatistics(), is(notNullValue())));
  }

  @Test
  public void testToArray() throws Exception {
    testOperation(TerminalOperation.TO_ARRAY_0, stream -> {
      final long[] array = stream.toArray();
      assertThat(array.length, is(ITEM_COUNT));
    });
  }

  @Test
  public void testIterator() throws Exception {
    final AtomicBoolean closed = new AtomicBoolean();
    try (final WrappedLongStream stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      stream.onClose(() -> closed.set(true));

      final PrimitiveIterator.OfLong streamIterator = stream.iterator();

      assertThat(terminalAction.get(), is(true));
      assertThat(closed.get(), is(false));

      assertTrue(streamIterator.hasNext());

      streamIterator.forEachRemaining((Consumer<? super Long>)s -> { });

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
    final ReferenceQueue<WrappedLongStream> queue = new ReferenceQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();
    WrappedLongStream stream = getTestStream();
    PrimitiveIterator.OfLong iterator = stream
        .selfClose(true)
        .onClose(() -> closed.set(true))
        .iterator();
    final PhantomReference<WrappedLongStream> ref = new PhantomReference<>(stream, queue);
    this.wrappedStream = null;
    stream = null;

    assertNotNull(iterator.next());
    assertThat(closed.get(), is(false));

    iterator = null;   // unusedAssignment - explicit null of reference for garbage collection

    /*
     * Await finalization of the iterator and stream.
     */
    Reference<? extends WrappedLongStream> queuedRef;
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
    final WrappedLongStream stream = getTestStream();
    final AtomicBoolean terminalAction = new AtomicBoolean();
    stream.appendTerminalAction(metaData -> terminalAction.set(true));

    final AtomicBoolean closed = new AtomicBoolean();
    stream.onClose(() -> closed.set(true));

    final LongStream nextStream = stream.onClose(() -> { });
    assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
    assertThat(terminalAction.get(), is(false));
    assertThat(closed.get(), is(false));

    final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
    assertThat(pipeline.size(), is(0));
  }

  @Test
  public void testParallel() throws Exception {
    testOperation(IntermediateOperation.PARALLEL, stream -> {
      final LongStream nextStream = stream.parallel();
      assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
    });
  }

  @Test
  public void testSelfClose() throws Exception {
    testOperation(IntermediateOperation.SELF_CLOSE, stream -> {
      final LongStream nextStream = stream.selfClose(!WrappedStream.SELF_CLOSE_DEFAULT);
      assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
    });
  }

  @Test
  public void testSequential() throws Exception {
    testOperation(IntermediateOperation.SEQUENTIAL, stream -> {
      final LongStream nextStream = stream.sequential();
      assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
    });
  }

  @Test
  public void testSpliterator() throws Exception {
    final AtomicBoolean closed = new AtomicBoolean();
    try (final WrappedLongStream stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      stream.onClose(() -> closed.set(true));

      final Spliterator.OfLong spliterator = stream.spliterator();

      assertThat(terminalAction.get(), is(true));
      assertThat(closed.get(), is(false));

      spliterator.forEachRemaining((Consumer<? super Long>)s -> { });

      assertThat(closed.get(), is(false));
      assertFalse(spliterator.tryAdvance((Consumer<? super Long>)s -> { }));

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
    final ReferenceQueue<WrappedLongStream> queue = new ReferenceQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();
    WrappedLongStream stream = getTestStream();
    Spliterator.OfLong spliterator = stream
        .selfClose(true)
        .onClose(() -> closed.set(true))
        .spliterator();
    final PhantomReference<WrappedLongStream> ref = new PhantomReference<>(stream, queue);
    this.wrappedStream = null;
    stream = null;

    assertTrue(spliterator.tryAdvance((LongConsumer)l -> { }));
    assertThat(closed.get(), is(false));

    spliterator = null;   // unusedAssignment - explicit null of reference for garbage collection

    /*
     * Await finalization of the spliterator and stream.
     */
    Reference<? extends WrappedLongStream> queuedRef;
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
      final LongStream nextStream = stream.unordered();
      assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
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
                             final Consumer<WrappedLongStream> operationTest,
                             final Object... operationArguments) {
    final WrappedLongStream stream = getTestStream();
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
        if (operationArguments[i] instanceof Long) {
          assertThat(capturedArguments.get(i), is(equalTo(operationArguments[i])));
        } else {
          assertThat(capturedArguments.get(i), is(sameInstance(operationArguments[i])));
        }
      }
    }
  }
}
