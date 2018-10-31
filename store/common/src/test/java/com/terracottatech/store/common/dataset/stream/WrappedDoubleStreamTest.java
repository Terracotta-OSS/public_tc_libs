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
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
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
public class WrappedDoubleStreamTest {
  private static final int ITEM_COUNT = 50;

  private DoubleStream wrappedStream;
  private double average;
  private double sum;

  @Before
  public void setUp() throws Exception {
    this.wrappedStream = LongStream.rangeClosed(1, ITEM_COUNT).asDoubleStream();
    this.sum = (1 + ITEM_COUNT) * ITEM_COUNT / 2;
    this.average = sum / ITEM_COUNT;
  }

  private WrappedDoubleStream getTestStream() {
    return new WrappedDoubleStream(this.wrappedStream);
  }

  @Test
  public void testInstantiation() throws Exception {
    final WrappedDoubleStream stream = getTestStream();
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
    final WrappedDoubleStream streamHead = getTestStream();
    streamHead.filter(d -> Math.IEEEremainder(d, 4.0D) == 0.0D);
    try {
      streamHead.filter(d -> Math.IEEEremainder(d, 10.0D) == 1.0D);
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testReuse() throws Exception {
    final WrappedDoubleStream streamHead = getTestStream();
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
    final WrappedDoubleStream stream = getTestStream();

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
    final DoublePredicate predicate = d -> Math.IEEEremainder(d, 4.0D) == 0.0D;
    testOperation(TerminalOperation.DOUBLE_ALL_MATCH, stream -> assertFalse(stream.allMatch(predicate)), predicate);
  }

  @Test
  public void testAnyMatch() throws Exception {
    final DoublePredicate predicate = d -> Math.IEEEremainder(d, 4.0D) == 0.0D;
    testOperation(TerminalOperation.DOUBLE_ANY_MATCH, stream -> assertTrue(stream.anyMatch(predicate)), predicate);
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
          final Stream<Double> nextStream = stream.boxed();
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        });
  }

  @Test
  public void testCollect() throws Exception {
    final Supplier<ArrayList<Object>> supplier = ArrayList::new;
    final ObjDoubleConsumer<ArrayList<Object>> accumulator = ArrayList::add;
    final BiConsumer<ArrayList<Object>, ArrayList<Object>> combiner = ArrayList::addAll;
    testOperation(TerminalOperation.DOUBLE_COLLECT,
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
      final DoubleStream nextStream = stream.distinct();
      assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
    });
  }

  @Test
  public void testFilter() throws Exception {
    final DoublePredicate filterPredicate = d -> Math.IEEEremainder(d, 4.0D) == 0.0D;
    testOperation(IntermediateOperation.DOUBLE_FILTER,
        stream -> {
          final DoubleStream nextStream = stream.filter(filterPredicate);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
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
    final DoubleFunction<DoubleStream> mapper = d -> LongStream.rangeClosed(1, (long)d).asDoubleStream();
    testOperation(IntermediateOperation.DOUBLE_FLAT_MAP,
        stream -> {
          final DoubleStream nextStream = stream.flatMap(mapper);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        mapper);
  }

  @Test
  public void testForEach() throws Exception {
    final DoubleConsumer consumer = d -> { };
    testOperation(TerminalOperation.DOUBLE_FOR_EACH, stream -> stream.forEach(consumer), consumer);
  }

  @Test
  public void testForEachOrdered() throws Exception {
    final DoubleConsumer consumer = d -> { };
    testOperation(TerminalOperation.DOUBLE_FOR_EACH_ORDERED, stream -> stream.forEachOrdered(consumer), consumer);
  }

  @Test
  public void testLimit() throws Exception {
    final long maxSize = 5;
    testOperation(IntermediateOperation.LIMIT,
        stream -> {
          final DoubleStream nextStream = stream.limit(maxSize);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        maxSize);
  }

  @Test
  public void testMap() throws Exception {
    final DoubleUnaryOperator mapper = d -> d * 2.0D;
    testOperation(IntermediateOperation.DOUBLE_MAP,
        stream -> {
          final DoubleStream nextStream = stream.map(mapper);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToLong() throws Exception {
    final DoubleToLongFunction mapper = d -> (long) d * 2L;
    testOperation(IntermediateOperation.DOUBLE_MAP_TO_LONG,
        stream -> {
          final LongStream nextStream = stream.mapToLong(mapper);
          assertThat(nextStream, is(instanceOf(WrappedLongStream.class)));
        },
        mapper);
  }

  @Test
  public void testMapToObj() throws Exception {
    final DoubleFunction<String> mapper = Double::toString;
    testOperation(IntermediateOperation.DOUBLE_MAP_TO_OBJ,
        stream -> {
          final Stream<String> nextStream = stream.mapToObj(mapper);
          assertThat(nextStream, is(instanceOf(WrappedReferenceStream.class)));
        },
        mapper);
  }

  @Test
  public void testMax() throws Exception {
    testOperation(TerminalOperation.MAX_0, stream -> assertThat(stream.max().getAsDouble(), is(equalTo((double)ITEM_COUNT))));
  }

  @Test
  public void testMin() throws Exception {
    testOperation(TerminalOperation.MIN_0, stream -> assertThat(stream.min().getAsDouble(), is(equalTo(1D))));
  }

  @Test
  public void testNoneMatch() throws Exception {
    final DoublePredicate predicate = d -> Math.IEEEremainder(d, 4.0D) == 0.0D;
    testOperation(TerminalOperation.DOUBLE_NONE_MATCH, stream -> assertFalse(stream.noneMatch(predicate)), predicate);
  }

  @Test
  public void testPeek() throws Exception {
    final DoubleConsumer consumer = d -> { };
    testOperation(IntermediateOperation.DOUBLE_PEEK,
        stream -> {
          final DoubleStream nextStream = stream.peek(consumer);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        consumer);
  }

  @Test
  public void testReduce1Arg() throws Exception {
    final DoubleBinaryOperator op = (i1, i2) -> i2;
    testOperation(TerminalOperation.DOUBLE_REDUCE_1,
        stream -> assertThat(stream.reduce(op).getAsDouble(), is((double)ITEM_COUNT)),
        op);
  }

  @Test
  public void testReduce2Arg() throws Exception {
    final double identity = 0;
    final DoubleBinaryOperator op = (i1, i2) -> i2;
    testOperation(TerminalOperation.DOUBLE_REDUCE_2,
        stream -> assertThat(stream.reduce(identity, op), is((double)ITEM_COUNT)),
        identity, op);
  }

  @Test
  public void testSkip() throws Exception {
    final long n = 1L;
    testOperation(IntermediateOperation.SKIP,
        stream -> {
          final DoubleStream nextStream = stream.skip(n);
          assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
        },
        n);
  }

  @Test
  public void testSorted() throws Exception {
    testOperation(IntermediateOperation.SORTED_0, stream -> {
      final DoubleStream nextStream = stream.sorted();
      assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
    });
  }

  @Test
  public void testSum() throws Exception {
    testOperation(TerminalOperation.SUM, stream -> {
      final double observedSum = stream.sum();
      assertThat(observedSum, is(closeTo(this.sum, 2 * Math.max(Math.ulp(observedSum), (Math.ulp(this.sum))))));
    });
  }

  @Test
  public void testSummaryStatistics() throws Exception {
    testOperation(TerminalOperation.SUMMARY_STATISTICS, stream -> assertThat(stream.summaryStatistics(), is(notNullValue())));
  }

  @Test
  public void testIterator() throws Exception {
    final AtomicBoolean closed = new AtomicBoolean();
    try (final WrappedDoubleStream stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      stream.onClose(() -> closed.set(true));

      final PrimitiveIterator.OfDouble streamIterator = stream.iterator();

      assertThat(terminalAction.get(), is(true));
      assertThat(closed.get(), is(false));

      assertTrue(streamIterator.hasNext());

      streamIterator.forEachRemaining((Consumer<? super Double>)s -> { });

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
    final ReferenceQueue<WrappedDoubleStream> queue = new ReferenceQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();
    WrappedDoubleStream stream = getTestStream();
    PrimitiveIterator.OfDouble iterator = stream
        .selfClose(true)
        .onClose(() -> closed.set(true))
        .iterator();
    final PhantomReference<WrappedDoubleStream> ref = new PhantomReference<>(stream, queue);
    this.wrappedStream = null;
    stream = null;      // unusedAssignment - explicit null of reference for garbage collection

    assertNotNull(iterator.next());
    assertThat(closed.get(), is(false));

    iterator = null;   // unusedAssignment - explicit null of reference for garbage collection

    /*
     * Await finalization of the iterator and stream.
     */
    Reference<? extends WrappedDoubleStream> queuedRef;
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
    final WrappedDoubleStream stream = getTestStream();
    final AtomicBoolean terminalAction = new AtomicBoolean();
    stream.appendTerminalAction(metaData -> terminalAction.set(true));

    final AtomicBoolean closed = new AtomicBoolean();
    stream.onClose(() -> closed.set(true));

    final DoubleStream nextStream = stream.onClose(() -> { });
    assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
    assertThat(terminalAction.get(), is(false));
    assertThat(closed.get(), is(false));

    final List<PipelineOperation> pipeline = stream.getMetaData().getPipeline();
    assertThat(pipeline.size(), is(0));
  }

  @Test
  public void testParallel() throws Exception {
    testOperation(IntermediateOperation.PARALLEL, stream -> {
      final DoubleStream nextStream = stream.parallel();
      assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
    });
  }

  @Test
  public void testSelfClose() throws Exception {
    testOperation(IntermediateOperation.SELF_CLOSE, stream -> {
      final DoubleStream nextStream = stream.selfClose(!WrappedStream.SELF_CLOSE_DEFAULT);
      assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
    });
  }

  @Test
  public void testSequential() throws Exception {
    testOperation(IntermediateOperation.SEQUENTIAL, stream -> {
      final DoubleStream nextStream = stream.sequential();
      assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
    });
  }

  @Test
  public void testSpliterator() throws Exception {
    final AtomicBoolean closed = new AtomicBoolean();
    try (final WrappedDoubleStream stream = getTestStream()) {
      final AtomicBoolean terminalAction = new AtomicBoolean();
      stream.appendTerminalAction(metaData -> terminalAction.set(true));

      stream.onClose(() -> closed.set(true));

      final Spliterator.OfDouble spliterator = stream.spliterator();

      assertThat(terminalAction.get(), is(true));
      assertThat(closed.get(), is(false));

      spliterator.forEachRemaining((Consumer<? super Double>)s -> { });

      assertThat(closed.get(), is(false));
      assertFalse(spliterator.tryAdvance((Consumer<? super Double>)s -> { }));

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
    final ReferenceQueue<WrappedDoubleStream> queue = new ReferenceQueue<>();

    final AtomicBoolean closed = new AtomicBoolean();
    WrappedDoubleStream stream = getTestStream();
    Spliterator.OfDouble spliterator = stream
        .selfClose(true)
        .onClose(() -> closed.set(true))
        .spliterator();

    final PhantomReference<WrappedDoubleStream> ref = new PhantomReference<>(stream, queue);
    this.wrappedStream = null;
    stream = null;         // unusedAssignment - explicit null of reference for garbage collection

    assertTrue(spliterator.tryAdvance((DoubleConsumer)d -> { }));
    assertThat(closed.get(), is(false));

    spliterator = null;   // unusedAssignment - explicit null of reference for garbage collection

    /*
     * Await finalization of the spliterator and stream.
     */
    Reference<? extends WrappedDoubleStream> queuedRef;
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
      final DoubleStream nextStream = stream.unordered();
      assertThat(nextStream, is(instanceOf(WrappedDoubleStream.class)));
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
                             final Consumer<WrappedDoubleStream> operationTest,
                             final Object... operationArguments) {
    final WrappedDoubleStream stream = getTestStream();
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
        if (operationArguments[i] instanceof Double) {
          assertThat(capturedArguments.get(i), is(equalTo(operationArguments[i])));
        } else {
          assertThat(capturedArguments.get(i), is(sameInstance(operationArguments[i])));
        }
      }
    }
  }
}