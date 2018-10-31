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
package com.terracottatech.store.async;

import org.junit.Test;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutorDrivenAsyncIntStreamTest extends AbstractExecutorDrivenAsyncTest {

  @Test
  public void testFilter() {
    testIntermediate(AsyncIntStream::filter, IntStream::filter, IntPredicate.class);
  }

  @Test
  public void testMap() {
    testIntermediate(AsyncIntStream::map, IntStream::map, IntUnaryOperator.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMapToObj() {
    testObjIntermediate(AsyncIntStream::mapToObj, IntStream::mapToObj, IntFunction.class);
  }

  @Test
  public void testMapToLong() {
    testLongIntermediate(AsyncIntStream::mapToLong, IntStream::mapToLong, IntToLongFunction.class);
  }

  @Test
  public void testMapToDouble() {
    testDoubleIntermediate(AsyncIntStream::mapToDouble, IntStream::mapToDouble, IntToDoubleFunction.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFlatMap() {
    testIntermediate(AsyncIntStream::flatMap, IntStream::flatMap, IntFunction.class);
  }

  @Test
  public void testDistinct() {
    testIntermediate(AsyncIntStream::distinct, IntStream::distinct);
  }

  @Test
  public void testSorted() {
    testIntermediate(AsyncIntStream::sorted, IntStream::sorted);
  }

  @Test
  public void testPeek() {
    testIntermediate(AsyncIntStream::peek, IntStream::peek, IntConsumer.class);
  }

  @Test
  public void testLimit() {
    testIntermediate(AsyncIntStream::limit, IntStream::limit, Long.TYPE);
  }

  @Test
  public void testSkip() {
    testIntermediate(AsyncIntStream::skip, IntStream::skip, Long.TYPE);
  }

  @Test
  public void testForEach() {
    testTerminal(AsyncIntStream::forEach, IntStream::forEach, IntConsumer.class);
  }

  @Test
  public void testForEachOrdered() {
    testTerminal(AsyncIntStream::forEachOrdered, IntStream::forEachOrdered, IntConsumer.class);
  }

  @Test
  public void testToArray() {
    testTerminal(int[].class, AsyncIntStream::toArray, IntStream::toArray);
  }

  @Test
  public void testReduceWithIdentity() {
    testTerminal(Integer.class, AsyncIntStream::reduce, IntStream::reduce, Integer.TYPE, IntBinaryOperator.class);
  }

  @Test
  public void testReduce() {
    testTerminal(OptionalInt.class, AsyncIntStream::reduce, IntStream::reduce, IntBinaryOperator.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCollect() {
    testTerminal(Object.class, AsyncIntStream::collect, IntStream::collect, Supplier.class, ObjIntConsumer.class, BiConsumer.class);
  }

  @Test
  public void testSum() {
    testTerminal(Integer.class, AsyncIntStream::sum, IntStream::sum);
  }

  @Test
  public void testMin() {
    testTerminal(OptionalInt.class, AsyncIntStream::min, IntStream::min);
  }

  @Test
  public void testMax() {
    testTerminal(OptionalInt.class, AsyncIntStream::max, IntStream::max);
  }

  @Test
  public void testCount() {
    testTerminal(Long.class, AsyncIntStream::count, IntStream::count);
  }

  @Test
  public void testAverage() {
    testTerminal(OptionalDouble.class, AsyncIntStream::average, IntStream::average);
  }

  @Test
  public void testSummaryStatistics() {
    testTerminal(IntSummaryStatistics.class, AsyncIntStream::summaryStatistics, IntStream::summaryStatistics);
  }

  @Test
  public void testAnyMatch() {
    testTerminal(Boolean.class, AsyncIntStream::anyMatch, IntStream::anyMatch, IntPredicate.class);
  }

  @Test
  public void testAllMatch() {
    testTerminal(Boolean.class, AsyncIntStream::allMatch, IntStream::allMatch, IntPredicate.class);
  }

  @Test
  public void testNoneMatch() {
    testTerminal(Boolean.class, AsyncIntStream::noneMatch, IntStream::noneMatch, IntPredicate.class);
  }

  @Test
  public void testFindFirst() {
    testTerminal(OptionalInt.class, AsyncIntStream::findFirst, IntStream::findFirst);
  }

  @Test
  public void testFindAny() {
    testTerminal(OptionalInt.class, AsyncIntStream::findAny, IntStream::findAny);
  }

  @Test
  public void testAsLongStream() {
    testLongIntermediate(AsyncIntStream::asLongStream, IntStream::asLongStream);
  }

  @Test
  public void testAsDoubleStream() {
    testDoubleIntermediate(AsyncIntStream::asDoubleStream, IntStream::asDoubleStream);
  }

  @Test
  public void testBoxed() {
    testObjIntermediate(AsyncIntStream::boxed, IntStream::boxed);
  }

  @Test
  public void testSequential() {
    testIntermediate(AsyncIntStream::sequential, IntStream::sequential);
  }

  @Test
  public void testParallel() {
    testIntermediate(AsyncIntStream::parallel, IntStream::parallel);
  }

  @Test
  public void testIterator() {
    IntStream stream = mock(IntStream.class);
    PrimitiveIterator.OfInt iterator = mock(PrimitiveIterator.OfInt.class);
    when(stream.iterator()).thenReturn(iterator);
    ExecutorDrivenAsyncIntStream asyncStream = new ExecutorDrivenAsyncIntStream(stream, mock(Executor.class));

    assertThat(asyncStream.iterator(), is(iterator));
    verify(stream).iterator();
  }

  @Test
  public void testSpliterator() {
    IntStream stream = mock(IntStream.class);
    Spliterator.OfInt spliterator = mock(Spliterator.OfInt.class);
    when(stream.spliterator()).thenReturn(spliterator);
    ExecutorDrivenAsyncIntStream asyncStream = new ExecutorDrivenAsyncIntStream(stream, mock(Executor.class));

    assertThat(asyncStream.spliterator(), is(spliterator));
    verify(stream).spliterator();
  }

  @Test
  public void testIsParallel() {
    IntStream stream = mock(IntStream.class);
    when(stream.isParallel()).thenReturn(true);
    ExecutorDrivenAsyncIntStream asyncStream = new ExecutorDrivenAsyncIntStream(stream, mock(Executor.class));

    assertThat(asyncStream.isParallel(), is(true));
    verify(stream).isParallel();
  }

  @Test
  public void testUnordered() {
    testIntermediate(AsyncIntStream::unordered, IntStream::unordered);
  }

  @Test
  public void testOnClose() {
    testIntermediate(AsyncIntStream::onClose, IntStream::onClose, Runnable.class);
  }

  @Test
  public void testClose() {
    IntStream stream = mock(IntStream.class);
    ExecutorDrivenAsyncIntStream asyncStream = new ExecutorDrivenAsyncIntStream(stream, mock(Executor.class));

    asyncStream.close();

    verify(stream).close();
  }

  private static void testIntermediate(Function<AsyncIntStream, AsyncIntStream> asyncMethod, Function<IntStream, IntStream> streamMethod) {
    Executor executor = mock(Executor.class);
    testDelegation(s -> new ExecutorDrivenAsyncIntStream(s, executor), ExecutorDrivenAsyncIntStream.class, asyncMethod::apply,
            IntStream.class, IntStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testIntermediate(BiFunction<AsyncIntStream, U, AsyncIntStream> asyncMethod, BiFunction<IntStream, U, IntStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static void testLongIntermediate(Function<AsyncIntStream, AsyncLongStream> asyncMethod, Function<IntStream, LongStream> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncIntStream(s, executor), ExecutorDrivenAsyncLongStream.class, asyncMethod::apply,
            IntStream.class, LongStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testLongIntermediate(BiFunction<AsyncIntStream, U, AsyncLongStream> asyncMethod, BiFunction<IntStream, U, LongStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);

    testLongIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static void testDoubleIntermediate(Function<AsyncIntStream, AsyncDoubleStream> asyncMethod, Function<IntStream, DoubleStream> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncIntStream(s, executor), ExecutorDrivenAsyncDoubleStream.class, asyncMethod::apply,
            IntStream.class, DoubleStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testDoubleIntermediate(BiFunction<AsyncIntStream, U, AsyncDoubleStream> asyncMethod, BiFunction<IntStream, U, DoubleStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testDoubleIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static void testObjIntermediate(Function<AsyncIntStream, AsyncStream<?>> asyncMethod, Function<IntStream, Stream<?>> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncIntStream(s, executor), ExecutorDrivenAsyncStream.class, asyncMethod::apply,
            IntStream.class, Stream.class, streamMethod::apply, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testObjIntermediate(BiFunction<AsyncIntStream, U, AsyncStream<?>> asyncMethod, BiFunction<IntStream, U, Stream<?>> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testObjIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static <T> void testTerminal(Class<T> resultType, Function<AsyncIntStream, Operation<T>> asyncMethod, Function<IntStream, T> streamMethod) {
    testDelegation(s -> new ExecutorDrivenAsyncIntStream(s, ForkJoinPool.commonPool()), resultType, asyncMethod.andThen(AbstractExecutorDrivenAsyncTest::retrieve),
            IntStream.class, resultType, streamMethod, (asyncOutput, delegateOutput) -> assertThat(asyncOutput, is(delegateOutput)));
  }

  private static <U> void testTerminal(BiFunction<AsyncIntStream, U, Operation<Void>> asyncMethod, BiConsumer<IntStream, U> streamMethod, Class<U> paramType) {
    U u = createInstance(paramType);
    testTerminal(Void.TYPE, as -> asyncMethod.apply(as, u), s -> { streamMethod.accept(s, u); return null; });
  }

  private static <T, U> void testTerminal(Class<T> resultType, BiFunction<AsyncIntStream, U, Operation<T>> asyncMethod, BiFunction<IntStream, U, T> streamMethod, Class<U> paramType) {
    U u = createInstance(paramType);
    testTerminal(resultType, as -> asyncMethod.apply(as, u), s -> streamMethod.apply(s, u));
  }

  private static <T, U, V> void testTerminal(Class<T> resultType, TriFunction<AsyncIntStream, U, V, Operation<T>> asyncMethod, TriFunction<IntStream, U, V, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV) {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v), s -> streamMethod.apply(s, u, v));
  }

  private static <T, U, V, W> void testTerminal(Class<T> resultType, QuadFunction<AsyncIntStream, U, V, W, Operation<T>> asyncMethod, QuadFunction<IntStream, U, V, W, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV, Class<W> paramTypeW) {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    W w = createInstance(paramTypeW);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v, w), s -> streamMethod.apply(s, u, v, w));
  }

}
