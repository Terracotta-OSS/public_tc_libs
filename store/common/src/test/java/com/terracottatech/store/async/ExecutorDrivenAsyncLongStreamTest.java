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

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutorDrivenAsyncLongStreamTest extends AbstractExecutorDrivenAsyncTest {

  @Test
  public void testFilter() {
    testIntermediate(AsyncLongStream::filter, LongStream::filter, LongPredicate.class);
  }

  @Test
  public void testMap() {
    testIntermediate(AsyncLongStream::map, LongStream::map, LongUnaryOperator.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMapToObj() {
    testObjIntermediate(AsyncLongStream::mapToObj, LongStream::mapToObj, LongFunction.class);
  }

  @Test
  public void testMapToInt() {
    testIntIntermediate(AsyncLongStream::mapToInt, LongStream::mapToInt, LongToIntFunction.class);
  }

  @Test
  public void testMapToDouble() {
    testDoubleIntermediate(AsyncLongStream::mapToDouble, LongStream::mapToDouble, LongToDoubleFunction.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFlatMap() {
    testIntermediate(AsyncLongStream::flatMap, LongStream::flatMap, LongFunction.class);
  }

  @Test
  public void testDistinct() {
    testIntermediate(AsyncLongStream::distinct, LongStream::distinct);
  }

  @Test
  public void testSorted() {
    testIntermediate(AsyncLongStream::sorted, LongStream::sorted);
  }

  @Test
  public void testPeek() {
    testIntermediate(AsyncLongStream::peek, LongStream::peek, LongConsumer.class);
  }

  @Test
  public void testLimit() {
    testIntermediate(AsyncLongStream::limit, LongStream::limit, Long.TYPE);
  }

  @Test
  public void testSkip() {
    testIntermediate(AsyncLongStream::skip, LongStream::skip, Long.TYPE);
  }

  @Test
  public void testForEach() {
    testTerminal(AsyncLongStream::forEach, LongStream::forEach, LongConsumer.class);
  }

  @Test
  public void testForEachOrdered() {
    testTerminal(AsyncLongStream::forEachOrdered, LongStream::forEachOrdered, LongConsumer.class);
  }

  @Test
  public void testToArray() {
    testTerminal(long[].class, AsyncLongStream::toArray, LongStream::toArray);
  }

  @Test
  public void testReduceWithIdentity() {
    testTerminal(Long.class, AsyncLongStream::reduce, LongStream::reduce, Long.TYPE, LongBinaryOperator.class);
  }

  @Test
  public void testReduce() {
    testTerminal(OptionalLong.class, AsyncLongStream::reduce, LongStream::reduce, LongBinaryOperator.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCollect() {
    testTerminal(Object.class, AsyncLongStream::collect, LongStream::collect, Supplier.class, ObjLongConsumer.class, BiConsumer.class);
  }

  @Test
  public void testSum() {
    testTerminal(Long.class, AsyncLongStream::sum, LongStream::sum);
  }

  @Test
  public void testMin() {
    testTerminal(OptionalLong.class, AsyncLongStream::min, LongStream::min);
  }

  @Test
  public void testMax() {
    testTerminal(OptionalLong.class, AsyncLongStream::max, LongStream::max);
  }

  @Test
  public void testCount() {
    testTerminal(Long.class, AsyncLongStream::count, LongStream::count);
  }

  @Test
  public void testAverage() {
    testTerminal(OptionalDouble.class, AsyncLongStream::average, LongStream::average);
  }

  @Test
  public void testSummaryStatistics() {
    testTerminal(LongSummaryStatistics.class, AsyncLongStream::summaryStatistics, LongStream::summaryStatistics);
  }

  @Test
  public void testAnyMatch() {
    testTerminal(Boolean.class, AsyncLongStream::anyMatch, LongStream::anyMatch, LongPredicate.class);
  }

  @Test
  public void testAllMatch() {
    testTerminal(Boolean.class, AsyncLongStream::allMatch, LongStream::allMatch, LongPredicate.class);
  }

  @Test
  public void testNoneMatch() {
    testTerminal(Boolean.class, AsyncLongStream::noneMatch, LongStream::noneMatch, LongPredicate.class);
  }

  @Test
  public void testFindFirst() {
    testTerminal(OptionalLong.class, AsyncLongStream::findFirst, LongStream::findFirst);
  }

  @Test
  public void testFindAny() {
    testTerminal(OptionalLong.class, AsyncLongStream::findAny, LongStream::findAny);
  }

  @Test
  public void testAsDoubleStream() {
    testDoubleIntermediate(AsyncLongStream::asDoubleStream, LongStream::asDoubleStream);
  }

  @Test
  public void testBoxed() {
    testObjIntermediate(AsyncLongStream::boxed, LongStream::boxed);
  }

  @Test
  public void testSequential() {
    testIntermediate(AsyncLongStream::sequential, LongStream::sequential);
  }

  @Test
  public void testParallel() {
    testIntermediate(AsyncLongStream::parallel, LongStream::parallel);
  }

  @Test
  public void testIterator() {
    LongStream stream = mock(LongStream.class);
    PrimitiveIterator.OfLong iterator = mock(PrimitiveIterator.OfLong.class);
    when(stream.iterator()).thenReturn(iterator);
    ExecutorDrivenAsyncLongStream asyncStream = new ExecutorDrivenAsyncLongStream(stream, mock(Executor.class));

    assertThat(asyncStream.iterator(), is(iterator));
    verify(stream).iterator();
  }

  @Test
  public void testSpliterator() {
    LongStream stream = mock(LongStream.class);
    Spliterator.OfLong spliterator = mock(Spliterator.OfLong.class);
    when(stream.spliterator()).thenReturn(spliterator);
    ExecutorDrivenAsyncLongStream asyncStream = new ExecutorDrivenAsyncLongStream(stream, mock(Executor.class));

    assertThat(asyncStream.spliterator(), is(spliterator));
    verify(stream).spliterator();
  }

  @Test
  public void testIsParallel() {
    LongStream stream = mock(LongStream.class);
    when(stream.isParallel()).thenReturn(true);
    ExecutorDrivenAsyncLongStream asyncStream = new ExecutorDrivenAsyncLongStream(stream, mock(Executor.class));

    assertThat(asyncStream.isParallel(), is(true));
    verify(stream).isParallel();
  }

  @Test
  public void testUnordered() {
    testIntermediate(AsyncLongStream::unordered, LongStream::unordered);
  }

  @Test
  public void testOnClose() {
    testIntermediate(AsyncLongStream::onClose, LongStream::onClose, Runnable.class);
  }

  @Test
  public void testClose() {
    LongStream stream = mock(LongStream.class);
    ExecutorDrivenAsyncLongStream asyncStream = new ExecutorDrivenAsyncLongStream(stream, mock(Executor.class));

    asyncStream.close();

    verify(stream).close();
  }

  private static void testIntermediate(Function<AsyncLongStream, AsyncLongStream> asyncMethod, Function<LongStream, LongStream> streamMethod) {
    Executor executor = mock(Executor.class);
    testDelegation(s -> new ExecutorDrivenAsyncLongStream(s, executor), ExecutorDrivenAsyncLongStream.class, asyncMethod::apply,
            LongStream.class, LongStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testIntermediate(BiFunction<AsyncLongStream, U, AsyncLongStream> asyncMethod, BiFunction<LongStream, U, LongStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static <U> void testIntIntermediate(BiFunction<AsyncLongStream, U, AsyncIntStream> asyncMethod, BiFunction<LongStream, U, IntStream> streamMethod, Class<U> paramType) {
    Executor executor = mock(Executor.class);
    U parameter = createInstance(paramType);

    testDelegation(s -> new ExecutorDrivenAsyncLongStream(s, executor), ExecutorDrivenAsyncIntStream.class, as -> asyncMethod.apply(as, parameter),
            LongStream.class, IntStream.class, s -> streamMethod.apply(s, parameter), (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static void testDoubleIntermediate(Function<AsyncLongStream, AsyncDoubleStream> asyncMethod, Function<LongStream, DoubleStream> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncLongStream(s, executor), ExecutorDrivenAsyncDoubleStream.class, asyncMethod::apply,
            LongStream.class, DoubleStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testDoubleIntermediate(BiFunction<AsyncLongStream, U, AsyncDoubleStream> asyncMethod, BiFunction<LongStream, U, DoubleStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testDoubleIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static void testObjIntermediate(Function<AsyncLongStream, AsyncStream<?>> asyncMethod, Function<LongStream, Stream<?>> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncLongStream(s, executor), ExecutorDrivenAsyncStream.class, asyncMethod::apply,
            LongStream.class, Stream.class, streamMethod::apply, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testObjIntermediate(BiFunction<AsyncLongStream, U, AsyncStream<?>> asyncMethod, BiFunction<LongStream, U, Stream<?>> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testObjIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static <T> void testTerminal(Class<T> resultType, Function<AsyncLongStream, Operation<T>> asyncMethod, Function<LongStream, T> streamMethod) {
    testDelegation(s -> new ExecutorDrivenAsyncLongStream(s, ForkJoinPool.commonPool()), resultType, asyncMethod.andThen(AbstractExecutorDrivenAsyncTest::retrieve),
            LongStream.class, resultType, streamMethod, (asyncOutput, delegateOutput) -> assertThat(asyncOutput, is(delegateOutput)));
  }

  private static <U> void testTerminal(BiFunction<AsyncLongStream, U, Operation<Void>> asyncMethod, BiConsumer<LongStream, U> streamMethod, Class<U> paramType) {
    U u = createInstance(paramType);
    testTerminal(Void.TYPE, as -> asyncMethod.apply(as, u), s -> { streamMethod.accept(s, u); return null; });
  }

  private static <T, U> void testTerminal(Class<T> resultType, BiFunction<AsyncLongStream, U, Operation<T>> asyncMethod, BiFunction<LongStream, U, T> streamMethod, Class<U> paramType) {
    U u = createInstance(paramType);
    testTerminal(resultType, as -> asyncMethod.apply(as, u), s -> streamMethod.apply(s, u));
  }

  private static <T, U, V> void testTerminal(Class<T> resultType, TriFunction<AsyncLongStream, U, V, Operation<T>> asyncMethod, TriFunction<LongStream, U, V, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV) {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v), s -> streamMethod.apply(s, u, v));
  }

  private static <T, U, V, W> void testTerminal(Class<T> resultType, QuadFunction<AsyncLongStream, U, V, W, Operation<T>> asyncMethod, QuadFunction<LongStream, U, V, W, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV, Class<W> paramTypeW) {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    W w = createInstance(paramTypeW);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v, w), s -> streamMethod.apply(s, u, v, w));
  }

}
