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

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.Function;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
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

public class ExecutorDrivenAsyncDoubleStreamTest extends AbstractExecutorDrivenAsyncTest {

  @Test
  public void testFilter() {
    testIntermediate(AsyncDoubleStream::filter, DoubleStream::filter, DoublePredicate.class);
  }

  @Test
  public void testMap() {
    testIntermediate(AsyncDoubleStream::map, DoubleStream::map, DoubleUnaryOperator.class);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testMapToObj() {
    testObjIntermediate(AsyncDoubleStream::mapToObj, DoubleStream::mapToObj, DoubleFunction.class);
  }

  @Test
  public void testMapToInt() {
    testIntIntermediate(AsyncDoubleStream::mapToInt, DoubleStream::mapToInt, DoubleToIntFunction.class);
  }

  @Test
  public void testMapToLong() {
    testLongIntermediate(AsyncDoubleStream::mapToLong, DoubleStream::mapToLong, DoubleToLongFunction.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFlatMap() {
    testIntermediate(AsyncDoubleStream::flatMap, DoubleStream::flatMap, DoubleFunction.class);
  }

  @Test
  public void testDistinct() {
    testIntermediate(AsyncDoubleStream::distinct, DoubleStream::distinct);
  }

  @Test
  public void testSorted() {
    testIntermediate(AsyncDoubleStream::sorted, DoubleStream::sorted);
  }

  @Test
  public void testPeek() {
    testIntermediate(AsyncDoubleStream::peek, DoubleStream::peek, DoubleConsumer.class);
  }

  @Test
  public void testLimit() {
    testIntermediate(AsyncDoubleStream::limit, DoubleStream::limit, Long.TYPE);
  }

  @Test
  public void testSkip() {
    testIntermediate(AsyncDoubleStream::skip, DoubleStream::skip, Long.TYPE);
  }

  @Test
  public void testForEach() {
    testTerminal(AsyncDoubleStream::forEach, DoubleStream::forEach, DoubleConsumer.class);
  }

  @Test
  public void testForEachOrdered() {
    testTerminal(AsyncDoubleStream::forEachOrdered, DoubleStream::forEachOrdered, DoubleConsumer.class);
  }

  @Test
  public void testToArray() {
    testTerminal(double[].class, AsyncDoubleStream::toArray, DoubleStream::toArray);
  }

  @Test
  public void testReduceWithIdentity() {
    testTerminal(Double.class, AsyncDoubleStream::reduce, DoubleStream::reduce, Double.TYPE, DoubleBinaryOperator.class);
  }

  @Test
  public void testReduce() {
    testTerminal(OptionalDouble.class, AsyncDoubleStream::reduce, DoubleStream::reduce, DoubleBinaryOperator.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCollect() {
    testTerminal(Object.class, AsyncDoubleStream::collect, DoubleStream::collect, Supplier.class, ObjDoubleConsumer.class, BiConsumer.class);
  }

  @Test
  public void testSum() {
    testTerminal(Double.class, AsyncDoubleStream::sum, DoubleStream::sum);
  }

  @Test
  public void testMin() {
    testTerminal(OptionalDouble.class, AsyncDoubleStream::min, DoubleStream::min);
  }

  @Test
  public void testMax() {
    testTerminal(OptionalDouble.class, AsyncDoubleStream::max, DoubleStream::max);
  }

  @Test
  public void testCount() {
    testTerminal(Long.class, AsyncDoubleStream::count, DoubleStream::count);
  }

  @Test
  public void testAverage() {
    testTerminal(OptionalDouble.class, AsyncDoubleStream::average, DoubleStream::average);
  }

  @Test
  public void testSummaryStatistics() {
    testTerminal(DoubleSummaryStatistics.class, AsyncDoubleStream::summaryStatistics, DoubleStream::summaryStatistics);
  }

  @Test
  public void testAnyMatch() {
    testTerminal(Boolean.class, AsyncDoubleStream::anyMatch, DoubleStream::anyMatch, DoublePredicate.class);
  }

  @Test
  public void testAllMatch() {
    testTerminal(Boolean.class, AsyncDoubleStream::allMatch, DoubleStream::allMatch, DoublePredicate.class);
  }

  @Test
  public void testNoneMatch() {
    testTerminal(Boolean.class, AsyncDoubleStream::noneMatch, DoubleStream::noneMatch, DoublePredicate.class);
  }

  @Test
  public void testFindFirst() {
    testTerminal(OptionalDouble.class, AsyncDoubleStream::findFirst, DoubleStream::findFirst);
  }

  @Test
  public void testFindAny() {
    testTerminal(OptionalDouble.class, AsyncDoubleStream::findAny, DoubleStream::findAny);
  }

  @Test
  public void testBoxed() {
    testObjIntermediate(AsyncDoubleStream::boxed, DoubleStream::boxed);
  }

  @Test
  public void testSequential() {
    testIntermediate(AsyncDoubleStream::sequential, DoubleStream::sequential);
  }

  @Test
  public void testParallel() {
    testIntermediate(AsyncDoubleStream::parallel, DoubleStream::parallel);
  }

  @Test
  public void testIterator() {
    DoubleStream stream = mock(DoubleStream.class);
    PrimitiveIterator.OfDouble iterator = mock(PrimitiveIterator.OfDouble.class);
    when(stream.iterator()).thenReturn(iterator);
    ExecutorDrivenAsyncDoubleStream asyncStream = new ExecutorDrivenAsyncDoubleStream(stream, mock(Executor.class));

    assertThat(asyncStream.iterator(), is(iterator));
    verify(stream).iterator();
  }

  @Test
  public void testSpliterator() {
    DoubleStream stream = mock(DoubleStream.class);
    Spliterator.OfDouble spliterator = mock(Spliterator.OfDouble.class);
    when(stream.spliterator()).thenReturn(spliterator);
    ExecutorDrivenAsyncDoubleStream asyncStream = new ExecutorDrivenAsyncDoubleStream(stream, mock(Executor.class));

    assertThat(asyncStream.spliterator(), is(spliterator));
    verify(stream).spliterator();
  }

  @Test
  public void testIsParallel() {
    DoubleStream stream = mock(DoubleStream.class);
    when(stream.isParallel()).thenReturn(true);
    ExecutorDrivenAsyncDoubleStream asyncStream = new ExecutorDrivenAsyncDoubleStream(stream, mock(Executor.class));

    assertThat(asyncStream.isParallel(), is(true));
    verify(stream).isParallel();
  }

  @Test
  public void testUnordered() {
    testIntermediate(AsyncDoubleStream::unordered, DoubleStream::unordered);
  }

  @Test
  public void testOnClose() {
    testIntermediate(AsyncDoubleStream::onClose, DoubleStream::onClose, Runnable.class);
  }

  @Test
  public void testClose() {
    DoubleStream stream = mock(DoubleStream.class);
    ExecutorDrivenAsyncDoubleStream asyncStream = new ExecutorDrivenAsyncDoubleStream(stream, mock(Executor.class));

    asyncStream.close();

    verify(stream).close();
  }

  private static void testIntermediate(Function<AsyncDoubleStream, AsyncDoubleStream> asyncMethod, Function<DoubleStream, DoubleStream> streamMethod) {
    Executor executor = mock(Executor.class);
    testDelegation(s -> new ExecutorDrivenAsyncDoubleStream(s, executor), ExecutorDrivenAsyncDoubleStream.class, asyncMethod::apply,
            DoubleStream.class, DoubleStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testIntermediate(BiFunction<AsyncDoubleStream, U, AsyncDoubleStream> asyncMethod, BiFunction<DoubleStream, U, DoubleStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static void testIntIntermediate(Function<AsyncDoubleStream, AsyncIntStream> asyncMethod, Function<DoubleStream, IntStream> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncDoubleStream(s, executor), ExecutorDrivenAsyncIntStream.class, asyncMethod::apply,
            DoubleStream.class, IntStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testIntIntermediate(BiFunction<AsyncDoubleStream, U, AsyncIntStream> asyncMethod, BiFunction<DoubleStream, U, IntStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);

    testIntIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static <U> void testLongIntermediate(Function<AsyncDoubleStream, AsyncLongStream> asyncMethod, Function<DoubleStream, LongStream> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncDoubleStream(s, executor), ExecutorDrivenAsyncLongStream.class, asyncMethod::apply,
            DoubleStream.class, LongStream.class, streamMethod, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testLongIntermediate(BiFunction<AsyncDoubleStream, U, AsyncLongStream> asyncMethod, BiFunction<DoubleStream, U, LongStream> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);

    testLongIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static void testObjIntermediate(Function<AsyncDoubleStream, AsyncStream<?>> asyncMethod, Function<DoubleStream, Stream<?>> streamMethod) {
    Executor executor = mock(Executor.class);

    testDelegation(s -> new ExecutorDrivenAsyncDoubleStream(s, executor), ExecutorDrivenAsyncStream.class, asyncMethod::apply,
            DoubleStream.class, Stream.class, streamMethod::apply, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testObjIntermediate(BiFunction<AsyncDoubleStream, U, AsyncStream<?>> asyncMethod, BiFunction<DoubleStream, U, Stream<?>> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testObjIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static <T> void testTerminal(Class<T> resultType, Function<AsyncDoubleStream, Operation<T>> asyncMethod, Function<DoubleStream, T> streamMethod) {
    testDelegation(s -> new ExecutorDrivenAsyncDoubleStream(s, ForkJoinPool.commonPool()), resultType, asyncMethod.andThen(AbstractExecutorDrivenAsyncTest::retrieve),
            DoubleStream.class, resultType, streamMethod, (asyncOutput, delegateOutput) -> assertThat(asyncOutput, is(delegateOutput)));
  }

  private static <U> void testTerminal(BiFunction<AsyncDoubleStream, U, Operation<Void>> asyncMethod, BiConsumer<DoubleStream, U> streamMethod, Class<U> paramType) {
    U u = createInstance(paramType);
    testTerminal(Void.TYPE, as -> asyncMethod.apply(as, u), s -> { streamMethod.accept(s, u); return null; });
  }

  private static <T, U> void testTerminal(Class<T> resultType, BiFunction<AsyncDoubleStream, U, Operation<T>> asyncMethod, BiFunction<DoubleStream, U, T> streamMethod, Class<U> paramType) {
    U u = createInstance(paramType);
    testTerminal(resultType, as -> asyncMethod.apply(as, u), s -> streamMethod.apply(s, u));
  }

  private static <T, U, V> void testTerminal(Class<T> resultType, TriFunction<AsyncDoubleStream, U, V, Operation<T>> asyncMethod, TriFunction<DoubleStream, U, V, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV) {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v), s -> streamMethod.apply(s, u, v));
  }

  private static <T, U, V, W> void testTerminal(Class<T> resultType, QuadFunction<AsyncDoubleStream, U, V, W, Operation<T>> asyncMethod, QuadFunction<DoubleStream, U, V, W, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV, Class<W> paramTypeW) {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    W w = createInstance(paramTypeW);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v, w), s -> streamMethod.apply(s, u, v, w));
  }

}
