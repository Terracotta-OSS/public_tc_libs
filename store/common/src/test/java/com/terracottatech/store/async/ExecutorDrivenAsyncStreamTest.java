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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class ExecutorDrivenAsyncStreamTest extends AbstractExecutorDrivenAsyncTest {

  @Test
  public void testFilter() {
    testIntermediate(AsyncStream::filter, Stream::filter, Predicate.class);
  }

  @Test
  public void testMap() {
    testIntermediate(AsyncStream::map, Stream::map, Function.class);
  }

  @Test
  public void testMapToInt() {
    testIntIntermediate(AsyncStream::mapToInt, Stream::mapToInt, ToIntFunction.class);
  }

  @Test
  public void testMapToLong() {
    testLongIntermediate(AsyncStream::mapToLong, Stream::mapToLong, ToLongFunction.class);
  }

  @Test
  public void testMapToDouble() {
    testDoubleIntermediate(AsyncStream::mapToDouble, Stream::mapToDouble, ToDoubleFunction.class);
  }

  @Test
  public void testFlatMap() {
    testIntermediate(AsyncStream::flatMap, Stream::flatMap, Function.class);
  }

  @Test
  public void testFlatMapToInt() {
    testIntIntermediate(AsyncStream::flatMapToInt, Stream::flatMapToInt, Function.class);
  }

  @Test
  public void testFlatMapToLong() {
    testLongIntermediate(AsyncStream::flatMapToLong, Stream::flatMapToLong, Function.class);
  }

  @Test
  public void testFlatMapToDouble() {
    testDoubleIntermediate(AsyncStream::flatMapToDouble, Stream::flatMapToDouble, Function.class);
  }

  @Test
  public void testDistinct() {
    testIntermediate(AsyncStream::distinct, Stream::distinct);
  }

  @Test
  public void testSorted() {
    testIntermediate(AsyncStream::sorted, Stream::sorted);
  }

  @Test
  public void testSortedWithComparator() {
    testIntermediate(AsyncStream::sorted, Stream::sorted, Comparator.class);
  }

  @Test
  public void testPeek() {
    testIntermediate(AsyncStream::peek, Stream::peek, Consumer.class);
  }

  @Test
  public void testLimit() {
    testIntermediate(AsyncStream::limit, Stream::limit, Long.TYPE);
  }

  @Test
  public void testSkip() {
    testIntermediate(AsyncStream::skip, Stream::skip, Long.TYPE);
  }

  @Test
  public void testForEach() throws ExecutionException, InterruptedException {
    testTerminal(AsyncStream::forEach, Stream::forEach, Consumer.class);
  }

  @Test
  public void testForEachOrdered() throws ExecutionException, InterruptedException {
    testTerminal(AsyncStream::forEachOrdered, Stream::forEachOrdered, Consumer.class);
  }

  @Test
  public void testToArray() throws ExecutionException, InterruptedException {
    testTerminal(Object[].class, AsyncStream::toArray, Stream::toArray);
  }

  @Test
  public void testToArrayWithSupplier() throws ExecutionException, InterruptedException {
    testTerminal(Object[].class, AsyncStream::toArray, Stream::toArray, IntFunction.class);
  }

  @Test
  public void testTwoArgReduce() throws ExecutionException, InterruptedException {
    testTerminal(String.class, AsyncStream::reduce, Stream::reduce, String.class, BinaryOperator.class);
  }

  @Test
  public void testOneArgReduce() throws ExecutionException, InterruptedException {
    testTerminal((Class<Optional<String>>) (Class) Optional.class, AsyncStream::reduce, Stream::reduce, BinaryOperator.class);
  }

  @Test
  public void testThreeArgReduce() throws ExecutionException, InterruptedException {
    testTerminal(String.class, AsyncStream::reduce, Stream::reduce, String.class, BiFunction.class, BinaryOperator.class);
  }

  @Test
  public void testThreeArgCollect() throws ExecutionException, InterruptedException {
    testTerminal(Object.class, AsyncStream::collect, Stream::collect, Supplier.class, BiConsumer.class, BiConsumer.class);
  }

  @Test
  public void testCollect() throws ExecutionException, InterruptedException {
    testTerminal(Object.class, AsyncStream::collect, Stream::collect, Collector.class);
  }

  @Test
  public void testMin() throws ExecutionException, InterruptedException {
    testTerminal((Class<Optional<String>>) (Class) Optional.class, AsyncStream::min, Stream::min, Comparator.class);
  }

  @Test
  public void testMax() throws ExecutionException, InterruptedException {
    testTerminal((Class<Optional<String>>) (Class) Optional.class, AsyncStream::max, Stream::max, Comparator.class);
  }

  @Test
  public void testCount() throws ExecutionException, InterruptedException {
    testTerminal(Long.class, AsyncStream::count, Stream::count);
  }

  @Test
  public void testAnyMatch() throws ExecutionException, InterruptedException {
    testTerminal(Boolean.class, AsyncStream::anyMatch, Stream::anyMatch, Predicate.class);
  }

  @Test
  public void testAllMatch() throws ExecutionException, InterruptedException {
    testTerminal(Boolean.class, AsyncStream::allMatch, Stream::allMatch, Predicate.class);
  }

  @Test
  public void testNoneMatch() throws ExecutionException, InterruptedException {
    testTerminal(Boolean.class, AsyncStream::noneMatch, Stream::noneMatch, Predicate.class);
  }

  @Test
  public void testFindFirst() throws ExecutionException, InterruptedException {
    testTerminal((Class<Optional<String>>) (Class) Optional.class, AsyncStream::findFirst, Stream::findFirst);
  }

  @Test
  public void testFindAny() throws ExecutionException, InterruptedException {
    testTerminal((Class<Optional<String>>) (Class) Optional.class, AsyncStream::findAny, Stream::findAny);
  }

  @Test
  public void testIterator() {
    Stream<String> stream = mock(Stream.class);
    Iterator<String> iterator = mock(Iterator.class);
    when(stream.iterator()).thenReturn(iterator);
    ExecutorDrivenAsyncStream<String> asyncStream = new ExecutorDrivenAsyncStream<>(stream, mock(Executor.class));

    assertThat(asyncStream.iterator(), is(iterator));
    verify(stream).iterator();
  }

  @Test
  public void testSpliterator() {
    Stream<String> stream = mock(Stream.class);
    Spliterator<String> spliterator = mock(Spliterator.class);
    when(stream.spliterator()).thenReturn(spliterator);
    ExecutorDrivenAsyncStream<String> asyncStream = new ExecutorDrivenAsyncStream<>(stream, mock(Executor.class));

    assertThat(asyncStream.spliterator(), is(spliterator));
    verify(stream).spliterator();
  }

  @Test
  public void testIsParallel() {
    Stream<String> stream = mock(Stream.class);
    when(stream.isParallel()).thenReturn(true);
    ExecutorDrivenAsyncStream<String> asyncStream = new ExecutorDrivenAsyncStream<>(stream, mock(Executor.class));

    assertThat(asyncStream.isParallel(), is(true));
    verify(stream).isParallel();
  }

  @Test
  public void testSequential() {
    testIntermediate(AsyncStream::sequential, Stream::sequential);
  }

  @Test
  public void testParallel() {
    testIntermediate(AsyncStream::parallel, Stream::parallel);
  }

  @Test
  public void testUnordered() {
    testIntermediate(AsyncStream::unordered, Stream::unordered);
  }

  @Test
  public void testOnClose() {
    testIntermediate(AsyncStream::onClose, Stream::onClose, Runnable.class);
  }

  @Test
  public void testClose() {
    Stream<String> stream = mock(Stream.class);
    ExecutorDrivenAsyncStream<String> asyncStream = new ExecutorDrivenAsyncStream<>(stream, mock(Executor.class));

    asyncStream.close();

    verify(stream).close();
  }

  private static <T> void testIntermediate(Function<AsyncStream<String>, AsyncStream<T>> asyncMethod, Function<Stream<String>, Stream<T>> streamMethod) {
    Executor executor = mock(Executor.class);
    testDelegation(s -> new ExecutorDrivenAsyncStream<String>(s, executor), ExecutorDrivenAsyncStream.class, asyncMethod::apply,
            Stream.class, Stream.class, streamMethod::apply, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <T, U> void testIntermediate(BiFunction<AsyncStream<String>, U, AsyncStream<T>> asyncMethod, BiFunction<Stream<String>, U, Stream<T>> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }

  private static <U> void testIntIntermediate(BiFunction<AsyncStream<String>, U, AsyncIntStream> asyncMethod, BiFunction<Stream<String>, U, IntStream> streamMethod, Class<U> paramType) {
    Executor executor = mock(Executor.class);
    U parameter = createInstance(paramType);

    testDelegation(s -> new ExecutorDrivenAsyncStream<>(s, executor), ExecutorDrivenAsyncIntStream.class, as -> asyncMethod.apply(as, parameter),
            Stream.class, IntStream.class, s -> streamMethod.apply(s, parameter), (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testLongIntermediate(BiFunction<AsyncStream<String>, U, AsyncLongStream> asyncMethod, BiFunction<Stream<String>, U, LongStream> streamMethod, Class<U> paramType) {
    Executor executor = mock(Executor.class);
    U parameter = createInstance(paramType);

    testDelegation(s -> new ExecutorDrivenAsyncStream<>(s, executor), ExecutorDrivenAsyncLongStream.class, as -> asyncMethod.apply(as, parameter),
            Stream.class, LongStream.class, s -> streamMethod.apply(s, parameter), (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testDoubleIntermediate(BiFunction<AsyncStream<String>, U, AsyncDoubleStream> asyncMethod, BiFunction<Stream<String>, U, DoubleStream> streamMethod, Class<U> paramType) {
    Executor executor = mock(Executor.class);
    U parameter = createInstance(paramType);

    testDelegation(s -> new ExecutorDrivenAsyncStream<>(s, executor), ExecutorDrivenAsyncDoubleStream.class, as -> asyncMethod.apply(as, parameter),
            Stream.class, DoubleStream.class, s -> streamMethod.apply(s, parameter), (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <T> void testTerminal(Class<T> resultType, Function<AsyncStream<String>, Operation<T>> asyncMethod, Function<Stream<String>, T> streamMethod) throws ExecutionException, InterruptedException {
    testDelegation(s -> new ExecutorDrivenAsyncStream<>(s, ForkJoinPool.commonPool()), resultType, asyncMethod.andThen(AbstractExecutorDrivenAsyncTest::retrieve),
            (Class<Stream<String>>) (Class) Stream.class, resultType, streamMethod, (asyncOutput, delegateOutput) -> assertThat(asyncOutput, is(delegateOutput)));
  }

  private static <U> void testTerminal(BiFunction<AsyncStream<String>, U, Operation<Void>> asyncMethod, BiConsumer<Stream<String>, U> streamMethod, Class<U> paramType) throws ExecutionException, InterruptedException {
    U u = createInstance(paramType);
    testTerminal(Void.TYPE, as -> asyncMethod.apply(as, u), s -> { streamMethod.accept(s, u); return null; });
  }

  private static <T, U> void testTerminal(Class<T> resultType, BiFunction<AsyncStream<String>, U, Operation<T>> asyncMethod, BiFunction<Stream<String>, U, T> streamMethod, Class<U> paramType) throws ExecutionException, InterruptedException {
    U u = createInstance(paramType);
    testTerminal(resultType, as -> asyncMethod.apply(as, u), s -> streamMethod.apply(s, u));
  }

  private static <T, U, V> void testTerminal(Class<T> resultType, TriFunction<AsyncStream<String>, U, V, Operation<T>> asyncMethod, TriFunction<Stream<String>, U, V, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV) throws ExecutionException, InterruptedException {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v), s -> streamMethod.apply(s, u, v));
  }

  private static <T, U, V, W> void testTerminal(Class<T> resultType, QuadFunction<AsyncStream<String>, U, V, W, Operation<T>> asyncMethod, QuadFunction<Stream<String>, U, V, W, T> streamMethod, Class<U> paramTypeU, Class<V> paramTypeV, Class<W> paramTypeW) throws ExecutionException, InterruptedException {
    U u = createInstance(paramTypeU);
    V v = createInstance(paramTypeV);
    W w = createInstance(paramTypeW);
    testTerminal(resultType, as -> asyncMethod.apply(as, u, v, w), s -> streamMethod.apply(s, u, v, w));
  }
}
