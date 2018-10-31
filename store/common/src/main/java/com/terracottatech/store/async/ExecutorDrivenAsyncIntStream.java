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


import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 *
 * @author cdennis
 */
class ExecutorDrivenAsyncIntStream implements AsyncIntStream {

  private final IntStream stream;
  private final Executor executor;

  public ExecutorDrivenAsyncIntStream(IntStream stream, Executor executor) {
    this.stream = stream;
    this.executor = executor;
  }

  protected Executor getExecutor() {
    return executor;
  }

  protected IntStream getStream() {
    return stream;
  }

  @Override
  public AsyncIntStream filter(IntPredicate predicate) {
    return new ExecutorDrivenAsyncIntStream(stream.filter(predicate), executor);
  }

  @Override
  public AsyncIntStream map(IntUnaryOperator mapper) {
    return new ExecutorDrivenAsyncIntStream(stream.map(mapper), executor);
  }

  @Override
  public <U> AsyncStream<U> mapToObj(IntFunction<? extends U> mapper) {
    return new ExecutorDrivenAsyncStream<>(stream.mapToObj(mapper), executor);
  }

  @Override
  public AsyncLongStream mapToLong(IntToLongFunction mapper) {
    return new ExecutorDrivenAsyncLongStream(stream.mapToLong(mapper), executor);
  }

  @Override
  public AsyncDoubleStream mapToDouble(IntToDoubleFunction mapper) {
    return new ExecutorDrivenAsyncDoubleStream(stream.mapToDouble(mapper), executor);
  }

  @Override
  public AsyncIntStream flatMap(IntFunction<? extends IntStream> mapper) {
    return new ExecutorDrivenAsyncIntStream(stream.flatMap(mapper), executor);
  }

  @Override
  public AsyncIntStream distinct() {
    return new ExecutorDrivenAsyncIntStream(stream.distinct(), executor);
  }

  @Override
  public AsyncIntStream sorted() {
    return new ExecutorDrivenAsyncIntStream(stream.sorted(), executor);
  }

  @Override
  public AsyncIntStream peek(IntConsumer action) {
    return new ExecutorDrivenAsyncIntStream(stream.peek(action), executor);
  }

  @Override
  public AsyncIntStream limit(long maxSize) {
    return new ExecutorDrivenAsyncIntStream(stream.limit(maxSize), executor);
  }

  @Override
  public AsyncIntStream skip(long n) {
    return new ExecutorDrivenAsyncIntStream(stream.skip(n), executor);
  }

  @Override
  public Operation<Void> forEach(IntConsumer action) {
    return operation(runAsync(() -> stream.forEach(action), executor));
  }

  @Override
  public Operation<Void> forEachOrdered(IntConsumer action) {
    return operation(runAsync(() -> stream.forEachOrdered(action), executor));
  }

  @Override
  public Operation<int[]> toArray() {
    return operation(supplyAsync(stream::toArray, executor));
  }

  @Override
  public Operation<Integer> reduce(int identity, IntBinaryOperator op) {
    return operation(supplyAsync(() -> stream.reduce(identity, op), executor));
  }

  @Override
  public Operation<OptionalInt> reduce(IntBinaryOperator op) {
    return operation(supplyAsync(() -> stream.reduce(op), executor));
  }

  @Override
  public <R> Operation<R> collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    return operation(supplyAsync(() -> stream.collect(supplier, accumulator, combiner), executor));
  }

  @Override
  public Operation<Integer> sum() {
    return operation(supplyAsync(stream::sum, executor));
  }

  @Override
  public Operation<OptionalInt> min() {
    return operation(supplyAsync(stream::min, executor));
  }

  @Override
  public Operation<OptionalInt> max() {
    return operation(supplyAsync(stream::max, executor));
  }

  @Override
  public Operation<Long> count() {
    return operation(supplyAsync(stream::count, executor));
  }

  @Override
  public Operation<OptionalDouble> average() {
    return operation(supplyAsync(stream::average, executor));
  }

  @Override
  public Operation<IntSummaryStatistics> summaryStatistics() {
    return operation(supplyAsync(stream::summaryStatistics, executor));
  }

  @Override
  public Operation<Boolean> anyMatch(IntPredicate predicate) {
    return operation(supplyAsync(() -> stream.anyMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> allMatch(IntPredicate predicate) {
    return operation(supplyAsync(() -> stream.allMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> noneMatch(IntPredicate predicate) {
    return operation(supplyAsync(() -> stream.noneMatch(predicate), executor));
  }

  @Override
  public Operation<OptionalInt> findFirst() {
    return operation(supplyAsync(stream::findFirst, executor));
  }

  @Override
  public Operation<OptionalInt> findAny() {
    return operation(supplyAsync(stream::findAny, executor));
  }

  @Override
  public AsyncLongStream asLongStream() {
    return new ExecutorDrivenAsyncLongStream(stream.asLongStream(), executor);
  }

  @Override
  public AsyncDoubleStream asDoubleStream() {
    return new ExecutorDrivenAsyncDoubleStream(stream.asDoubleStream(), executor);
  }

  @Override
  public AsyncStream<Integer> boxed() {
    return new ExecutorDrivenAsyncStream<>(stream.boxed(), executor);
  }

  @Override
  public AsyncIntStream sequential() {
    return new ExecutorDrivenAsyncIntStream(stream.sequential(), executor);
  }

  @Override
  public AsyncIntStream parallel() {
    return new ExecutorDrivenAsyncIntStream(stream.parallel(), executor);
  }

  @Override
  public PrimitiveIterator.OfInt iterator() {
    return stream.iterator();
  }

  @Override
  public Spliterator.OfInt spliterator() {
    return stream.spliterator();
  }

  @Override
  public boolean isParallel() {
    return stream.isParallel();
  }

  @Override
  public AsyncIntStream unordered() {
    return new ExecutorDrivenAsyncIntStream(stream.unordered(), executor);
  }

  @Override
  public AsyncIntStream onClose(Runnable closeHandler) {
    return new ExecutorDrivenAsyncIntStream(stream.onClose(closeHandler), executor);
  }

  @Override
  public void close() {
    stream.close();
  }
}
