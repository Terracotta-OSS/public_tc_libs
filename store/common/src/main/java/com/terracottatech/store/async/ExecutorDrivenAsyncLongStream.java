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

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 *
 * @author cdennis
 */
class ExecutorDrivenAsyncLongStream implements AsyncLongStream {

  private final LongStream stream;
  private final Executor executor;

  public ExecutorDrivenAsyncLongStream(LongStream stream, Executor executor) {
    this.stream = stream;
    this.executor = executor;
  }

  protected Executor getExecutor() {
    return executor;
  }

  protected LongStream getStream() {
    return stream;
  }

  @Override
  public AsyncLongStream filter(LongPredicate predicate) {
    return new ExecutorDrivenAsyncLongStream(stream.filter(predicate), executor);
  }

  @Override
  public AsyncLongStream map(LongUnaryOperator mapper) {
    return new ExecutorDrivenAsyncLongStream(stream.map(mapper), executor);
  }

  @Override
  public <U> AsyncStream<U> mapToObj(LongFunction<? extends U> mapper) {
    return new ExecutorDrivenAsyncStream<>(stream.mapToObj(mapper), executor);
  }

  @Override
  public AsyncIntStream mapToInt(LongToIntFunction mapper) {
    return new ExecutorDrivenAsyncIntStream(stream.mapToInt(mapper), executor);
  }

  @Override
  public AsyncDoubleStream mapToDouble(LongToDoubleFunction mapper) {
    return new ExecutorDrivenAsyncDoubleStream(stream.mapToDouble(mapper), executor);
  }

  @Override
  public AsyncLongStream flatMap(LongFunction<? extends LongStream> mapper) {
    return new ExecutorDrivenAsyncLongStream(stream.flatMap(mapper), executor);
  }

  @Override
  public AsyncLongStream distinct() {
    return new ExecutorDrivenAsyncLongStream(stream.distinct(), executor);
  }

  @Override
  public AsyncLongStream sorted() {
    return new ExecutorDrivenAsyncLongStream(stream.sorted(), executor);
  }

  @Override
  public AsyncLongStream peek(LongConsumer action) {
    return new ExecutorDrivenAsyncLongStream(stream.peek(action), executor);
  }

  @Override
  public AsyncLongStream limit(long maxSize) {
    return new ExecutorDrivenAsyncLongStream(stream.limit(maxSize), executor);
  }

  @Override
  public AsyncLongStream skip(long n) {
    return new ExecutorDrivenAsyncLongStream(stream.skip(n), executor);
  }

  @Override
  public Operation<Void> forEach(LongConsumer action) {
    return operation(runAsync(() -> stream.forEach(action), executor));
  }

  @Override
  public Operation<Void> forEachOrdered(LongConsumer action) {
    return operation(runAsync(() -> stream.forEachOrdered(action), executor));
  }

  @Override
  public Operation<long[]> toArray() {
    return operation(supplyAsync(stream::toArray, executor));
  }

  @Override
  public Operation<Long> reduce(long identity, LongBinaryOperator op) {
    return operation(supplyAsync(() -> stream.reduce(identity, op), executor));
  }

  @Override
  public Operation<OptionalLong> reduce(LongBinaryOperator op) {
    return operation(supplyAsync(() -> stream.reduce(op), executor));
  }

  @Override
  public <R> Operation<R> collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    return operation(supplyAsync(() -> stream.collect(supplier, accumulator, combiner), executor));
  }

  @Override
  public Operation<Long> sum() {
    return operation(supplyAsync(stream::sum, executor));
  }

  @Override
  public Operation<OptionalLong> min() {
    return operation(supplyAsync(stream::min, executor));
  }

  @Override
  public Operation<OptionalLong> max() {
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
  public Operation<LongSummaryStatistics> summaryStatistics() {
    return operation(supplyAsync(stream::summaryStatistics, executor));
  }

  @Override
  public Operation<Boolean> anyMatch(LongPredicate predicate) {
    return operation(supplyAsync(() -> stream.anyMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> allMatch(LongPredicate predicate) {
    return operation(supplyAsync(() -> stream.allMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> noneMatch(LongPredicate predicate) {
    return operation(supplyAsync(() -> stream.noneMatch(predicate), executor));
  }

  @Override
  public Operation<OptionalLong> findFirst() {
    return operation(supplyAsync(stream::findFirst, executor));
  }

  @Override
  public Operation<OptionalLong> findAny() {
    return operation(supplyAsync(stream::findAny, executor));
  }

  @Override
  public AsyncDoubleStream asDoubleStream() {
    return new ExecutorDrivenAsyncDoubleStream(stream.asDoubleStream(), executor);
  }

  @Override
  public AsyncStream<Long> boxed() {
    return new ExecutorDrivenAsyncStream<>(stream.boxed(), executor);
  }

  @Override
  public AsyncLongStream sequential() {
    return new ExecutorDrivenAsyncLongStream(stream.sequential(), executor);
  }

  @Override
  public AsyncLongStream parallel() {
    return new ExecutorDrivenAsyncLongStream(stream.parallel(), executor);
  }

  @Override
  public PrimitiveIterator.OfLong iterator() {
    return stream.iterator();
  }

  @Override
  public Spliterator.OfLong spliterator() {
    return stream.spliterator();
  }

  @Override
  public boolean isParallel() {
    return stream.isParallel();
  }

  @Override
  public AsyncLongStream unordered() {
    return new ExecutorDrivenAsyncLongStream(stream.unordered(), executor);
  }

  @Override
  public AsyncLongStream onClose(Runnable closeHandler) {
    return new ExecutorDrivenAsyncLongStream(stream.onClose(closeHandler), executor);
  }

  @Override
  public void close() {
    stream.close();
  }
}
