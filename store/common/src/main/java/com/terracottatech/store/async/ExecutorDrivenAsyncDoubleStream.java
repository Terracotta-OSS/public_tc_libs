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

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 *
 * @author cdennis
 */
class ExecutorDrivenAsyncDoubleStream implements AsyncDoubleStream {

  private final DoubleStream stream;
  private final Executor executor;

  public ExecutorDrivenAsyncDoubleStream(DoubleStream stream, Executor executor) {
    this.stream = stream;
    this.executor = executor;
  }

  protected Executor getExecutor() {
    return executor;
  }

  protected DoubleStream getStream() {
    return stream;
  }

  @Override
  public AsyncDoubleStream filter(DoublePredicate predicate) {
    return new ExecutorDrivenAsyncDoubleStream(stream.filter(predicate), executor);
  }

  @Override
  public AsyncDoubleStream map(DoubleUnaryOperator mapper) {
    return new ExecutorDrivenAsyncDoubleStream(stream.map(mapper), executor);
  }

  @Override
  public <U> AsyncStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
    return new ExecutorDrivenAsyncStream<>(this.stream.mapToObj(mapper), executor);
  }

  @Override
  public AsyncIntStream mapToInt(DoubleToIntFunction mapper) {
    return new ExecutorDrivenAsyncIntStream(stream.mapToInt(mapper), executor);
  }

  @Override
  public AsyncLongStream mapToLong(DoubleToLongFunction mapper) {
    return new ExecutorDrivenAsyncLongStream(stream.mapToLong(mapper), executor);
  }

  @Override
  public AsyncDoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
    return new ExecutorDrivenAsyncDoubleStream(stream.flatMap(mapper), executor);
  }

  @Override
  public AsyncDoubleStream distinct() {
    return new ExecutorDrivenAsyncDoubleStream(stream.distinct(), executor);
  }

  @Override
  public AsyncDoubleStream sorted() {
    return new ExecutorDrivenAsyncDoubleStream(stream.sorted(), executor);
  }

  @Override
  public AsyncDoubleStream peek(DoubleConsumer action) {
    return new ExecutorDrivenAsyncDoubleStream(stream.peek(action), executor);
  }

  @Override
  public AsyncDoubleStream limit(long maxSize) {
    return new ExecutorDrivenAsyncDoubleStream(stream.limit(maxSize), executor);
  }

  @Override
  public AsyncDoubleStream skip(long n) {
    return new ExecutorDrivenAsyncDoubleStream(stream.skip(n), executor);
  }

  @Override
  public Operation<Void> forEach(DoubleConsumer action) {
    return operation(runAsync(() -> stream.forEach(action), executor));
  }

  @Override
  public Operation<Void> forEachOrdered(DoubleConsumer action) {
    return operation(runAsync(() -> stream.forEachOrdered(action), executor));
  }

  @Override
  public Operation<double[]> toArray() {
    return operation(supplyAsync(stream::toArray, executor));
  }

  @Override
  public Operation<Double> reduce(double identity, DoubleBinaryOperator op) {
    return operation(supplyAsync(() -> stream.reduce(identity, op), executor));
  }

  @Override
  public Operation<OptionalDouble> reduce(DoubleBinaryOperator op) {
    return operation(supplyAsync(() -> stream.reduce(op), executor));
  }

  @Override
  public <R> Operation<R> collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    return operation(supplyAsync(() -> stream.collect(supplier, accumulator, combiner), executor));
  }

  @Override
  public Operation<Double> sum() {
    return operation(supplyAsync(stream::sum, executor));
  }

  @Override
  public Operation<OptionalDouble> min() {
    return operation(supplyAsync(stream::min, executor));
  }

  @Override
  public Operation<OptionalDouble> max() {
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
  public Operation<DoubleSummaryStatistics> summaryStatistics() {
    return operation(supplyAsync(stream::summaryStatistics, executor));
  }

  @Override
  public Operation<Boolean> anyMatch(DoublePredicate predicate) {
    return operation(supplyAsync(() -> stream.anyMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> allMatch(DoublePredicate predicate) {
    return operation(supplyAsync(() -> stream.allMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> noneMatch(DoublePredicate predicate) {
    return operation(supplyAsync(() -> stream.noneMatch(predicate), executor));
  }

  @Override
  public Operation<OptionalDouble> findFirst() {
    return operation(supplyAsync(stream::findFirst, executor));
  }

  @Override
  public Operation<OptionalDouble> findAny() {
    return operation(supplyAsync(stream::findAny, executor));
  }

  @Override
  public AsyncStream<Double> boxed() {
    return new ExecutorDrivenAsyncStream<>(stream.boxed(), executor);
  }

  @Override
  public AsyncDoubleStream sequential() {
    return new ExecutorDrivenAsyncDoubleStream(stream.sequential(), executor);
  }

  @Override
  public AsyncDoubleStream parallel() {
    return new ExecutorDrivenAsyncDoubleStream(stream.parallel(), executor);
  }

  @Override
  public PrimitiveIterator.OfDouble iterator() {
    return stream.iterator();
  }

  @Override
  public Spliterator.OfDouble spliterator() {
    return stream.spliterator();
  }

  @Override
  public boolean isParallel() {
    return stream.isParallel();
  }

  @Override
  public AsyncDoubleStream unordered() {
    return new ExecutorDrivenAsyncDoubleStream(stream.unordered(), executor);
  }

  @Override
  public AsyncDoubleStream onClose(Runnable closeHandler) {
    return new ExecutorDrivenAsyncDoubleStream(stream.onClose(closeHandler), executor);
  }

  @Override
  public void close() {
    stream.close();
  }
}
