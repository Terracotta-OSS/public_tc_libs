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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.Executor;
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

import static com.terracottatech.store.async.Operation.operation;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 *
 * @author cdennis
 */
class ExecutorDrivenAsyncStream<T> implements AsyncStream<T> {

  private final Stream<T> stream;
  private final Executor executor;

  ExecutorDrivenAsyncStream(Stream<T> stream, Executor executor) {
    this.stream = stream;
    this.executor = executor;
  }

  protected Executor getExecutor() {
    return executor;
  }

  protected Stream<T> getStream() {
    return stream;
  }

  @Override
  public AsyncStream<T> filter(Predicate<? super T> predicate) {
    return new ExecutorDrivenAsyncStream<>(stream.filter(predicate), executor);
  }

  @Override
  public <R> AsyncStream<R> map(Function<? super T, ? extends R> mapper) {
    return new ExecutorDrivenAsyncStream<>(stream.map(mapper), executor);
  }

  @Override
  public AsyncIntStream mapToInt(ToIntFunction<? super T> mapper) {
    return new ExecutorDrivenAsyncIntStream(stream.mapToInt(mapper), executor);
  }

  @Override
  public AsyncLongStream mapToLong(ToLongFunction<? super T> mapper) {
    return new ExecutorDrivenAsyncLongStream(stream.mapToLong(mapper), executor);
  }

  @Override
  public AsyncDoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    return new ExecutorDrivenAsyncDoubleStream(stream.mapToDouble(mapper), executor);
  }

  @Override
  public <R> AsyncStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    return new ExecutorDrivenAsyncStream<>(stream.flatMap(mapper), executor);
  }

  @Override
  public AsyncIntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    return new ExecutorDrivenAsyncIntStream(stream.flatMapToInt(mapper), executor);
  }

  @Override
  public AsyncLongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    return new ExecutorDrivenAsyncLongStream(stream.flatMapToLong(mapper), executor);
  }

  @Override
  public AsyncDoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    return new ExecutorDrivenAsyncDoubleStream(stream.flatMapToDouble(mapper), executor);
  }

  @Override
  public AsyncStream<T> distinct() {
    return new ExecutorDrivenAsyncStream<>(stream.distinct(), executor);
  }

  @Override
  public AsyncStream<T> sorted() {
    return new ExecutorDrivenAsyncStream<>(stream.sorted(), executor);
  }

  @Override
  public AsyncStream<T> sorted(Comparator<? super T> comparator) {
    return new ExecutorDrivenAsyncStream<>(stream.sorted(comparator), executor);
  }

  @Override
  public AsyncStream<T> peek(Consumer<? super T> action) {
    return new ExecutorDrivenAsyncStream<>(stream.peek(action), executor);
  }

  @Override
  public AsyncStream<T> limit(long maxSize) {
    return new ExecutorDrivenAsyncStream<>(stream.limit(maxSize), executor);
  }

  @Override
  public AsyncStream<T> skip(long n) {
    return new ExecutorDrivenAsyncStream<>(stream.skip(n), executor);
  }

  @Override
  public Operation<Void> forEach(Consumer<? super T> action) {
    return operation(runAsync(() -> stream.forEach(action), executor));
  }

  @Override
  public Operation<Void> forEachOrdered(Consumer<? super T> action) {
    return operation(runAsync(() -> stream.forEachOrdered(action), executor));
  }

  @Override
  public Operation<Object[]> toArray() {
    return operation(supplyAsync(() -> stream.toArray(), executor));
  }

  @Override
  public <A> Operation<A[]> toArray(IntFunction<A[]> generator) {
    return operation(supplyAsync(() -> stream.toArray(generator), executor));
  }

  @Override
  public Operation<T> reduce(T identity, BinaryOperator<T> accumulator) {
    return operation(supplyAsync(() -> stream.reduce(identity, accumulator), executor));
  }

  @Override
  public Operation<Optional<T>> reduce(BinaryOperator<T> accumulator) {
    return operation(supplyAsync(() -> stream.reduce(accumulator), executor));
  }

  @Override
  public <U> Operation<U> reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
    return operation(supplyAsync(() -> stream.reduce(identity, accumulator, combiner), executor));
  }

  @Override
  public <R> Operation<R> collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
    return operation(supplyAsync(() -> stream.collect(supplier, accumulator, combiner), executor));
  }

  @Override
  public <R, A> Operation<R> collect(Collector<? super T, A, R> collector) {
    return operation(supplyAsync(() -> stream.collect(collector), executor));
  }

  @Override
  public Operation<Optional<T>> min(Comparator<? super T> comparator) {
    return operation(supplyAsync(() -> stream.min(comparator), executor));
  }

  @Override
  public Operation<Optional<T>> max(Comparator<? super T> comparator) {
    return operation(supplyAsync(() -> stream.max(comparator), executor));
  }

  @Override
  public Operation<Long> count() {
    return operation(supplyAsync(() -> stream.count(), executor));
  }

  @Override
  public Operation<Boolean> anyMatch(Predicate<? super T> predicate) {
    return operation(supplyAsync(() -> stream.anyMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> allMatch(Predicate<? super T> predicate) {
    return operation(supplyAsync(() -> stream.allMatch(predicate), executor));
  }

  @Override
  public Operation<Boolean> noneMatch(Predicate<? super T> predicate) {
    return operation(supplyAsync(() -> stream.noneMatch(predicate), executor));
  }

  @Override
  public Operation<Optional<T>> findFirst() {
    return operation(supplyAsync(() -> stream.findFirst(), executor));
  }

  @Override
  public Operation<Optional<T>> findAny() {
    return operation(supplyAsync(() -> stream.findAny(), executor));
  }

  @Override
  public Iterator<T> iterator() {
    return stream.iterator();
  }

  @Override
  public Spliterator<T> spliterator() {
    return stream.spliterator();
  }

  @Override
  public boolean isParallel() {
    return stream.isParallel();
  }

  @Override
  public AsyncStream<T> sequential() {
    return new ExecutorDrivenAsyncStream<>(stream.sequential(), executor);
  }

  @Override
  public AsyncStream<T> parallel() {
    return new ExecutorDrivenAsyncStream<>(stream.parallel(), executor);
  }

  @Override
  public AsyncStream<T> unordered() {
    return new ExecutorDrivenAsyncStream<>(stream.unordered(), executor);
  }

  @Override
  public AsyncStream<T> onClose(Runnable closeHandler) {
    return new ExecutorDrivenAsyncStream<>(stream.onClose(closeHandler), executor);
  }

  @Override
  public void close() {
    stream.close();
  }
}
