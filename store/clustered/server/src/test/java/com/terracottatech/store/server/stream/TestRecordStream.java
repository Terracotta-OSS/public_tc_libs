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
package com.terracottatech.store.server.stream;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.plan.StreamPlan;
import com.terracottatech.store.Record;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
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

/**
 * A test version of {@link RecordStream} that delegates all calls directly to a wrapped
 * {@link Stream Stream<Record<K>>}.
 */
public class TestRecordStream<K extends Comparable<K>> implements RecordStream<K> {
  private final Stream<Record<K>> delegate;

  public TestRecordStream(Stream<Record<K>> delegate) {
    this.delegate = delegate;
  }

  private RecordStream<K> wrap(Stream<Record<K>> stream) {
    return new TestRecordStream<>(stream);
  }

  @Override
  public Stream<Record<K>> filter(Predicate<? super Record<K>> predicate) {
    return delegate.filter(predicate);
  }

  @Override
  public <R> Stream<R> map(Function<? super Record<K>, ? extends R> mapper) {
    return delegate.map(mapper);
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super Record<K>> mapper) {
    return delegate.mapToInt(mapper);
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super Record<K>> mapper) {
    return delegate.mapToLong(mapper);
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super Record<K>> mapper) {
    return delegate.mapToDouble(mapper);
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super Record<K>, ? extends Stream<? extends R>> mapper) {
    return delegate.flatMap(mapper);
  }

  @Override
  public IntStream flatMapToInt(Function<? super Record<K>, ? extends IntStream> mapper) {
    return delegate.flatMapToInt(mapper);
  }

  @Override
  public LongStream flatMapToLong(Function<? super Record<K>, ? extends LongStream> mapper) {
    return delegate.flatMapToLong(mapper);
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super Record<K>, ? extends DoubleStream> mapper) {
    return delegate.flatMapToDouble(mapper);
  }

  @Override
  public Stream<Record<K>> distinct() {
    return delegate.distinct();
  }

  @Override
  public Stream<Record<K>> sorted() {
    return delegate.sorted();
  }

  @Override
  public Stream<Record<K>> sorted(Comparator<? super Record<K>> comparator) {
    return delegate.sorted(comparator);
  }

  @Override
  public Stream<Record<K>> peek(Consumer<? super Record<K>> action) {
    return delegate.peek(action);
  }

  @Override
  public Stream<Record<K>> limit(long maxSize) {
    return delegate.limit(maxSize);
  }

  @Override
  public Stream<Record<K>> skip(long n) {
    return delegate.skip(n);
  }

  @Override
  public void forEach(Consumer<? super Record<K>> action) {
    delegate.forEach(action);
  }

  @Override
  public void forEachOrdered(Consumer<? super Record<K>> action) {
    delegate.forEachOrdered(action);
  }

  @Override
  public Object[] toArray() {
    return delegate.toArray();
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    return delegate.toArray(generator);
  }

  @Override
  public Record<K> reduce(Record<K> identity, BinaryOperator<Record<K>> accumulator) {
    return delegate.reduce(identity, accumulator);
  }

  @Override
  public Optional<Record<K>> reduce(BinaryOperator<Record<K>> accumulator) {
    return delegate.reduce(accumulator);
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super Record<K>, U> accumulator, BinaryOperator<U> combiner) {
    return delegate.reduce(identity, accumulator, combiner);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super Record<K>> accumulator, BiConsumer<R, R> combiner) {
    return delegate.collect(supplier, accumulator, combiner);
  }

  @Override
  public <R, A> R collect(Collector<? super Record<K>, A, R> collector) {
    return delegate.collect(collector);
  }

  @Override
  public Optional<Record<K>> min(Comparator<? super Record<K>> comparator) {
    return delegate.min(comparator);
  }

  @Override
  public Optional<Record<K>> max(Comparator<? super Record<K>> comparator) {
    return delegate.max(comparator);
  }

  @Override
  public long count() {
    return delegate.count();
  }

  @Override
  public boolean anyMatch(Predicate<? super Record<K>> predicate) {
    return delegate.anyMatch(predicate);
  }

  @Override
  public boolean allMatch(Predicate<? super Record<K>> predicate) {
    return delegate.allMatch(predicate);
  }

  @Override
  public boolean noneMatch(Predicate<? super Record<K>> predicate) {
    return delegate.noneMatch(predicate);
  }

  @Override
  public Optional<Record<K>> findFirst() {
    return delegate.findFirst();
  }

  @Override
  public Optional<Record<K>> findAny() {
    return delegate.findAny();
  }

  @Override
  public Iterator<Record<K>> iterator() {
    return delegate.iterator();
  }

  @Override
  public Spliterator<Record<K>> spliterator() {
    return delegate.spliterator();
  }

  @Override
  public boolean isParallel() {
    return delegate.isParallel();
  }

  @Override
  public Stream<Record<K>> sequential() {
    return delegate.sequential();
  }

  @Override
  public Stream<Record<K>> parallel() {
    return delegate.parallel();
  }

  @Override
  public Stream<Record<K>> unordered() {
    return delegate.unordered();
  }

  @Override
  public Stream<Record<K>> onClose(Runnable closeHandler) {
    return delegate.onClose(closeHandler);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public RecordStream<K> explain(Consumer<? super StreamPlan> consumer) {
    return wrap(onClose(() -> consumer.accept(null)));
  }

  @Override
  public Stream<Record<K>> selfClose(boolean close) {
    throw new UnsupportedOperationException("TestRecordStream.selfClose not implemented");
  }
}
