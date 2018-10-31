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

package com.terracottatech.store.wrapper;

import com.terracottatech.store.Record;
import com.terracottatech.store.stream.RecordStream;

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

public class WrapperRecordStream<K extends Comparable<K>> implements RecordStream<K> {

  @Override
  public RecordStream<K> explain(Consumer<Object> consumer) {
    return new WrapperRecordStream<>(((com.terracottatech.sovereign.RecordStream<K>) delegate).explain(consumer));
  }

  @Override
  public RecordStream<K> batch(int sizeHint) {
    return this;
  }

  @Override
  public RecordStream<K> inline() {
    return this;
  }

  @Override
  public RecordStream<K> filter(Predicate<? super Record<K>> predicate) {
    return new WrapperRecordStream<>(delegate.filter(predicate));
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
  public RecordStream<K> distinct() {
    return new WrapperRecordStream<>(delegate.distinct());
  }

  @Override
  public RecordStream<K> sorted() {
    throw new UnsupportedOperationException("sorted() is not supported - Record is not Comparable, what you mean probably is sorted(keyFunction().asComparator())");
  }

  @Override
  public RecordStream<K> sorted(Comparator<? super Record<K>> comparator) {
    return new WrapperRecordStream<>(delegate.sorted(comparator));
  }

  @Override
  public RecordStream<K> peek(Consumer<? super Record<K>> action) {
    return new WrapperRecordStream<>(delegate.peek(action));
  }

  @Override
  public RecordStream<K> limit(long maxSize) {
    return new WrapperRecordStream<>(delegate.limit(maxSize));
  }

  @Override
  public RecordStream<K> skip(long n) {
    return new WrapperRecordStream<>(delegate.skip(n));
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
  public RecordStream<K> sequential() {
    return new WrapperRecordStream<>(delegate.sequential());
  }

  @Override
  public RecordStream<K> parallel() {
    return new WrapperRecordStream<>(delegate.parallel());
  }

  @Override
  public RecordStream<K> unordered() {
    return new WrapperRecordStream<>(delegate.unordered());
  }

  @Override
  public RecordStream<K> onClose(Runnable closeHandler) {
    return new WrapperRecordStream<>(delegate.onClose(closeHandler));
  }

  @Override
  public void close() {
    delegate.close();
  }

  private final Stream<Record<K>> delegate;

  public WrapperRecordStream(Stream<Record<K>> delegate) {
    this.delegate = delegate;
  }


}
