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
package com.terracottatech.store.common.dataset.stream;

import com.terracottatech.store.StoreRetryableStreamTerminatedException;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;

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
 * Implements a {@link Stream} delegating method calls to a wrapped {@code Stream}.
 * The terminal methods in this implementation each close the wrapped stream when the
 * method is complete.
 * <p>
 * Implementations of static methods constructing a {@link Stream} instance are intentionally omitted.
 *
 * @author Clifford W. Johnson
 */
public class WrappedReferenceStream<T>
    extends AbstractWrappedStream<WrappedReferenceStream<T>, T, Stream<T>> implements Stream<T> {

  public WrappedReferenceStream(final Stream<T> wrappedStream) {
    super(wrappedStream);
  }

  protected WrappedReferenceStream(final Stream<T> wrappedStream, final boolean isHead) {
    super(wrappedStream, isHead);
  }

  @Override
  protected WrappedReferenceStream<T> wrap(final Stream<T> stream) {
    return wrapReferenceStream(stream);
  }

  @Override
  public Stream<T> filter(final Predicate<? super T> predicate) {
    return chain(IntermediateOperation.FILTER, () -> wrap(nativeStream.filter(predicate)), predicate);
  }

  @Override
  public <R> Stream<R> map(final Function<? super T, ? extends R> mapper) {
    return chain(IntermediateOperation.MAP, () -> wrapReferenceStream(nativeStream.map(mapper)), mapper);
  }

  @Override
  public IntStream mapToInt(final ToIntFunction<? super T> mapper) {
    return chain(IntermediateOperation.MAP_TO_INT, () -> wrapIntStream(nativeStream.mapToInt(mapper)), mapper);
  }

  @Override
  public LongStream mapToLong(final ToLongFunction<? super T> mapper) {
    return chain(IntermediateOperation.MAP_TO_LONG, () -> wrapLongStream(nativeStream.mapToLong(mapper)), mapper);
  }

  @Override
  public DoubleStream mapToDouble(final ToDoubleFunction<? super T> mapper) {
    return chain(IntermediateOperation.MAP_TO_DOUBLE, () -> wrapDoubleStream(nativeStream.mapToDouble(mapper)), mapper);
  }

  @Override
  public <R> Stream<R> flatMap(final Function<? super T, ? extends Stream<? extends R>> mapper) {
    return chain(IntermediateOperation.FLAT_MAP, () -> wrapReferenceStream(nativeStream.flatMap(mapper)), mapper);
  }

  @Override
  public IntStream flatMapToInt(final Function<? super T, ? extends IntStream> mapper) {
    return chain(IntermediateOperation.FLAT_MAP_TO_INT, () -> wrapIntStream(nativeStream.flatMapToInt(mapper)), mapper);
  }

  @Override
  public LongStream flatMapToLong(final Function<? super T, ? extends LongStream> mapper) {
    return chain(IntermediateOperation.FLAT_MAP_TO_LONG, () -> wrapLongStream(nativeStream.flatMapToLong(mapper)), mapper);
  }

  @Override
  public DoubleStream flatMapToDouble(final Function<? super T, ? extends DoubleStream> mapper) {
    return chain(IntermediateOperation.FLAT_MAP_TO_DOUBLE, () -> wrapDoubleStream(nativeStream.flatMapToDouble(mapper)), mapper);
  }

  @Override
  public Stream<T> distinct() {
    return chain(IntermediateOperation.DISTINCT, () -> wrap(nativeStream.distinct()));
  }

  @Override
  public Stream<T> sorted() {
    return chain(IntermediateOperation.SORTED_0, () -> wrap(nativeStream.sorted()));
  }

  @Override
  public Stream<T> sorted(final Comparator<? super T> comparator) {
    return chain(IntermediateOperation.SORTED_1, () -> wrap(nativeStream.sorted(comparator)), comparator);
  }

  @Override
  public Stream<T> peek(final Consumer<? super T> action) {
    return chain(IntermediateOperation.PEEK, () -> wrap(nativeStream.peek(action)), action);
  }

  @Override
  public Stream<T> limit(final long maxSize) {
    return chain(IntermediateOperation.LIMIT, () -> wrap(nativeStream.limit(maxSize)), maxSize);
  }

  @Override
  public Stream<T> skip(final long n) {
    return chain(IntermediateOperation.SKIP, () -> wrap(nativeStream.skip(n)), n);
  }

  @Override
  public void forEach(final Consumer<? super T> action) {
    terminal(TerminalOperation.FOR_EACH, action);
    selfClose(s -> {
      s.forEach(action);
      return null;
    });
  }

  @Override
  public void forEachOrdered(final Consumer<? super T> action) {
    terminal(TerminalOperation.FOR_EACH_ORDERED, action);
    selfClose(s -> {
      s.forEachOrdered(action);
      return null;
    });
  }

  @Override
  public Object[] toArray() {
    terminal(TerminalOperation.TO_ARRAY_0);
    return selfClose(Stream::toArray);
  }

  @Override
  public <A> A[] toArray(final IntFunction<A[]> generator) {
    terminal(TerminalOperation.TO_ARRAY_1, generator);
    return selfClose(s -> s.toArray(generator));
  }

  @Override
  public T reduce(final T identity, final BinaryOperator<T> accumulator) {
    terminal(TerminalOperation.REDUCE_2, identity, accumulator);
    return selfClose(s -> s.reduce(identity, accumulator));
  }

  @Override
  public Optional<T> reduce(final BinaryOperator<T> accumulator) {
    terminal(TerminalOperation.REDUCE_1, accumulator);
    return selfClose(s -> s.reduce(accumulator));
  }

  @Override
  public <U> U reduce(final U identity, final BiFunction<U, ? super T, U> accumulator, final BinaryOperator<U> combiner) {
    terminal(TerminalOperation.REDUCE_3, identity, accumulator, combiner);
    return selfClose(s -> s.reduce(identity, accumulator, combiner));
  }

  @Override
  public <R> R collect(final Supplier<R> supplier, final BiConsumer<R, ? super T> accumulator, final BiConsumer<R, R> combiner) {
    terminal(TerminalOperation.COLLECT_3, supplier, accumulator, combiner);
    return selfClose(s -> s.collect(supplier, accumulator, combiner));
  }

  @Override
  public <R, A> R collect(final Collector<? super T, A, R> collector) {
    terminal(TerminalOperation.COLLECT_1, collector);
    return selfClose(s -> s.collect(collector));
  }

  @Override
  public Optional<T> min(final Comparator<? super T> comparator) {
    terminal(TerminalOperation.MIN_1, comparator);
    return selfClose(s -> s.min(comparator));
  }

  @Override
  public Optional<T> max(final Comparator<? super T> comparator) {
    terminal(TerminalOperation.MAX_1, comparator);
    return selfClose(s -> s.max(comparator));
  }

  @Override
  public long count() {
    terminal(TerminalOperation.COUNT);
    return selfClose(Stream::count);
  }

  @Override
  public boolean anyMatch(final Predicate<? super T> predicate) {
    terminal(TerminalOperation.ANY_MATCH, predicate);
    return selfClose(s -> s.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(final Predicate<? super T> predicate) {
    terminal(TerminalOperation.ALL_MATCH, predicate);
    return selfClose(s -> s.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(final Predicate<? super T> predicate) {
    terminal(TerminalOperation.NONE_MATCH, predicate);
    return selfClose(s -> s.noneMatch(predicate));
  }

  @Override
  public Optional<T> findFirst() {
    terminal(TerminalOperation.FIND_FIRST);
    return selfClose(Stream::findFirst);
  }

  @Override
  public Optional<T> findAny() {
    terminal(TerminalOperation.FIND_ANY);
    return selfClose(Stream::findAny);
  }

  /**
   * {@inheritDoc}
   * <p>
   * When {@link #selfClose(boolean) self-closing} is enabled, this method allocates and sets a
   * <a href="http://www.informit.com/articles/article.aspx?p=1216151&seqNum=7">finalizer guardian</a>
   * (<cite>Effective Java, 2ed - Item 7: Avoid finalizers</cite>, Bloch) in this stream.  The finalizer
   * guardian may be called once no strong references remain for this stream.  When called, the guardian
   * attempts to ensure the underlying stream and its resources are closed.  Given the uncertain execution
   * and timing of {@link #finalize()}, it is always better to close this stream by calling {@link #close()}
   * or through use of a try-with-resources construct to ensure the stream is closed when the iterator is no
   * longer of use.
   *
   * @return {@inheritDoc}
   */
  @Override
  public Iterator<T> iterator() {
    terminal(TerminalOperation.ITERATOR);
    if (this.isSelfClosing()) {
      this.setClosureGuard();
    }
    return nativeStream.iterator();
  }

  /**
   * {@inheritDoc}
   * <p>
   * When {@link #selfClose(boolean) self-closing} is enabled, this method allocates and sets a
   * <a href="http://www.informit.com/articles/article.aspx?p=1216151&seqNum=7">finalizer guardian</a>
   * (<cite>Effective Java, 2ed - Item 7: Avoid finalizers</cite>, Bloch) in this stream.  The finalizer
   * guardian may be called once no strong references remain for this stream.  When called, the guardian
   * attempts to ensure the underlying stream and its resources are closed.  Given the uncertain execution
   * and timing of {@link #finalize()}, it is always better to close this stream by calling {@link #close()}
   * or through use of a try-with-resources construct to ensure the stream is closed when the spliterator is no
   * longer of use.
   *
   * @return {@inheritDoc}
   */
  @Override
  public Spliterator<T> spliterator() {
    terminal(TerminalOperation.SPLITERATOR);
    if (this.isSelfClosing()) {
      this.setClosureGuard();
    }
    return nativeStream.spliterator();
  }
}
