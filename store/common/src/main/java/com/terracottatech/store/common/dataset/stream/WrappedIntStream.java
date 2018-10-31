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

import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implements a {@link IntStream} delegating method calls to a wrapped {@code IntStream}.
 * <p>
 * Implementations of static methods constructing a {@link IntStream} instance are intentionally omitted.
 *
 * @author Clifford W. Johnson
 */
public class WrappedIntStream
    extends AbstractWrappedStream<WrappedIntStream, Integer, IntStream> implements IntStream {

  public WrappedIntStream(final IntStream wrappedStream) {
    super(wrappedStream);
  }

  protected WrappedIntStream(final IntStream wrappedStream, final boolean isHead) {
    super(wrappedStream, isHead);
  }

  @Override
  protected final WrappedIntStream wrap(final IntStream stream) {
    return wrapIntStream(stream);
  }

  @Override
  public IntStream filter(final IntPredicate predicate) {
    return chain(IntermediateOperation.INT_FILTER, () -> wrap(nativeStream.filter(predicate)), predicate);
  }

  @Override
  public IntStream map(final IntUnaryOperator mapper) {
    return chain(IntermediateOperation.INT_MAP, () -> wrap(nativeStream.map(mapper)), mapper);
  }

  @Override
  public <U> Stream<U> mapToObj(final IntFunction<? extends U> mapper) {
    return chain(IntermediateOperation.INT_MAP_TO_OBJ, () -> wrapReferenceStream(nativeStream.mapToObj(mapper)), mapper);
  }

  @Override
  public LongStream mapToLong(final IntToLongFunction mapper) {
    return chain(IntermediateOperation.INT_MAP_TO_LONG, () -> wrapLongStream(nativeStream.mapToLong(mapper)), mapper);
  }

  @Override
  public DoubleStream mapToDouble(final IntToDoubleFunction mapper) {
    return chain(IntermediateOperation.INT_MAP_TO_DOUBLE, () -> wrapDoubleStream(nativeStream.mapToDouble(mapper)), mapper);
  }

  @Override
  public IntStream flatMap(final IntFunction<? extends IntStream> mapper) {
    return chain(IntermediateOperation.INT_FLAT_MAP, () -> wrap(nativeStream.flatMap(mapper)), mapper);
  }

  @Override
  public IntStream distinct() {
    return chain(IntermediateOperation.DISTINCT, () -> wrap(nativeStream.distinct()));
  }

  @Override
  public IntStream sorted() {
    return chain(IntermediateOperation.SORTED_0, () -> wrap(nativeStream.sorted()));
  }

  @Override
  public IntStream peek(final IntConsumer action) {
    return chain(IntermediateOperation.INT_PEEK, () -> wrap(nativeStream.peek(action)), action);
  }

  @Override
  public IntStream limit(final long maxSize) {
    return chain(IntermediateOperation.LIMIT, () -> wrap(nativeStream.limit(maxSize)), maxSize);
  }

  @Override
  public IntStream skip(final long n) {
    return chain(IntermediateOperation.SKIP, () -> wrap(nativeStream.skip(n)), n);
  }

  @Override
  public void forEach(final IntConsumer action) {
    terminal(TerminalOperation.INT_FOR_EACH, action);
    selfClose(s -> {
      s.forEach(action);
      return null;
    });
  }

  @Override
  public void forEachOrdered(final IntConsumer action) {
    terminal(TerminalOperation.INT_FOR_EACH_ORDERED, action);
    selfClose(s -> {
      s.forEachOrdered(action);
      return null;
    });
  }

  @Override
  public int[] toArray() {
    terminal(TerminalOperation.TO_ARRAY_0);
    return selfClose(IntStream::toArray);
  }

  @Override
  public int reduce(final int identity, final IntBinaryOperator op) {
    terminal(TerminalOperation.INT_REDUCE_2, identity, op);
    return selfClose(s -> s.reduce(identity, op));
  }

  @Override
  public OptionalInt reduce(final IntBinaryOperator op) {
    terminal(TerminalOperation.INT_REDUCE_1, op);
    return selfClose(s -> s.reduce(op));
  }

  @Override
  public <R> R collect(final Supplier<R> supplier, final ObjIntConsumer<R> accumulator, final BiConsumer<R, R> combiner) {
    terminal(TerminalOperation.INT_COLLECT, supplier, accumulator, combiner);
    return selfClose(s -> s.collect(supplier, accumulator, combiner));
  }

  @Override
  public int sum() {
    terminal(TerminalOperation.SUM);
    return selfClose(IntStream::sum);
  }

  @Override
  public OptionalInt min() {
    terminal(TerminalOperation.MIN_0);
    return selfClose(IntStream::min);
  }

  @Override
  public OptionalInt max() {
    terminal(TerminalOperation.MAX_0);
    return selfClose(IntStream::max);
  }

  @Override
  public long count() {
    terminal(TerminalOperation.COUNT);
    return selfClose(IntStream::count);
  }

  @Override
  public OptionalDouble average() {
    terminal(TerminalOperation.AVERAGE);
    return selfClose(IntStream::average);
  }

  @Override
  public IntSummaryStatistics summaryStatistics() {
    terminal(TerminalOperation.SUMMARY_STATISTICS);
    return selfClose(IntStream::summaryStatistics);
  }

  @Override
  public boolean anyMatch(final IntPredicate predicate) {
    terminal(TerminalOperation.INT_ANY_MATCH, predicate);
    return selfClose(s -> s.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(final IntPredicate predicate) {
    terminal(TerminalOperation.INT_ALL_MATCH, predicate);
    return selfClose(s -> s.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(final IntPredicate predicate) {
    terminal(TerminalOperation.INT_NONE_MATCH, predicate);
    return selfClose(s -> s.noneMatch(predicate));
  }

  @Override
  public OptionalInt findFirst() {
    terminal(TerminalOperation.FIND_FIRST);
    return selfClose(IntStream::findFirst);
  }

  @Override
  public OptionalInt findAny() {
    terminal(TerminalOperation.FIND_ANY);
    return selfClose(IntStream::findAny);
  }

  @Override
  public LongStream asLongStream() {
    return chain(IntermediateOperation.AS_LONG_STREAM, () -> wrapLongStream(nativeStream.asLongStream()));
  }

  @Override
  public DoubleStream asDoubleStream() {
    return chain(IntermediateOperation.AS_DOUBLE_STREAM, () -> wrapDoubleStream(nativeStream.asDoubleStream()));
  }

  @Override
  public Stream<Integer> boxed() {
    return chain(IntermediateOperation.BOXED, () -> wrapReferenceStream(nativeStream.boxed()));
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
  public PrimitiveIterator.OfInt iterator() {
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
  public Spliterator.OfInt spliterator() {
    terminal(TerminalOperation.SPLITERATOR);
    if (this.isSelfClosing()) {
      this.setClosureGuard();
    }
    return nativeStream.spliterator();
  }
}
