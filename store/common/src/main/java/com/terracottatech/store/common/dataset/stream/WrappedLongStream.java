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

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implements a {@link LongStream} delegating method calls to a wrapped {@code LongStream}.
 * <p>
 * Implementations of static methods constructing a {@link LongStream} instance are intentionally omitted.
 *
 * @author Clifford W. Johnson
 */
public class WrappedLongStream
    extends AbstractWrappedStream<WrappedLongStream, Long, LongStream> implements LongStream {

  public WrappedLongStream(final LongStream wrappedStream) {
    super(wrappedStream);
  }

  protected WrappedLongStream(final LongStream wrappedStream, final boolean isHead) {
    super(wrappedStream, isHead);
  }

  @Override
  protected final WrappedLongStream wrap(final LongStream stream) {
    return wrapLongStream(stream);
  }

  @Override
  public LongStream filter(final LongPredicate predicate) {
    return chain(IntermediateOperation.LONG_FILTER, () -> wrap(nativeStream.filter(predicate)), predicate);
  }

  @Override
  public LongStream map(final LongUnaryOperator mapper) {
    return chain(IntermediateOperation.LONG_MAP, () -> wrap(nativeStream.map(mapper)), mapper);
  }

  @Override
  public <U> Stream<U> mapToObj(final LongFunction<? extends U> mapper) {
    return chain(IntermediateOperation.LONG_MAP_TO_OBJ, () -> wrapReferenceStream(nativeStream.mapToObj(mapper)), mapper);
  }

  @Override
  public IntStream mapToInt(final LongToIntFunction mapper) {
    return chain(IntermediateOperation.LONG_MAP_TO_INT, () -> wrapIntStream(nativeStream.mapToInt(mapper)), mapper);
  }

  @Override
  public DoubleStream mapToDouble(final LongToDoubleFunction mapper) {
    return chain(IntermediateOperation.LONG_MAP_TO_DOUBLE, () -> wrapDoubleStream(nativeStream.mapToDouble(mapper)), mapper);
  }

  @Override
  public LongStream flatMap(final LongFunction<? extends LongStream> mapper) {
    return chain(IntermediateOperation.LONG_FLAT_MAP, () -> wrapLongStream(nativeStream.flatMap(mapper)), mapper);
  }

  @Override
  public LongStream distinct() {
    return chain(IntermediateOperation.DISTINCT, () -> wrap(nativeStream.distinct()));
  }

  @Override
  public LongStream sorted() {
    return chain(IntermediateOperation.SORTED_0, () -> wrap(nativeStream.sorted()));
  }

  @Override
  public LongStream peek(final LongConsumer action) {
    return chain(IntermediateOperation.LONG_PEEK, () -> wrap(nativeStream.peek(action)), action);
  }

  @Override
  public LongStream limit(final long maxSize) {
    return chain(IntermediateOperation.LIMIT, () -> wrap(nativeStream.limit(maxSize)), maxSize);
  }

  @Override
  public LongStream skip(final long n) {
    return chain(IntermediateOperation.SKIP, () -> wrap(nativeStream.skip(n)), n);
  }

  @Override
  public void forEach(final LongConsumer action) {
    terminal(TerminalOperation.LONG_FOR_EACH, action);
    selfClose(s -> {
      s.forEach(action);
      return null;
    });
  }

  @Override
  public void forEachOrdered(final LongConsumer action) {
    terminal(TerminalOperation.LONG_FOR_EACH_ORDERED, action);
    selfClose(s -> {
      s.forEachOrdered(action);
      return null;
    });
  }

  @Override
  public long[] toArray() {
    terminal(TerminalOperation.TO_ARRAY_0);
    return selfClose(LongStream::toArray);
  }

  @Override
  public long reduce(final long identity, final LongBinaryOperator op) {
    terminal(TerminalOperation.LONG_REDUCE_2, identity, op);
    return selfClose(s -> s.reduce(identity, op));
  }

  @Override
  public OptionalLong reduce(final LongBinaryOperator op) {
    terminal(TerminalOperation.LONG_REDUCE_1, op);
    return selfClose(s -> s.reduce(op));
  }

  @Override
  public <R> R collect(final Supplier<R> supplier, final ObjLongConsumer<R> accumulator, final BiConsumer<R, R> combiner) {
    terminal(TerminalOperation.LONG_COLLECT, supplier, accumulator, combiner);
    return selfClose(s -> s.collect(supplier, accumulator, combiner));
  }

  @Override
  public long sum() {
    terminal(TerminalOperation.SUM);
    return selfClose(LongStream::sum);
  }

  @Override
  public OptionalLong min() {
    terminal(TerminalOperation.MIN_0);
    return selfClose(LongStream::min);
  }

  @Override
  public OptionalLong max() {
    terminal(TerminalOperation.MAX_0);
    return selfClose(LongStream::max);
  }

  @Override
  public long count() {
    terminal(TerminalOperation.COUNT);
    return selfClose(LongStream::count);
  }

  @Override
  public OptionalDouble average() {
    terminal(TerminalOperation.AVERAGE);
    return selfClose(LongStream::average);
  }

  @Override
  public LongSummaryStatistics summaryStatistics() {
    terminal(TerminalOperation.SUMMARY_STATISTICS);
    return selfClose(LongStream::summaryStatistics);
  }

  @Override
  public boolean anyMatch(final LongPredicate predicate) {
    terminal(TerminalOperation.LONG_ANY_MATCH, predicate);
    return selfClose(s -> s.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(final LongPredicate predicate) {
    terminal(TerminalOperation.LONG_ALL_MATCH, predicate);
    return selfClose(s -> s.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(final LongPredicate predicate) {
    terminal(TerminalOperation.LONG_NONE_MATCH, predicate);
    return selfClose(s -> s.noneMatch(predicate));
  }

  @Override
  public OptionalLong findFirst() {
    terminal(TerminalOperation.FIND_FIRST);
    return selfClose(LongStream::findFirst);
  }

  @Override
  public OptionalLong findAny() {
    terminal(TerminalOperation.FIND_ANY);
    return selfClose(LongStream::findAny);
  }

  @Override
  public DoubleStream asDoubleStream() {
    return chain(IntermediateOperation.AS_DOUBLE_STREAM, () -> wrapDoubleStream(nativeStream.asDoubleStream()));
  }

  @Override
  public Stream<Long> boxed() {
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
  public PrimitiveIterator.OfLong iterator() {
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
  public Spliterator.OfLong spliterator() {
    terminal(TerminalOperation.SPLITERATOR);
    if (this.isSelfClosing()) {
      this.setClosureGuard();
    }
    return nativeStream.spliterator();
  }
}
