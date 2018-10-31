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

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
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
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implements a {@link DoubleStream} delegating method calls to a wrapped {@code DoubleStream}.
 * <p>
 * Implementations of static methods constructing a {@link DoubleStream} instance are intentionally omitted.
 *
 * @author Clifford W. Johnson
 */
public class WrappedDoubleStream
    extends AbstractWrappedStream<WrappedDoubleStream, Double, DoubleStream> implements DoubleStream {

  public WrappedDoubleStream(final DoubleStream wrappedStream) {
    super(wrappedStream);
  }

  protected WrappedDoubleStream(final DoubleStream wrappedStream, final boolean isHead) {
    super(wrappedStream, isHead);
  }

  @Override
  protected final WrappedDoubleStream wrap(final DoubleStream stream) {
    return wrapDoubleStream(stream);
  }

  @Override
  public DoubleStream filter(final DoublePredicate predicate) {
    return chain(IntermediateOperation.DOUBLE_FILTER, () -> wrap(nativeStream.filter(predicate)), predicate);
  }

  @Override
  public DoubleStream map(final DoubleUnaryOperator mapper) {
    return chain(IntermediateOperation.DOUBLE_MAP, () -> wrap(nativeStream.map(mapper)), mapper);
  }

  @Override
  public <U> Stream<U> mapToObj(final DoubleFunction<? extends U> mapper) {
    return chain(IntermediateOperation.DOUBLE_MAP_TO_OBJ, () -> wrapReferenceStream(nativeStream.mapToObj(mapper)), mapper);
  }

  @Override
  public IntStream mapToInt(final DoubleToIntFunction mapper) {
    return chain(IntermediateOperation.DOUBLE_MAP_TO_INT, () -> wrapIntStream(nativeStream.mapToInt(mapper)), mapper);
  }

  @Override
  public LongStream mapToLong(final DoubleToLongFunction mapper) {
    return chain(IntermediateOperation.DOUBLE_MAP_TO_LONG, () -> wrapLongStream(nativeStream.mapToLong(mapper)), mapper);
  }

  @Override
  public DoubleStream flatMap(final DoubleFunction<? extends DoubleStream> mapper) {
    return chain(IntermediateOperation.DOUBLE_FLAT_MAP, () -> wrap(nativeStream.flatMap(mapper)), mapper);
  }

  @Override
  public DoubleStream distinct() {
    return chain(IntermediateOperation.DISTINCT, () -> wrap(nativeStream.distinct()));
  }

  @Override
  public DoubleStream sorted() {
    return chain(IntermediateOperation.SORTED_0, () -> wrap(nativeStream.sorted()));
  }

  @Override
  public DoubleStream peek(final DoubleConsumer action) {
    return chain(IntermediateOperation.DOUBLE_PEEK, () -> wrap(nativeStream.peek(action)), action);
  }

  @Override
  public DoubleStream limit(final long maxSize) {
    return chain(IntermediateOperation.LIMIT, () -> wrap(nativeStream.limit(maxSize)), maxSize);
  }

  @Override
  public DoubleStream skip(final long n) {
    return chain(IntermediateOperation.SKIP, () -> wrap(nativeStream.skip(n)), n);
  }

  @Override
  public void forEach(final DoubleConsumer action) {
    terminal(TerminalOperation.DOUBLE_FOR_EACH, action);
    selfClose(s -> {
      s.forEach(action);
      return null;
    });
  }

  @Override
  public void forEachOrdered(final DoubleConsumer action) {
    terminal(TerminalOperation.DOUBLE_FOR_EACH_ORDERED, action);
    selfClose(s -> {
      s.forEachOrdered(action);
      return null;
    });
  }

  @Override
  public double[] toArray() {
    terminal(TerminalOperation.TO_ARRAY_0);
    return selfClose(DoubleStream::toArray);
  }

  @Override
  public double reduce(final double identity, final DoubleBinaryOperator op) {
    terminal(TerminalOperation.DOUBLE_REDUCE_2, identity, op);
    return selfClose(s -> s.reduce(identity, op));
  }

  @Override
  public OptionalDouble reduce(final DoubleBinaryOperator op) {
    terminal(TerminalOperation.DOUBLE_REDUCE_1, op);
    return selfClose(s -> s.reduce(op));
  }

  @Override
  public <R> R collect(final Supplier<R> supplier, final ObjDoubleConsumer<R> accumulator, final BiConsumer<R, R> combiner) {
    terminal(TerminalOperation.DOUBLE_COLLECT, supplier, accumulator, combiner);
    return selfClose(s -> s.collect(supplier, accumulator, combiner));
  }

  @Override
  public double sum() {
    terminal(TerminalOperation.SUM);
    return selfClose(DoubleStream::sum);
  }

  @Override
  public OptionalDouble min() {
    terminal(TerminalOperation.MIN_0);
    return selfClose(DoubleStream::min);
  }

  @Override
  public OptionalDouble max() {
    terminal(TerminalOperation.MAX_0);
    return selfClose(DoubleStream::max);
  }

  @Override
  public long count() {
    terminal(TerminalOperation.COUNT);
    return selfClose(DoubleStream::count);
  }

  @Override
  public OptionalDouble average() {
    terminal(TerminalOperation.AVERAGE);
    return selfClose(DoubleStream::average);
  }

  @Override
  public DoubleSummaryStatistics summaryStatistics() {
    terminal(TerminalOperation.SUMMARY_STATISTICS);
    return selfClose(DoubleStream::summaryStatistics);
  }

  @Override
  public boolean anyMatch(final DoublePredicate predicate) {
    terminal(TerminalOperation.DOUBLE_ANY_MATCH, predicate);
    return selfClose(s -> s.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(final DoublePredicate predicate) {
    terminal(TerminalOperation.DOUBLE_ALL_MATCH, predicate);
    return selfClose(s -> s.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(final DoublePredicate predicate) {
    terminal(TerminalOperation.DOUBLE_NONE_MATCH, predicate);
    return selfClose(s -> s.noneMatch(predicate));
  }

  @Override
  public OptionalDouble findFirst() {
    terminal(TerminalOperation.FIND_FIRST);
    return selfClose(DoubleStream::findFirst);
  }

  @Override
  public OptionalDouble findAny() {
    terminal(TerminalOperation.FIND_ANY);
    return selfClose(DoubleStream::findAny);
  }

  @Override
  public Stream<Double> boxed() {
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
  public PrimitiveIterator.OfDouble iterator() {
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
  public Spliterator.OfDouble spliterator() {
    terminal(TerminalOperation.SPLITERATOR);
    if (this.isSelfClosing()) {
      this.setClosureGuard();
    }
    return nativeStream.spliterator();
  }
}
