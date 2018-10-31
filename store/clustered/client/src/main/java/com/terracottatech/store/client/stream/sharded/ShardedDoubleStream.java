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

package com.terracottatech.store.client.stream.sharded;

import com.terracottatech.store.common.dataset.stream.WrappedDoubleStream;

import java.util.Arrays;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Comparator.naturalOrder;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;
import static java.util.stream.StreamSupport.doubleStream;

public final class ShardedDoubleStream extends AbstractShardedBaseStream<Double, DoubleStream> implements DoubleStream {

  ShardedDoubleStream(Stream<DoubleStream> shards, Comparator<? super Double> ordering, int characteristics, AbstractShardedRecordStream<?, ?> source) {
    super(shards, ordering, characteristics | NONNULL, source);
  }

  private DoubleStream merge() {
    boolean parallel = isParallel();
    return linkClosureAndMerge(stream -> {
      Supplier<Spliterator.OfDouble> spliterator = stream.apply(s -> (Supplier<Spliterator.OfDouble>) s::spliterator)
          .reduce((a, b) -> mergeSpliterators(a, b, parallel))
          .orElse(Spliterators::emptyDoubleSpliterator);

      return new WrappedDoubleStream(doubleStream(spliterator, with(), parallel));
    });
  }

  private Supplier<Spliterator.OfDouble> mergeSpliterators(Supplier<Spliterator.OfDouble> a, Supplier<Spliterator.OfDouble> b, boolean parallel) {
    if (is(ORDERED)) {
      return new SortedSpliteratorOfDouble.FactoryOfDouble(a, b, getOrdering(), with());
    } else {
      return new SequencedSpliteratorOfDouble.FactoryOfDouble(a, b, with(), parallel);
    }
  }

  @Override
  public DoubleStream filter(DoublePredicate predicate) {
    return derive(s -> s.filter(predicate));
  }

  @Override
  public DoubleStream map(DoubleUnaryOperator mapper) {
    if (is(ORDERED)) {
      return merge().map(mapper);
    } else {
      return derive(s -> s.map(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
    if (is(ORDERED)) {
      return merge().mapToObj(mapper);
    } else {
      return deriveObj(s -> s.mapToObj(mapper), null, without(DISTINCT, SORTED, NONNULL));
    }
  }

  @Override
  public IntStream mapToInt(DoubleToIntFunction mapper) {
    if (is(ORDERED)) {
      return merge().mapToInt(mapper);
    } else {
      return deriveInt(s -> s.mapToInt(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public LongStream mapToLong(DoubleToLongFunction mapper) {
    if (is(ORDERED)) {
      return merge().mapToLong(mapper);
    } else {
      return deriveLong(s -> s.mapToLong(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
    if (is(ORDERED)) {
      return merge().flatMap(mapper);
    } else {
      return derive(s -> s.flatMap(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public DoubleStream distinct() {
    return derive(DoubleStream::distinct).merge().distinct();
  }

  @Override
  public DoubleStream sorted() {
    /*
     * You can't tell the difference between a stable and unstable sort when the whole input value is used in the comparator
     */
    return derive(DoubleStream::sorted, naturalOrder(), with(SORTED, ORDERED));
  }

  @Override
  public DoubleStream peek(DoubleConsumer action) {
    return derive(s -> s.peek(action));
  }

  @Override
  public DoubleStream limit(long maxSize) {
    return derive(s -> s.limit(maxSize)).merge().limit(maxSize);
  }

  @Override
  public DoubleStream skip(long n) {
    return merge().skip(n);
  }

  @Override
  public void forEach(DoubleConsumer action) {
    consume(s -> s.forEach(action));
  }

  @Override
  public void forEachOrdered(DoubleConsumer action) {
    if (is(ORDERED)) {
      merge().forEachOrdered(action);
    } else {
      forEach(action);
    }
  }

  @Override
  public double[] toArray() {
    if (is(ORDERED)) {
      return merge().toArray();
    } else {
      return terminateWith(DoubleStream::toArray).reduce(explainedAs("array-merging", (a, b) -> {
        double[] c = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
      })).orElse(new double[0]);
    }
  }

  @Override
  public double reduce(double identity, DoubleBinaryOperator op) {
    if (is(ORDERED)) {
      return merge().reduce(identity, op);
    } else {
      return terminateWithDouble(s -> s.reduce(identity, op)).reduce(identity, explainedAs("final-reduction: {0}", op));
    }
  }

  @Override
  public OptionalDouble reduce(DoubleBinaryOperator op) {
    if (is(ORDERED)) {
      return merge().reduce(op);
    } else {
      return terminateWith(s -> s.reduce(op)).filter(OptionalDouble::isPresent).mapToDouble(OptionalDouble::getAsDouble).reduce(explainedAs("final-reduction: {0}", op));
    }
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    if (is(ORDERED)) {
      return merge().collect(supplier, accumulator, combiner);
    } else {
      return terminateWith(s -> s.collect(supplier, accumulator, combiner)).reduce(explainedAs("final-combining: {0}", (a, b) -> {
        combiner.accept(a, b);
        return a;
      })).orElse(supplier.get());
    }
  }

  @Override
  public double sum() {
    if (is(ORDERED)) {
      return merge().sum();
    } else {
      return terminateWithDouble(DoubleStream::sum).sum();
    }
  }

  @Override
  public OptionalDouble min() {
    return terminateWith(DoubleStream::min).min(explainedAs("emptiesLast(naturalOrder())", emptiesLast(naturalOrder()))).orElse(OptionalDouble.empty());
  }

  @Override
  public OptionalDouble max() {
    return terminateWith(DoubleStream::max).max(explainedAs("emptiesFirst(naturalOrder())", emptiesFirst(naturalOrder()))).orElse(OptionalDouble.empty());
  }

  @Override
  public long count() {
    return terminateWithLong(DoubleStream::count).sum();
  }

  @Override
  public OptionalDouble average() {
    DoubleSummaryStatistics summary = summaryStatistics();
    if (summary.getCount() == 0) {
      return OptionalDouble.empty();
    } else {
      return OptionalDouble.of(summary.getAverage());
    }
  }

  @Override
  public DoubleSummaryStatistics summaryStatistics() {
    if (is(ORDERED)) {
      return merge().summaryStatistics();
    } else {
      return terminateWith(DoubleStream::summaryStatistics).reduce(explainedAs("DoubleSummaryStatistics::combine", (a, b) -> {
        a.combine(b);
        return a;
      })).orElse(new DoubleSummaryStatistics());
    }
  }

  @Override
  public boolean anyMatch(DoublePredicate predicate) {
    return terminateWith(s -> s.anyMatch(predicate)).anyMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean allMatch(DoublePredicate predicate) {
    return terminateWith(s -> s.allMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean noneMatch(DoublePredicate predicate) {
    return terminateWith(s -> s.noneMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public OptionalDouble findFirst() {
    if (is(ORDERED)) {
      return terminateWith(DoubleStream::findFirst).min(explainedAs("emptiesLast(" + getOrdering() + ")", emptiesLast(getOrdering()))).orElse(OptionalDouble.empty());
    } else {
      return findAny();
    }
  }

  @Override
  public OptionalDouble findAny() {
    return terminateWith(DoubleStream::findAny).filter(explainedAs("OptionalDouble::isPresent", OptionalDouble::isPresent)).findAny().orElse(OptionalDouble.empty());
  }

  @Override
  public Stream<Double> boxed() {
    return deriveObj(DoubleStream::boxed);
  }

  @Override
  public PrimitiveIterator.OfDouble iterator() {
    return merge().iterator();
  }

  @Override
  public Spliterator.OfDouble spliterator() {
    return merge().spliterator();
  }

  @Override
  public DoubleStream sequential() {
    return new ShardedDoubleStream(apply(DoubleStream::sequential).sequential(), getOrdering(), with(), getSource());
  }

  @Override
  public DoubleStream parallel() {
    return new ShardedDoubleStream(apply(DoubleStream::parallel).parallel(), getOrdering(), with(), getSource());
  }

  @Override
  public DoubleStream unordered() {
    return new ShardedDoubleStream(apply(DoubleStream::unordered).unordered(), null, without(ORDERED), getSource());
  }

  /**
   * Derive a new sharded douebl stream with the same characteristics and ordering.
   *
   * @param operation shard pipeline operation
   * @return a new stream
   */
  private ShardedDoubleStream derive(Function<DoubleStream, DoubleStream> operation) {
    return derive(operation, with());
  }

  /**
   * Derive a new sharded double stream with updated characteristics.
   *
   * @param operation shard pipeline operation
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedDoubleStream derive(Function<DoubleStream, DoubleStream> operation, int characteristics) {
    return derive(operation, getOrdering(), characteristics);
  }

  /**
   * Derive a new sharded double stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedDoubleStream derive(Function<DoubleStream, DoubleStream> operation, Comparator<? super Double> order, int characteristics) {
    return new ShardedDoubleStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded int stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedIntStream deriveInt(Function<DoubleStream, IntStream> operation, Comparator<? super Integer> order, int characteristics) {
    return new ShardedIntStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded long stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedLongStream deriveLong(Function<DoubleStream, LongStream> operation, Comparator<? super Long> order, int characteristics) {
    return new ShardedLongStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded double stream with the same characteristics and ordering.
   *
   * @param operation shard pipeline operation
   * @return a new stream
   */
  private ShardedReferenceStream<Double> deriveObj(Function<DoubleStream, Stream<Double>> operation) {
    return new ShardedReferenceStream<>(apply(operation), getOrdering(), with(), getSource());
  }

  /**
   * Derive a new sharded stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private <T> ShardedReferenceStream<T> deriveObj(Function<DoubleStream, Stream<T>> operation, Comparator<? super T> order, int characteristics) {
    return new ShardedReferenceStream<>(apply(operation), order, characteristics, getSource());
  }

  private static Comparator<OptionalDouble> emptiesFirst(Comparator<? super Double> comparator) {
    return Comparator.comparing(OptionalDouble::isPresent, (a, b) -> a ? b ? 0 : -1 : 1).thenComparing(OptionalDouble::getAsDouble, comparator);
  }

  private static Comparator<OptionalDouble> emptiesLast(Comparator<? super Double> comparator) {
    return Comparator.comparing(OptionalDouble::isPresent, (a, b) -> a ? b ? 0 : 1 : -1).thenComparing(OptionalDouble::getAsDouble, comparator);
  }
}
