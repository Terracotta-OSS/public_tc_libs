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

import com.terracottatech.store.common.dataset.stream.WrappedLongStream;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Function;
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

import static java.util.Comparator.naturalOrder;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;
import static java.util.stream.StreamSupport.longStream;

public final class ShardedLongStream extends AbstractShardedBaseStream<Long, LongStream> implements LongStream {

  ShardedLongStream(Stream<LongStream> shards, Comparator<? super Long> ordering, int characteristics, AbstractShardedRecordStream<?, ?> source) {
    super(shards, ordering, characteristics | NONNULL, source);
  }

  private LongStream merge() {
    boolean parallel = isParallel();
    return linkClosureAndMerge(stream -> {
      Supplier<Spliterator.OfLong> spliterator = apply(s -> (Supplier<Spliterator.OfLong>) s::spliterator)
          .reduce((a, b) -> mergeSpliterators(a, b, parallel))
          .orElse(Spliterators::emptyLongSpliterator);
      return new WrappedLongStream(longStream(spliterator, with(), parallel));
    });
  }

  private Supplier<Spliterator.OfLong> mergeSpliterators(Supplier<Spliterator.OfLong> a, Supplier<Spliterator.OfLong> b, boolean parallel) {
    if (is(ORDERED)) {
      return new SortedSpliteratorOfLong.FactoryOfLong(a, b, getOrdering(), with());
    } else {
      return new SequencedSpliteratorOfLong.FactoryOfLong(a, b, with(), parallel);
    }
  }

  @Override
  public LongStream filter(LongPredicate predicate) {
    return derive(s -> s.filter(predicate));
  }

  @Override
  public LongStream map(LongUnaryOperator mapper) {
    if (is(ORDERED)) {
      return merge().map(mapper);
    } else {
      return derive(s -> s.map(mapper), null, without(SORTED, DISTINCT));
    }
  }

  @Override
  public <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
    if (is(ORDERED)) {
      return merge().mapToObj(mapper);
    } else {
      return deriveObj(s -> s.mapToObj(mapper), null, without(SORTED, DISTINCT, NONNULL));
    }
  }

  @Override
  public IntStream mapToInt(LongToIntFunction mapper) {
    if (is(ORDERED)) {
      return merge().mapToInt(mapper);
    } else {
      return deriveInt(s -> s.mapToInt(mapper), null, without(SORTED, DISTINCT));
    }
  }

  @Override
  public DoubleStream mapToDouble(LongToDoubleFunction mapper) {
    if (is(ORDERED)) {
      return merge().mapToDouble(mapper);
    } else {
      return deriveDouble(s -> s.mapToDouble(mapper), null, without(SORTED, DISTINCT));
    }
  }

  @Override
  public LongStream flatMap(LongFunction<? extends LongStream> mapper) {
    if (is(ORDERED)) {
      return merge().flatMap(mapper);
    } else {
      return derive(s -> s.flatMap(mapper), null, without(SORTED, DISTINCT));
    }
  }

  @Override
  public LongStream distinct() {
    return derive(LongStream::distinct).merge().distinct();
  }

  @Override
  public LongStream sorted() {
    /*
     * You can't tell the difference between a stable and unstable sort when the whole input value is used in the comparator
     */
    return derive(LongStream::sorted, naturalOrder(), with(SORTED, ORDERED));
  }

  @Override
  public LongStream peek(LongConsumer action) {
    return derive(s -> s.peek(action));
  }

  @Override
  public LongStream limit(long maxSize) {
    return derive(s -> s.limit(maxSize)).merge().limit(maxSize);
  }

  @Override
  public LongStream skip(long n) {
    return merge().skip(n);
  }

  @Override
  public void forEach(LongConsumer action) {
    consume(s -> s.forEach(action));
  }

  @Override
  public void forEachOrdered(LongConsumer action) {
    if (is(ORDERED)) {
      merge().forEachOrdered(action);
    } else {
      forEach(action);
    }
  }

  @Override
  public long[] toArray() {
    if (is(ORDERED)) {
      return merge().toArray();
    } else {
      return terminateWith(LongStream::toArray).reduce(explainedAs("array-merging", (a, b) -> {
        long[] c = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
      })).orElse(new long[0]);
    }
  }

  @Override
  public long reduce(long identity, LongBinaryOperator op) {
    if (is(ORDERED)) {
      return merge().reduce(identity, op);
    } else {
      return terminateWithLong(s -> s.reduce(identity, op)).reduce(identity, explainedAs("final-reduction: {0}", op));
    }
  }

  @Override
  public OptionalLong reduce(LongBinaryOperator op) {
    if (is(ORDERED)) {
      return merge().reduce(op);
    } else {
      return terminateWith(s -> s.reduce(op)).filter(OptionalLong::isPresent).mapToLong(OptionalLong::getAsLong).reduce(explainedAs("final-reduction: {0}", op));
    }
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
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
  public long sum() {
    return terminateWithLong(LongStream::sum).sum();
  }

  @Override
  public OptionalLong min() {
    return terminateWith(LongStream::min).min(explainedAs("emptiesLast(naturalOrder())", emptiesLast(naturalOrder()))).orElse(OptionalLong.empty());
  }

  @Override
  public OptionalLong max() {
    return terminateWith(LongStream::max).max(explainedAs("emptiesFirst(naturalOrder())", emptiesFirst(naturalOrder()))).orElse(OptionalLong.empty());
  }

  @Override
  public long count() {
    return terminateWithLong(LongStream::count).sum();
  }

  @Override
  public OptionalDouble average() {
    LongSummaryStatistics summary = summaryStatistics();
    if (summary.getCount() == 0) {
      return OptionalDouble.empty();
    } else {
      return OptionalDouble.of(summary.getAverage());
    }
  }

  @Override
  public LongSummaryStatistics summaryStatistics() {
    return terminateWith(LongStream::summaryStatistics).reduce(explainedAs("LongSummaryStatistics::combine", (a, b) -> {
      a.combine(b);
      return a;
    })).orElse(new LongSummaryStatistics());
  }

  @Override
  public boolean anyMatch(LongPredicate predicate) {
    return terminateWith(s -> s.anyMatch(predicate)).anyMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean allMatch(LongPredicate predicate) {
    return terminateWith(s -> s.allMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean noneMatch(LongPredicate predicate) {
    return terminateWith(s -> s.noneMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public OptionalLong findFirst() {
    if (is(ORDERED)) {
      return terminateWith(LongStream::findFirst).min(explainedAs("emptiesLast(" + getOrdering() + ")", emptiesLast(getOrdering()))).orElse(OptionalLong.empty());
    } else {
      return findAny();
    }
  }

  @Override
  public OptionalLong findAny() {
    return terminateWith(LongStream::findAny).filter(explainedAs("OptionalLong::isPresent", OptionalLong::isPresent)).findAny().orElse(OptionalLong.empty());
  }

  @Override
  public DoubleStream asDoubleStream() {
    if (is(ORDERED)) {
      if (naturalOrder().equals(getOrdering())) {
        return deriveDouble(LongStream::asDoubleStream, naturalOrder(), with(ORDERED));
      } else {
        return merge().asDoubleStream();
      }
    } else {
      return deriveDouble(LongStream::asDoubleStream, null, without(ORDERED));
    }
  }

  @Override
  public Stream<Long> boxed() {
    return deriveObj(LongStream::boxed);
  }

  @Override
  public PrimitiveIterator.OfLong iterator() {
    return merge().iterator();
  }

  @Override
  public Spliterator.OfLong spliterator() {
    return merge().spliterator();
  }

  @Override
  public LongStream sequential() {
    return new ShardedLongStream(apply(LongStream::sequential).sequential(), getOrdering(), with(), getSource());
  }

  @Override
  public LongStream parallel() {
    return new ShardedLongStream(apply(LongStream::parallel).parallel(), getOrdering(), with(), getSource());
  }

  @Override
  public LongStream unordered() {
    return new ShardedLongStream(apply(LongStream::unordered).unordered(), null, without(ORDERED), getSource());
  }

  /**
   * Derive a new sharded long stream with the same characteristics and ordering.
   *
   * @param operation shard pipeline operation
   * @return a new stream
   */
  private ShardedLongStream derive(Function<LongStream, LongStream> operation) {
    return derive(operation, with());
  }

  /**
   * Derive a new sharded long stream with updated characteristics.
   *
   * @param operation shard pipeline operation
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedLongStream derive(Function<LongStream, LongStream> operation, int characteristics) {
    return derive(operation, getOrdering(), characteristics);
  }

  /**
   * Derive a new sharded long stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedLongStream derive(Function<LongStream, LongStream> operation, Comparator<? super Long> order, int characteristics) {
    return new ShardedLongStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded int stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedIntStream deriveInt(Function<LongStream, IntStream> operation, Comparator<? super Integer> order, int characteristics) {
    return new ShardedIntStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded double stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedDoubleStream deriveDouble(Function<LongStream, DoubleStream> operation, Comparator<? super Double> order, int characteristics) {
    return new ShardedDoubleStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded stream with the same characteristics and ordering.
   *
   * @param operation shard pipeline operation
   * @return a new stream
   */
  private ShardedReferenceStream<Long> deriveObj(Function<LongStream, Stream<Long>> operation) {
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
  private <T> ShardedReferenceStream<T> deriveObj(Function<LongStream, Stream<T>> operation, Comparator<? super T> order, int characteristics) {
    return new ShardedReferenceStream<>(apply(operation), order, characteristics, getSource());
  }

  private static Comparator<OptionalLong> emptiesFirst(Comparator<? super Long> comparator) {
    return Comparator.comparing(OptionalLong::isPresent, (a, b) -> a ? b ? 0 : -1 : 1).thenComparing(OptionalLong::getAsLong, comparator);
  }

  private static Comparator<OptionalLong> emptiesLast(Comparator<? super Long> comparator) {
    return Comparator.comparing(OptionalLong::isPresent, (a, b) -> a ? b ? 0 : 1 : -1).thenComparing(OptionalLong::getAsLong, comparator);
  }
}
