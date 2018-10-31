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

import com.terracottatech.store.common.dataset.stream.WrappedIntStream;

import java.util.Arrays;
import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Function;
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

import static java.util.Comparator.naturalOrder;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;
import static java.util.stream.StreamSupport.intStream;

public final class ShardedIntStream extends AbstractShardedBaseStream<Integer, IntStream> implements IntStream {

  ShardedIntStream(Stream<IntStream> shards, Comparator<? super Integer> ordering, int characteristics, AbstractShardedRecordStream<?, ?> source) {
    super(shards, ordering, characteristics | NONNULL, source);
  }

  private IntStream merge() {
    boolean parallel = isParallel();
    return linkClosureAndMerge(stream -> {
      Supplier<Spliterator.OfInt> spliterator = apply(s -> (Supplier<Spliterator.OfInt>) s::spliterator)
          .reduce((a, b) -> mergeSpliterators(a, b, parallel))
          .orElse(Spliterators::emptyIntSpliterator);

      return new WrappedIntStream(intStream(spliterator, with(), parallel));
    });
  }

  private Supplier<Spliterator.OfInt> mergeSpliterators(Supplier<Spliterator.OfInt> a, Supplier<Spliterator.OfInt> b, boolean parallel) {
    if (is(ORDERED)) {
      return new SortedSpliteratorOfInt.FactoryOfInt(a, b, getOrdering(), with());
    } else {
      return new SequencedSpliteratorOfInt.FactoryOfInt(a, b, with(), parallel);
    }
  }

  @Override
  public IntStream filter(IntPredicate predicate) {
    return derive(s -> s.filter(predicate));
  }

  @Override
  public IntStream map(IntUnaryOperator mapper) {
    if (is(ORDERED)) {
      return merge().map(mapper);
    } else {
      return derive(s -> s.map(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
    if (is(ORDERED)) {
      return merge().mapToObj(mapper);
    } else {
      return deriveObj(s -> s.mapToObj(mapper), null, without(DISTINCT, SORTED, NONNULL));
    }
  }

  @Override
  public LongStream mapToLong(IntToLongFunction mapper) {
    if (is(ORDERED)) {
      return merge().mapToLong(mapper);
    } else {
      return deriveLong(s -> s.mapToLong(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
    if (is(ORDERED)) {
      return merge().mapToDouble(mapper);
    } else {
      return deriveDouble(s -> s.mapToDouble(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public IntStream flatMap(IntFunction<? extends IntStream> mapper) {
    if (is(ORDERED)) {
      return merge().flatMap(mapper);
    } else {
      return derive(s -> s.flatMap(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public IntStream distinct() {
    return derive(IntStream::distinct).merge().distinct();
  }

  @Override
  public IntStream sorted() {
    /*
     * You can't tell the difference between a stable and unstable sort when the whole input value is used in the comparator
     */
    return derive(IntStream::sorted, naturalOrder(), with(SORTED, ORDERED));
  }

  @Override
  public IntStream peek(IntConsumer action) {
    return derive(s -> s.peek(action));
  }

  @Override
  public IntStream limit(long maxSize) {
    return derive(s -> s.limit(maxSize)).merge().limit(maxSize);
  }

  @Override
  public IntStream skip(long n) {
    return merge().skip(n);
  }

  @Override
  public void forEach(IntConsumer action) {
    consume(s -> s.forEach(action));
  }

  @Override
  public void forEachOrdered(IntConsumer action) {
    if (is(ORDERED)) {
      merge().forEachOrdered(action);
    } else {
      forEach(action);
    }
  }

  @Override
  public int[] toArray() {
    if (is(ORDERED)) {
      return merge().toArray();
    } else {
      return terminateWith(IntStream::toArray).reduce(explainedAs("array-merging", (a, b) -> {
        int[] c = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
      })).orElse(new int[0]);
    }
  }

  @Override
  public int reduce(int identity, IntBinaryOperator op) {
    if (is(ORDERED)) {
      return merge().reduce(identity, op);
    } else {
      return terminateWithInt(s -> s.reduce(identity, op)).reduce(identity, explainedAs("final-reduction: {0}", op));
    }
  }

  @Override
  public OptionalInt reduce(IntBinaryOperator op) {
    if (is(ORDERED)) {
      return merge().reduce(op);
    } else {
      return terminateWith(s -> s.reduce(op)).filter(OptionalInt::isPresent).mapToInt(OptionalInt::getAsInt).reduce(explainedAs("final-reduction: {0}", op));
    }
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
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
  public int sum() {
    return terminateWithInt(IntStream::sum).sum();
  }

  @Override
  public OptionalInt min() {
    return terminateWith(IntStream::min).min(explainedAs("emptiesLast(naturalOrder())", emptiesLast(naturalOrder()))).orElse(OptionalInt.empty());
  }

  @Override
  public OptionalInt max() {
    return terminateWith(IntStream::max).max(explainedAs("emptiesFirst(naturalOrder())", emptiesFirst(naturalOrder()))).orElse(OptionalInt.empty());
  }

  @Override
  public long count() {
    return terminateWithLong(IntStream::count).sum();
  }

  @Override
  public OptionalDouble average() {
    IntSummaryStatistics summary = summaryStatistics();
    if (summary.getCount() == 0) {
      return OptionalDouble.empty();
    } else {
      return OptionalDouble.of(summary.getAverage());
    }
  }

  @Override
  public IntSummaryStatistics summaryStatistics() {
    return terminateWith(IntStream::summaryStatistics).reduce(explainedAs("IntSummaryStatistics::combine", (a, b) -> {
      a.combine(b);
      return a;
    })).orElse(new IntSummaryStatistics());
  }

  @Override
  public boolean anyMatch(IntPredicate predicate) {
    return terminateWith(s -> s.anyMatch(predicate)).anyMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean allMatch(IntPredicate predicate) {
    return terminateWith(s -> s.allMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean noneMatch(IntPredicate predicate) {
    return terminateWith(s -> s.noneMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public OptionalInt findFirst() {
    if (is(ORDERED)) {
      return terminateWith(IntStream::findFirst).min(explainedAs("emptiesLast(" + getOrdering() + ")", emptiesLast(getOrdering()))).orElse(OptionalInt.empty());
    } else {
      return findAny();
    }
  }

  @Override
  public OptionalInt findAny() {
    return terminateWith(IntStream::findAny).filter(explainedAs("OptionalInt::isPresent", OptionalInt::isPresent)).findAny().orElse(OptionalInt.empty());
  }

  @Override
  public LongStream asLongStream() {
    if (is(ORDERED)) {
      if (naturalOrder().equals(getOrdering())) {
        return deriveLong(IntStream::asLongStream, naturalOrder(), with(ORDERED));
      } else {
        return merge().asLongStream();
      }
    } else {
      return deriveLong(IntStream::asLongStream, null, without(ORDERED));
    }
  }

  @Override
  public DoubleStream asDoubleStream() {
    if (is(ORDERED)) {
      if (naturalOrder().equals(getOrdering())) {
        return deriveDouble(IntStream::asDoubleStream, naturalOrder(), with(ORDERED));
      } else {
        return merge().asDoubleStream();
      }
    } else {
      return deriveDouble(IntStream::asDoubleStream, null, without(ORDERED));
    }
  }

  @Override
  public Stream<Integer> boxed() {
    return deriveObj(IntStream::boxed);
  }

  @Override
  public PrimitiveIterator.OfInt iterator() {
    return merge().iterator();
  }

  @Override
  public Spliterator.OfInt spliterator() {
    return merge().spliterator();
  }

  @Override
  public IntStream sequential() {
    return new ShardedIntStream(apply(IntStream::sequential).sequential(), getOrdering(), with(), getSource());
  }

  @Override
  public IntStream parallel() {
    return new ShardedIntStream(apply(IntStream::parallel).parallel(), getOrdering(), with(), getSource());
  }

  @Override
  public IntStream unordered() {
    return new ShardedIntStream(apply(IntStream::unordered).unordered(), null, without(ORDERED), getSource());
  }

  /**
   * Derive a new sharded int stream with the same characteristics and ordering.
   *
   * @param operation shard pipeline operation
   * @return a new stream
   */
  private ShardedIntStream derive(Function<IntStream, IntStream> operation) {
    return derive(operation, with());
  }

  /**
   * Derive a new sharded int stream with updated characteristics.
   *
   * @param operation shard pipeline operation
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedIntStream derive(Function<IntStream, IntStream> operation, int characteristics) {
    return derive(operation, getOrdering(), characteristics);
  }

  /**
   * Derive a new sharded int stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedIntStream derive(Function<IntStream, IntStream> operation, Comparator<? super Integer> order, int characteristics) {
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
  private ShardedLongStream deriveLong(Function<IntStream, LongStream> operation, Comparator<? super Long> order, int characteristics) {
    return new ShardedLongStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded double stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  private ShardedDoubleStream deriveDouble(Function<IntStream, DoubleStream> operation, Comparator<? super Double> order, int characteristics) {
    return new ShardedDoubleStream(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded stream with the same characteristics and ordering.
   *
   * @param operation shard pipeline operation
   * @return a new stream
   */
  private ShardedReferenceStream<Integer> deriveObj(Function<IntStream, Stream<Integer>> operation) {
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
  private <T> ShardedReferenceStream<T> deriveObj(Function<IntStream, Stream<T>> operation, Comparator<? super T> order, int characteristics) {
    return new ShardedReferenceStream<>(apply(operation), order, characteristics, getSource());
  }

  private static Comparator<OptionalInt> emptiesFirst(Comparator<? super Integer> comparator) {
    return Comparator.comparing(OptionalInt::isPresent, (a, b) -> a ? b ? 0 : -1 : 1).thenComparing(OptionalInt::getAsInt, comparator);
  }

  private static Comparator<OptionalInt> emptiesLast(Comparator<? super Integer> comparator) {
    return Comparator.comparing(OptionalInt::isPresent, (a, b) -> a ? b ? 0 : 1 : -1).thenComparing(OptionalInt::getAsInt, comparator);
  }
}
