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

import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
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
import java.util.stream.Collector.Characteristics;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Comparator.naturalOrder;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;
import static java.util.stream.StreamSupport.stream;

public class ShardedReferenceStream<T> extends AbstractShardedBaseStream<T, Stream<T>> implements Stream<T> {

  ShardedReferenceStream(Collection<? extends Stream<T>> shards, Comparator<? super T> ordering, int characteristics) {
    super(shards, ordering, characteristics);
  }

  ShardedReferenceStream(Stream<Stream<T>> shards, Comparator<? super T> ordering, int characteristics, AbstractShardedRecordStream<?, ?> source) {
    super(shards, ordering, characteristics, source);
  }

  private Stream<T> merge() {
    boolean parallel = isParallel();
    return linkClosureAndMerge(stream -> {
      Supplier<Spliterator<T>> spliterator = apply(s -> (Supplier<Spliterator<T>>) s::spliterator)
          .reduce((a, b) -> mergeSpliterators(a, b, parallel))
          .orElse(Spliterators::emptySpliterator);

      return new WrappedReferenceStream<>(stream(spliterator, with(), parallel));
    });
  }

  private Supplier<Spliterator<T>> mergeSpliterators(Supplier<Spliterator<T>> a, Supplier<Spliterator<T>> b, boolean parallel) {
    if (is(ORDERED)) {
      return new SortedSpliterator.FactoryOfObj<>(a, b, getOrdering(), with());
    } else {
      return new SequencedSpliterator.FactoryOfObj<>(a, b, with(), parallel);
    }
  }

  @Override
  public Stream<T> filter(Predicate<? super T> predicate) {
    return derive(s -> s.filter(predicate));
  }

  @Override
  public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
    if (is(ORDERED)) {
      return merge().map(mapper);
    } else {
      return derive(s -> s.map(mapper), null, without(DISTINCT, SORTED, NONNULL));
    }
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super T> mapper) {
    if (is(ORDERED)) {
      return merge().mapToInt(mapper);
    } else {
      return deriveInt(s -> s.mapToInt(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    if (is(ORDERED)) {
      return merge().mapToLong(mapper);
    } else {
      return deriveLong(s -> s.mapToLong(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    if (is(ORDERED)) {
      return merge().mapToDouble(mapper);
    } else {
      return deriveDouble(s -> s.mapToDouble(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    if (is(ORDERED)) {
      return merge().flatMap(mapper);
    } else {
      return derive(s -> s.flatMap(mapper), null, without(DISTINCT, SORTED, NONNULL));
    }
  }

  @Override
  public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    if (is(ORDERED)) {
      return merge().flatMapToInt(mapper);
    } else {
      return deriveInt(s -> s.flatMapToInt(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    if (is(ORDERED)) {
      return merge().flatMapToLong(mapper);
    } else {
      return deriveLong(s -> s.flatMapToLong(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    if (is(ORDERED)) {
      return merge().flatMapToDouble(mapper);
    } else {
      return deriveDouble(s -> s.flatMapToDouble(mapper), null, without(DISTINCT, SORTED));
    }
  }

  @Override
  public Stream<T> distinct() {
    return derive(Stream::distinct).merge().distinct();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Stream<T> sorted() {
    Comparator<T> ordering = (Comparator<T>) naturalOrder();
    if (is(ORDERED)) {
      ordering = ordering.thenComparing(getOrdering());
    }
    return derive(Stream::sorted, ordering, with(SORTED, ORDERED));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Stream<T> sorted(Comparator<? super T> comparator) {
    Comparator<T> ordering = (Comparator<T>) comparator;
    if (is(ORDERED)) {
      ordering = ordering.thenComparing(getOrdering());
    }
    return derive(s -> s.sorted(comparator), ordering, with(SORTED, ORDERED));
  }

  @Override
  public Stream<T> peek(Consumer<? super T> action) {
    return derive(s -> s.peek(action));
  }

  @Override
  public Stream<T> limit(long maxSize) {
    return derive(s -> s.limit(maxSize)).merge().limit(maxSize);
  }

  @Override
  public Stream<T> skip(long n) {
    return merge().skip(n);
  }

  @Override
  public void forEach(Consumer<? super T> action) {
    consume(s -> s.forEach(action));
  }

  @Override
  public void forEachOrdered(Consumer<? super T> action) {
    if (is(ORDERED)) {
      merge().forEachOrdered(action);
    } else {
      forEach(action);
    }
  }

  @Override
  public Object[] toArray() {
    if (is(ORDERED)) {
      return merge().toArray();
    } else {
      return terminateWith(Stream::toArray).reduce(explainedAs("array-merging", (a, b) -> {
        Object[] c = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
      })).orElse(new Object[0]);
    }
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    if (is(ORDERED)) {
      return merge().toArray(generator);
    } else {
      return terminateWith(s -> s.toArray(generator)).reduce(explainedAs("array-merging", (a, b) -> {
        A[] c = generator.apply(a.length + b.length);
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
      })).orElse(generator.apply(0));
    }
  }

  @Override
  public T reduce(T identity, BinaryOperator<T> accumulator) {
    if (is(ORDERED)) {
      return merge().reduce(identity, accumulator);
    } else {
      return terminateWith(s -> s.reduce(identity, accumulator)).reduce(identity, explainedAs("final-reduction: {0}", accumulator));
    }
  }

  @Override
  public Optional<T> reduce(BinaryOperator<T> accumulator) {
    if (is(ORDERED)) {
      return merge().reduce(accumulator);
    } else {
      return terminateWith(s -> s.reduce(accumulator)).filter(Optional::isPresent).map(Optional::get).reduce(explainedAs("final-reduction: {0}", accumulator));
    }
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
    if (is(ORDERED)) {
      return merge().reduce(identity, accumulator, combiner);
    } else {
      return terminateWith(s -> s.reduce(identity, accumulator, combiner)).reduce(identity, explainedAs("final-reduction: {0}", combiner));
    }
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
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
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    Set<Characteristics> characteristics = collector.characteristics();

    if (is(ORDERED) && !characteristics.contains(Characteristics.UNORDERED)) {
      //ordered stream and collector purports to preserve ordering
      return merge().collect(collector);
    } else if (collector.characteristics().contains(Characteristics.IDENTITY_FINISH)) {
      @SuppressWarnings("unchecked") BinaryOperator<R> combiner = (BinaryOperator<R>) collector.combiner();
      return terminateWith(s -> s.collect(collector)).reduce(explainedAs("final-combining: {0}", combiner)).orElse(Stream.<T>empty().collect(collector));
    } else {
      Collector<? super T, A, A> shardCollector = Collector.of(collector.supplier(), collector.accumulator(), collector.combiner(), characteristics.toArray(new Characteristics[0]));
      return terminateWith(s -> s.collect(shardCollector)).reduce(explainedAs("final-combining: {0}", collector.combiner()))
          .map(explainedAs("finishing: {0}", collector.finisher())).orElse(Stream.<T>empty().collect(collector));
    }
  }

  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    return terminateWith(s -> s.min(comparator)).min(explainedAs("emptiesLast(" + comparator + ")", emptiesLast(comparator))).orElse(Optional.empty());
  }

  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    return terminateWith(s -> s.max(comparator)).max(explainedAs("emptiesLast(" + comparator + ")", emptiesFirst(comparator))).orElse(Optional.empty());
  }

  @Override
  public long count() {
    return terminateWithLong(Stream::count).sum();
  }

  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    return terminateWith(s -> s.anyMatch(predicate)).anyMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    return terminateWith(s -> s.allMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public boolean noneMatch(Predicate<? super T> predicate) {
    return terminateWith(s -> s.noneMatch(predicate)).allMatch(explainedAs("Boolean.TRUE::equals", Boolean.TRUE::equals));
  }

  @Override
  public Optional<T> findFirst() {
    if (is(ORDERED)) {
      return terminateWith(Stream::findFirst).min(explainedAs("emptiesLast(" + getOrdering() + ")", emptiesLast(getOrdering()))).orElse(Optional.empty());
    } else {
      return findAny();
    }
  }

  @Override
  public Optional<T> findAny() {
    return terminateWith(Stream::findAny).filter(explainedAs("Optional::isPresent", Optional::isPresent)).findAny().orElse(Optional.empty());
  }

  @Override
  public Iterator<T> iterator() {
    return merge().iterator();
  }

  @Override
  public Spliterator<T> spliterator() {
    return merge().spliterator();
  }

  @Override
  public Stream<T> sequential() {
    return new ShardedReferenceStream<>(apply(Stream::sequential).sequential(), getOrdering(), with(), getSource());
  }

  @Override
  public Stream<T> parallel() {
    return new ShardedReferenceStream<>(apply(Stream::parallel).parallel(), getOrdering(), with(), getSource());
  }

  @Override
  public Stream<T> unordered() {
    return new ShardedReferenceStream<>(apply(Stream::unordered).unordered(), null, without(ORDERED), getSource());
  }

  /**
   * Derive a new sharded stream with the same characteristics and ordering.
   *
   * @param operation shard pipeline operation
   * @return a new stream
   */
  ShardedReferenceStream<T> derive(Function<Stream<T>, Stream<T>> operation) {
    return derive(operation, with());
  }

  /**
   * Derive a new sharded stream with updated characteristics.
   *
   * @param operation shard pipeline operation
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  ShardedReferenceStream<T> derive(Function<Stream<T>, Stream<T>> operation, int characteristics) {
    return derive(operation, getOrdering(), characteristics);
  }

  /**
   * Derive a new sharded stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  <U> ShardedReferenceStream<U> derive(Function<Stream<T>, Stream<U>> operation, Comparator<? super U> order, int characteristics) {
    return new ShardedReferenceStream<>(apply(operation), order, characteristics, getSource());
  }

  /**
   * Derive a new sharded int stream with updated characteristics and ordering comparator.
   *
   * @param operation shard pipeline operation
   * @param order ordering comparator
   * @param characteristics new stream characteristics
   * @return a new stream
   */
  ShardedIntStream deriveInt(Function<Stream<T>, IntStream> operation, Comparator<? super Integer> order, int characteristics) {
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
  ShardedLongStream deriveLong(Function<Stream<T>, LongStream> operation, Comparator<? super Long> order, int characteristics) {
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
  ShardedDoubleStream deriveDouble(Function<Stream<T>, DoubleStream> operation, Comparator<? super Double> order, int characteristics) {
    return new ShardedDoubleStream(apply(operation), order, characteristics, getSource());
  }

  private static <T> Comparator<Optional<T>> emptiesFirst(Comparator<? super T> comparator) {
    return Comparator.<Optional<T>, Boolean>comparing(Optional::isPresent, (a, b) -> a ? b ? 0 : -1 : 1).thenComparing(Optional::get, comparator);
  }

  private static <T> Comparator<Optional<T>> emptiesLast(Comparator<? super T> comparator) {
    return Comparator.<Optional<T>, Boolean>comparing(Optional::isPresent, (a, b) -> a ? b ? 0 : 1 : -1).thenComparing(Optional::get, comparator);
  }
}
