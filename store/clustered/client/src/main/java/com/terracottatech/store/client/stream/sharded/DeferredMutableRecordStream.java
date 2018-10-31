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

import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import java.util.stream.*;

import static com.terracottatech.store.client.stream.AbstractRemoteBaseStream.STREAM_USED_OR_CLOSED;

/**
 * Provides for deferral of stream operation until the mutativeness of a record stream can be determined.
 * <p>
 * This record stream implementation maintains two independent suppliers of a future pipeline allowing deferral of the
 * pipeline construction until the stream is known to be mutative (or not).  For now this allows us to use different
 * skip and limit implementations depending on whether the stream ends up being mutative or not.
 *
 * @param <K> record key type
 */
class DeferredMutableRecordStream<K extends Comparable<K>> implements MutableRecordStream<K> {

  private final Supplier<RecordStream<K>> regularPipeline;
  private final Supplier<MutableRecordStream<K>> mutativePipeline;
  private final Function<Stream<?>, Stream<?>> mutativePostPipeline;
  private final boolean parallel;

  private boolean linkedOrConsumed;

  DeferredMutableRecordStream(RecordStream<K> record, MutableRecordStream<K> mutable) {
    this(() -> record, () -> mutable, Function.identity(), record.isParallel());
  }

  private DeferredMutableRecordStream(Supplier<RecordStream<K>> regularPipeline,
                                      Supplier<MutableRecordStream<K>> mutativePipeline,
                                      Function<Stream<?>, Stream<?>> mutativePostPipeline,
                                      boolean parallel) {
    this.regularPipeline = regularPipeline;
    this.mutativePipeline = mutativePipeline;
    this.mutativePostPipeline = mutativePostPipeline;
    this.parallel = parallel;
  }

  @SuppressWarnings("unchecked")
  private <U> Stream<U> mutative(Function<MutableRecordStream<K>, Stream<U>> operation) {
    return (Stream<U>) operation.andThen(mutativePostPipeline).apply(mutativePipeline());
  }

  private <U> U nonMutative(Function<RecordStream<K>, U> operation) {
    return operation.apply(nonMutativePipeline());
  }

  private void consumeNonMutative(Consumer<RecordStream<K>> operation) {
    operation.accept(nonMutativePipeline());
  }

  private MutableRecordStream<K> andDefer(Function<? super RecordStream<K>, ? extends RecordStream<K>> regular,
                                          Function<? super MutableRecordStream<K>, ? extends MutableRecordStream<K>> mutative) {
    return andDefer(regular, mutative, isParallel());
  }

  private MutableRecordStream<K> andDefer(Function<? super RecordStream<K>, ? extends RecordStream<K>> regular,
                                          Function<? super MutableRecordStream<K>, ? extends MutableRecordStream<K>> mutative,
                                          boolean parallel) {
    return andDefer(regular, mutative, UnaryOperator.identity(), parallel);
  }

  private MutableRecordStream<K> andDefer(Function<? super RecordStream<K>, ? extends RecordStream<K>> regular,
                                          Function<? super MutableRecordStream<K>, ? extends MutableRecordStream<K>> mutative,
                                          UnaryOperator<Stream<?>> postMutative) {
    return andDefer(regular, mutative, postMutative, isParallel());
  }

  private MutableRecordStream<K> andDefer(Function<? super RecordStream<K>, ? extends RecordStream<K>> regular,
                                          Function<? super MutableRecordStream<K>, ? extends MutableRecordStream<K>> mutative,
                                          UnaryOperator<Stream<?>> postMutative, boolean parallel) {
    if (linkedOrConsumed) {
      throw new IllegalStateException(STREAM_USED_OR_CLOSED);
    } else {
      linkedOrConsumed = true;
      return new DeferredMutableRecordStream<>(() -> regular.apply(regularPipeline.get()), () -> mutative.apply(mutativePipeline.get()), mutativePostPipeline.andThen(postMutative), parallel);
    }
  }

  @Override
  public void mutate(UpdateOperation<? super K> transform) {
    mutative(s -> s.mutateThen(transform)).count();
  }

  @Override
  public Stream<Tuple<Record<K>, Record<K>>> mutateThen(UpdateOperation<? super K> transform) {
    return mutative(s -> s.mutateThen(transform));
  }

  @Override
  public void delete() {
    mutative(MutableRecordStream::deleteThen).count();
  }

  @Override
  public Stream<Record<K>> deleteThen() {
    return mutative(MutableRecordStream::deleteThen);
  }

  @Override
  public MutableRecordStream<K> filter(Predicate<? super Record<K>> predicate) {
    return andDefer(s -> s.filter(predicate), s -> s.filter(predicate));
  }

  @Override
  public <R> Stream<R> map(Function<? super Record<K>, ? extends R> mapper) {
    return nonMutative(s -> s.map(mapper));
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super Record<K>> mapper) {
    return nonMutative(s -> s.mapToInt(mapper));
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super Record<K>> mapper) {
    return nonMutative(s -> s.mapToLong(mapper));
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super Record<K>> mapper) {
    return nonMutative(s -> s.mapToDouble(mapper));
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super Record<K>, ? extends Stream<? extends R>> mapper) {
    return nonMutative(s -> s.flatMap(mapper));
  }

  @Override
  public IntStream flatMapToInt(Function<? super Record<K>, ? extends IntStream> mapper) {
    return nonMutative(s -> s.flatMapToInt(mapper));
  }

  @Override
  public LongStream flatMapToLong(Function<? super Record<K>, ? extends LongStream> mapper) {
    return nonMutative(s -> s.flatMapToLong(mapper));
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super Record<K>, ? extends DoubleStream> mapper) {
    return nonMutative(s -> s.flatMapToDouble(mapper));
  }

  @Override
  public MutableRecordStream<K> distinct() {
    return this;
  }

  @Override
  public RecordStream<K> sorted() {
    return nonMutative(RecordStream::sorted);
  }

  @Override
  public RecordStream<K> sorted(Comparator<? super Record<K>> comparator) {
    return nonMutative(s -> s.sorted(comparator));
  }

  @Override
  public MutableRecordStream<K> peek(Consumer<? super Record<K>> action) {
    return andDefer(s -> s.peek(action), s -> s.peek(action));
  }

  @Override
  public MutableRecordStream<K> limit(long maxSize) {
    AtomicLong seen = new AtomicLong();
    Predicate<? super Record<K>> predicate = r -> seen.incrementAndGet() <= maxSize;
    return andDefer(
        s -> s.limit(maxSize),
        s -> s.filter(predicate), s -> s.limit(maxSize)
    );
  }

  @Override
  public MutableRecordStream<K> skip(long n) {
    AtomicLong seen = new AtomicLong();
    Predicate<? super Record<K>> predicate = r -> seen.incrementAndGet() > n;
    return andDefer(
        s -> s.skip(n),
        s -> s.filter(predicate)
    );
  }

  @Override
  public void forEach(Consumer<? super Record<K>> action) {
    consumeNonMutative(s -> s.forEach(action));
  }

  @Override
  public void forEachOrdered(Consumer<? super Record<K>> action) {
    consumeNonMutative(s -> s.forEachOrdered(action));
  }

  @Override
  public Object[] toArray() {
    return nonMutative(Stream::toArray);
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    return nonMutative(s -> s.toArray(generator));
  }

  @Override
  public Record<K> reduce(Record<K> identity, BinaryOperator<Record<K>> accumulator) {
    return nonMutative(s -> s.reduce(identity, accumulator));
  }

  @Override
  public Optional<Record<K>> reduce(BinaryOperator<Record<K>> accumulator) {
    return nonMutative(s -> s.reduce(accumulator));
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super Record<K>, U> accumulator, BinaryOperator<U> combiner) {
    return nonMutative(s -> s.reduce(identity, accumulator, combiner));
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super Record<K>> accumulator, BiConsumer<R, R> combiner) {
    return nonMutative(s -> s.collect(supplier, accumulator, combiner));
  }

  @Override
  public <R, A> R collect(Collector<? super Record<K>, A, R> collector) {
    return nonMutative(s -> s.collect(collector));
  }

  @Override
  public Optional<Record<K>> min(Comparator<? super Record<K>> comparator) {
    return nonMutative(s -> s.min(comparator));
  }

  @Override
  public Optional<Record<K>> max(Comparator<? super Record<K>> comparator) {
    return nonMutative(s -> s.max(comparator));
  }

  @Override
  public long count() {
    return nonMutative(Stream::count);
  }

  @Override
  public boolean anyMatch(Predicate<? super Record<K>> predicate) {
    return nonMutative(s -> s.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(Predicate<? super Record<K>> predicate) {
    return nonMutative(s -> s.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(Predicate<? super Record<K>> predicate) {
    return nonMutative(s -> s.noneMatch(predicate));
  }

  @Override
  public Optional<Record<K>> findFirst() {
    return nonMutative(Stream::findFirst);
  }

  @Override
  public Optional<Record<K>> findAny() {
    return nonMutative(Stream::findAny);
  }

  @Override
  public Iterator<Record<K>> iterator() {
    return nonMutative(BaseStream::iterator);
  }

  @Override
  public Spliterator<Record<K>> spliterator() {
    return nonMutative(BaseStream::spliterator);
  }

  @Override
  public boolean isParallel() {
    return parallel;
  }

  @Override
  public MutableRecordStream<K> sequential() {
    return andDefer(RecordStream::sequential, MutableRecordStream::sequential, false);
  }

  @Override
  public MutableRecordStream<K> parallel() {
    return andDefer(RecordStream::parallel, MutableRecordStream::parallel, true);
  }

  @Override
  public MutableRecordStream<K> unordered() {
    return andDefer(RecordStream::unordered, MutableRecordStream::unordered);
  }

  @Override
  public MutableRecordStream<K> onClose(Runnable closeHandler) {
    return andDefer(s -> s.onClose(closeHandler), s -> s.onClose(closeHandler));
  }

  @Override
  public void close() {
    consumeNonMutative(RecordStream::close);
  }

  @Override
  public MutableRecordStream<K> explain(Consumer<Object> consumer) {
    return andDefer(s -> s.explain(consumer), s -> s.explain(consumer));
  }

  @Override
  public MutableRecordStream<K> batch(int sizeHint) {
    return andDefer(s -> s.batch(sizeHint), s -> s.batch(sizeHint));
  }

  @Override
  public MutableRecordStream<K> inline() {
    return andDefer(RecordStream::inline, MutableRecordStream::inline);
  }

  private MutableRecordStream<K> mutativePipeline() {
    linkedOrConsumed = true;
    return mutativePipeline.get();
  }

  private RecordStream<K> nonMutativePipeline() {
    linkedOrConsumed = true;
    return regularPipeline.get();
  }
}
