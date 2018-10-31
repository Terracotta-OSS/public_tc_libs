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
package com.terracottatech.store.client.stream;

import com.terracottatech.store.client.RecordImpl;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;
import com.terracottatech.store.intrinsics.impl.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.REFERENCE_STREAM;
import static java.util.Objects.requireNonNull;

/**
 * The TC Store proxy for a (partially) portable {@link Stream} instance.
 *
 * @param <T> the element type of the stream
 */
class RemoteReferenceStream<K extends Comparable<K>, T>
    extends AbstractRemoteBaseStream<K, T, Stream<T>> implements Stream<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteReferenceStream.class);

  /**
   * Creates a new {@code RemoteReferenceStream}.
   * @param rootStream the head of the stream/pipeline chain of which this instance is a part.
   *                   If {@code null}, {@code this} instance becomes the head of the stream/pipeline chain;
   *                   otherwise, identifies the root {@code RootRemoteRecordStream}.
   */
  RemoteReferenceStream(RootRemoteRecordStream<K> rootStream) {
    super(rootStream);
  }

  /**
   * Creates a new {@code RemoteReferenceStream} using the root stream of the specified parent.
   * @param parentStream the {@code AbstractRemoteBaseStream} from which the root stream is obtained
   */
  RemoteReferenceStream(AbstractRemoteBaseStream<K, ?, ? extends BaseStream<?, ?>> parentStream) {
    super(parentStream);
  }

  @Override
  public Stream<T> filter(Predicate<? super T> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FILTER, predicate)) {
      return this;
    }
    return this.createRootObjectStream().filter(predicate);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP, mapper)) {
      // Return this proxy cast as needed
      return (Stream<R>)this;   // unchecked
    }
    return this.createRootObjectStream().map(mapper);
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super T> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP_TO_INT, mapper)) {
      return remoteIntStream();
    }
    return this.createRootObjectStream().mapToInt(mapper);
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP_TO_LONG, mapper)) {
      return remoteLongStream();
    }
    return this.createRootObjectStream().mapToLong(mapper);
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP_TO_DOUBLE, mapper)) {
      return remoteDoubleStream();
    }
    return this.createRootObjectStream().mapToDouble(mapper);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP, mapper)) {
      // Return this proxy cast as needed
      return (Stream<R>)this;     // unchecked
    }
    return this.createRootObjectStream().flatMap(mapper);
  }

  @Override
  public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP_TO_INT, mapper)) {
      return remoteIntStream();
    }
    return this.createRootObjectStream().flatMapToInt(mapper);
  }

  @Override
  public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP_TO_LONG, mapper)) {
      return remoteLongStream();
    }
    return this.createRootObjectStream().flatMapToLong(mapper);
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP_TO_DOUBLE, mapper)) {
      return remoteDoubleStream();
    }
    return this.createRootObjectStream().flatMapToDouble(mapper);
  }

  @Override
  public Stream<T> distinct() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DISTINCT)) {
      return this;
    }
    return this.createRootObjectStream().distinct();
  }

  @Override
  public Stream<T> sorted() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SORTED_0)) {
      setStreamIsOrdered(true);
      return this;
    }
    return this.createRootObjectStream().sorted();
  }

  @Override
  public Stream<T> sorted(Comparator<? super T> comparator) {
    requireNonNull(comparator);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SORTED_1, comparator)) {
      setStreamIsOrdered(true);
      return this;
    }
    return this.createRootObjectStream().sorted(comparator);
  }

  @Override
  public Stream<T> peek(Consumer<? super T> action) {
    requireNonNull(action);
    checkClosed();

    if (tryAddPortable(IntermediateOperation.PEEK, action)) {
      return this;
    }

    LOGGER.warn("A non-portable peek action has made the stream non portable.");
    return this.createRootObjectStream().peek(action);
  }

  @Override
  public Stream<T> limit(long maxSize) {
    if (maxSize < 0) {
      throw new IllegalArgumentException(Long.toString(maxSize));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LIMIT, new Constant<>(maxSize))) {
      return this;
    }
    return this.createRootObjectStream().limit(maxSize);
  }

  @Override
  public Stream<T> skip(long n) {
    if (n < 0) {
      throw new IllegalArgumentException(Long.toString(n));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SKIP, new Constant<>(n))) {
      return this;
    }
    return this.createRootObjectStream().skip(n);
  }

  @Override
  public void forEach(Consumer<? super T> action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootObjectStream().forEach(action);
  }

  @Override
  public void forEachOrdered(Consumer<? super T> action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootObjectStream().forEachOrdered(action);
  }

  @Override
  public Object[] toArray() {
    // Not yet portable
    checkClosed();
    return this.createRootObjectStream().toArray();
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    // Not yet portable
    requireNonNull(generator);
    checkClosed();
    return this.createRootObjectStream().toArray(generator);
  }

  @Override
  public T reduce(T identity, BinaryOperator<T> accumulator) {
    // Not yet portable
    requireNonNull(accumulator);
    checkClosed();
    return this.createRootObjectStream().reduce(identity, accumulator);
  }

  @Override
  public Optional<T> reduce(BinaryOperator<T> accumulator) {
    // Not yet portable
    requireNonNull(accumulator);
    checkClosed();
    return this.createRootObjectStream().reduce(accumulator);
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
    // Not yet portable
    requireNonNull(accumulator);
    requireNonNull(combiner);
    checkClosed();
    return this.createRootObjectStream().reduce(identity, accumulator, combiner);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
    // Not yet portable
    requireNonNull(supplier);
    requireNonNull(accumulator);
    requireNonNull(combiner);
    checkClosed();
    return this.createRootObjectStream().collect(supplier, accumulator, combiner);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    requireNonNull(collector);
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.COLLECT_1, collector)) {
      return (R) executeTerminated().getValue(RecordImpl::toRecord);      // unchecked
    }
    return this.createRootObjectStream().collect(collector);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    requireNonNull(comparator);
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MIN_1, comparator)) {
      return (Optional<T>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return this.createRootObjectStream().min(comparator);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    requireNonNull(comparator);
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MAX_1, comparator)) {
      return (Optional<T>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return this.createRootObjectStream().max(comparator);
  }

  @Override
  public long count() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.COUNT)) {
      return (long) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return this.createRootObjectStream().count();
  }

  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.ANY_MATCH, predicate)) {
      return (boolean) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return this.createRootObjectStream().anyMatch(predicate);
  }

  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.ALL_MATCH, predicate)) {
      return (boolean) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return this.createRootObjectStream().allMatch(predicate);
  }

  @Override
  public boolean noneMatch(Predicate<? super T> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.NONE_MATCH, predicate)) {
      return (boolean) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return this.createRootObjectStream().noneMatch(predicate);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<T> findFirst() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_FIRST)) {
      return (Optional<T>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return this.createRootObjectStream().findFirst();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<T> findAny() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_ANY)) {
      return (Optional<T>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return this.createRootObjectStream().findAny();
  }

  @Override
  public Stream<T> sequential() {
    // Java Stream.sequential() doesn't check closure
    if (tryAddPortable(IntermediateOperation.SEQUENTIAL)) {
      setStreamIsParallel(false);
      return this;
    }
    return this.createRootObjectStream().sequential();
  }

  @Override
  public Stream<T> parallel() {
    // Java Stream.parallel() doesn't check closure
    if (tryAddPortable(IntermediateOperation.PARALLEL)) {
      setStreamIsParallel(true);
      return this;
    }
    return this.createRootObjectStream().parallel();
  }

  @Override
  public Stream<T> unordered() {
    // Java Stream.unordered() checks closure only when the underlying stream is 'ordered'.
    if (isStreamOrdered()) {
      checkClosed();
    }
    if (tryAddPortable(IntermediateOperation.UNORDERED)) {
      setStreamIsOrdered(false);
      return this;
    }
    return this.createRootObjectStream().unordered();
  }

  @Override
  public Iterator<T> iterator() {
    // Can not be portable
    checkClosed();
    return this.createRootObjectStream().iterator();
  }

  @Override
  public Spliterator<T> spliterator() {
    // Can not be portable
    checkClosed();
    return this.createRootObjectStream().spliterator();
  }

  protected IntStream remoteIntStream() {
    return new RemoteIntStream<>(this);
  }

  protected LongStream remoteLongStream() {
    return new RemoteLongStream<>(this);
  }

  protected DoubleStream remoteDoubleStream() {
    return new RemoteDoubleStream<>(this);
  }

  /**
   * Creates the root <i>native</i> stream to handle the client-side, non-portable operations.
   * This method may be called only once within a stream chain.
   * @return a new wrapped, native {@code Stream} on which client-side, non-portable operations
   *        may be anchored
   */
  protected Stream<T> createRootObjectStream() {
    RootRemoteRecordStream<K> rootStream = this.getRootStream();
    if (rootStream.getRootWrappedStream() != null) {
      throw new IllegalStateException("root native stream already created");
    }

    WrappedReferenceStream<T> wrappedStream = createWrappedReferenceStream(rootStream);
    rootStream.setRootWrappedStream(wrappedStream, REFERENCE_STREAM);
    return wrappedStream;
  }

  public WrappedReferenceStream<T> createWrappedReferenceStream(RootRemoteRecordStream<K> rootStream) {
    RemoteSpliteratorSupplier<K, T> spliteratorSupplier = new RemoteSpliteratorSupplier<>(rootStream,
            rootStreamDescriptor -> rootStream.getDataset().objSpliterator(rootStreamDescriptor));
    Stream<T> nativeStream = StreamSupport.stream(spliteratorSupplier,
                                                  getSpliteratorCharacteristics(),
                                                  isStreamParallel());
    WrappedReferenceStream<T> wrappedStream = new ResilientWrappedReferenceStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  public static <K extends Comparable<K> , T> WrappedReferenceStream<T> recreateWrappedReferenceStream(RootRemoteRecordStream<K> rootStream) {
    RemoteSpliteratorSupplier<K, T> spliteratorSupplier = new RemoteSpliteratorSupplier<>(rootStream,
            rootStreamDescriptor -> rootStream.getDataset().objSpliterator(rootStreamDescriptor));
    Stream<T> nativeStream = StreamSupport.stream(spliteratorSupplier,
            rootStream.getSourceSpliterator().characteristics(),
            rootStream.isStreamParallel());
    WrappedReferenceStream<T> wrappedStream = new ResilientWrappedReferenceStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  /**
   * A combination {@link Consumer} and {@link Supplier} used to provide the
   * {@link Spliterator} to a new {@code RecordStream}.
   *
   * @param <K> the key type of the dataset backing the spliterator
   * @param <T> the type of element emitted from the spliterator
   */
  protected static final class RemoteSpliteratorSupplier<K extends Comparable<K>, T>
      extends AbstractRemoteSpliteratorSupplier<K, T, RemoteSpliterator<T>> {

    RemoteSpliteratorSupplier(
        RootRemoteRecordStream<K> recordStream,
        Function<RootStreamDescriptor, RemoteSpliterator<T>> spliteratorGetter) {
      super(recordStream, spliteratorGetter);
    }

    @Override
    public RemoteSpliterator<T> get() {
      return new AutoClosingSpliterator<>(super.get(), getRootStream());
    }

    /**
     * A delegating {@link Spliterator} that closes the associated {@link Stream} once the spliterator is exhausted.
     */
    @SuppressWarnings("try")
    private static final class AutoClosingSpliterator<T>
        extends AbstractAutoClosingSpliterator<T, RemoteSpliterator<T>, Spliterator<T>> {

      private AutoClosingSpliterator(RemoteSpliterator<T> delegate, BaseStream<?, ?> headStream) {
        super(delegate, headStream);
      }

      private AutoClosingSpliterator(AutoClosingSpliterator<T> owner, Spliterator<T> split) {
        super(owner, split);
      }

      @Override
      protected Spliterator<T> chainedSpliterator(Spliterator<T> split) {
        return new AutoClosingSpliterator<>(this, split);
      }
    }
  }
}
