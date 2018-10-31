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

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.WrappedIntStream;
import com.terracottatech.store.intrinsics.impl.Constant;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterator.OfInt;
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
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.INT_STREAM;
import static java.util.Objects.requireNonNull;

/**
 * The TC Store proxy for a (partially) portable {@link IntStream} instance.
 *
 * @param <K> the key type of the dataset anchoring this stream
 */
class RemoteIntStream<K extends Comparable<K>>
    extends AbstractRemoteBaseStream<K, Integer, IntStream>
    implements IntStream {

  RemoteIntStream(AbstractRemoteBaseStream<K, ?, ? extends BaseStream<?, ?>> parentStream) {
    super(parentStream);
  }

  @Override
  public IntStream filter(IntPredicate predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.INT_FILTER, predicate)) {
      return this;
    }
    return this.createRootIntStream().filter(predicate);
  }

  @Override
  public IntStream map(IntUnaryOperator mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.INT_MAP, mapper)) {
      return this;
    }
    return this.createRootIntStream().map(mapper);
  }

  @Override
  public <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.INT_MAP_TO_OBJ, mapper)) {
      return remoteReferenceStream();
    }
    return this.createRootIntStream().mapToObj(mapper);
  }

  protected  <U> Stream<U> remoteReferenceStream() {
    return new RemoteReferenceStream<>(this);
  }

  protected DoubleStream remoteDoubleStream() {
    return new RemoteDoubleStream<>(this);
  }

  protected LongStream remoteLongStream() {
    return new RemoteLongStream<>(this);
  }

  @Override
  public LongStream mapToLong(IntToLongFunction mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.INT_MAP_TO_LONG, mapper)) {
      return remoteLongStream();
    }
    return this.createRootIntStream().mapToLong(mapper);
  }

  @Override
  public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.INT_MAP_TO_DOUBLE, mapper)) {
      return remoteDoubleStream();
    }
    return this.createRootIntStream().mapToDouble(mapper);
  }

  @Override
  public IntStream flatMap(IntFunction<? extends IntStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.INT_FLAT_MAP, mapper)) {
      return this;
    }
    return this.createRootIntStream().flatMap(mapper);
  }

  @Override
  public IntStream distinct() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DISTINCT)) {
      return this;
    }
    return this.createRootIntStream().distinct();
  }

  @Override
  public IntStream sorted() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SORTED_0)) {
      setStreamIsOrdered(true);
      return this;
    }
    return this.createRootIntStream().sorted();
  }

  @Override
  public IntStream peek(IntConsumer action) {
    // Peek isn't poartable
    requireNonNull(action);
    checkClosed();
    return this.createRootIntStream().peek(action);
  }

  @Override
  public IntStream limit(long maxSize) {
    if (maxSize < 0) {
      throw new IllegalArgumentException(Long.toString(maxSize));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LIMIT, new Constant<>(maxSize))) {
      return this;
    }
    return this.createRootIntStream().limit(maxSize);
  }

  @Override
  public IntStream skip(long n) {
    if (n < 0) {
      throw new IllegalArgumentException(Long.toString(n));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SKIP, new Constant<>(n))) {
      return this;
    }
    return this.createRootIntStream().skip(n);
  }

  @Override
  public void forEach(IntConsumer action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootIntStream().forEach(action);
  }

  @Override
  public void forEachOrdered(IntConsumer action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootIntStream().forEachOrdered(action);
  }

  @Override
  public int[] toArray() {
    // Not yet portable
    checkClosed();
    return this.createRootIntStream().toArray();
  }

  @Override
  public int reduce(int identity, IntBinaryOperator op) {
    // Not yet portable
    requireNonNull(op);
    checkClosed();
    return this.createRootIntStream().reduce(identity, op);
  }

  @Override
  public OptionalInt reduce(IntBinaryOperator op) {
    // Not yet portable
    requireNonNull(op);
    checkClosed();
    return this.createRootIntStream().reduce(op);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    // Not yet portable
    requireNonNull(supplier);
    requireNonNull(accumulator);
    requireNonNull(combiner);
    checkClosed();
    return this.createRootIntStream().collect(supplier, accumulator, combiner);
  }

  @Override
  public int sum() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.SUM)) {
      return (int) executeTerminated().getValue();
    }
    return this.createRootIntStream().sum();
  }

  @Override
  public OptionalInt min() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MIN_0)) {
      return (OptionalInt) executeTerminated().getValue();
    }
    return this.createRootIntStream().min();
  }

  @Override
  public OptionalInt max() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MAX_0)) {
      return (OptionalInt) executeTerminated().getValue();
    }
    return this.createRootIntStream().max();
  }

  @Override
  public long count() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.COUNT)) {
      return (long) executeTerminated().getValue();
    }
    return this.createRootIntStream().count();
  }

  @Override
  public OptionalDouble average() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.AVERAGE)) {
      return (OptionalDouble) executeTerminated().getValue();
    }
    return this.createRootIntStream().average();
  }

  @Override
  public IntSummaryStatistics summaryStatistics() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.SUMMARY_STATISTICS)) {
      return (IntSummaryStatistics) executeTerminated().getValue();
    }
    return this.createRootIntStream().summaryStatistics();
  }

  @Override
  public boolean anyMatch(IntPredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootIntStream().anyMatch(predicate);
  }

  @Override
  public boolean allMatch(IntPredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootIntStream().allMatch(predicate);
  }

  @Override
  public boolean noneMatch(IntPredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootIntStream().noneMatch(predicate);
  }

  @Override
  public OptionalInt findFirst() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_FIRST)) {
      return (OptionalInt) executeTerminated().getValue();
    }
    return this.createRootIntStream().findFirst();
  }

  @Override
  public OptionalInt findAny() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_ANY)) {
      return (OptionalInt) executeTerminated().getValue();
    }
    return this.createRootIntStream().findAny();
  }

  @Override
  public LongStream asLongStream() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.AS_LONG_STREAM)) {
      return remoteLongStream();
    }
    return this.createRootIntStream().asLongStream();
  }

  @Override
  public DoubleStream asDoubleStream() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.AS_DOUBLE_STREAM)) {
      return remoteDoubleStream();
    }
    return this.createRootIntStream().asDoubleStream();
  }

  @Override
  public Stream<Integer> boxed() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.BOXED)) {
      return remoteReferenceStream();
    }
    return this.createRootIntStream().boxed();
  }

  @Override
  public PrimitiveIterator.OfInt iterator() {
    // Can not be portable
    checkClosed();
    return this.createRootIntStream().iterator();
  }

  @Override
  public OfInt spliterator() {
    // Can not be portable
    checkClosed();
    return this.createRootIntStream().spliterator();
  }

  @Override
  public IntStream sequential() {
    // Java IntStream.sequential() doesn't check closure
    if (tryAddPortable(IntermediateOperation.SEQUENTIAL)) {
      setStreamIsParallel(false);
      return this;
    }
    return this.createRootIntStream().sequential();
  }

  @Override
  public IntStream parallel() {
    // Java IntStream.parallel() doesn't check closure
    if (tryAddPortable(IntermediateOperation.PARALLEL)) {
      setStreamIsParallel(true);
      return this;
    }
    return this.createRootIntStream().parallel();
  }

  @Override
  public IntStream unordered() {
    // Java IntStream.unordered() checks closure only when the underlying stream is 'ordered'.
    if (isStreamOrdered()) {
      checkClosed();
    }
    if (tryAddPortable(IntermediateOperation.UNORDERED)) {
      setStreamIsOrdered(false);
      return this;
    }
    return this.createRootIntStream().unordered();
  }

  /**
   * Creates the root <i>native</i> stream to handle the client-side, non-portable operations.
   * This method may be called only once within a stream chain.
   * @return a new wrapped, native {@code IntStream} on which client-side, non-portable operations
   *        may be anchored
   */
  private IntStream createRootIntStream() {
    RootRemoteRecordStream<K> rootStream = this.getRootStream();
    if (rootStream.getRootWrappedStream() != null) {
      throw new IllegalStateException("root native stream already created");
    }

    WrappedIntStream wrappedStream = createWrappedIntStream(rootStream);
    rootStream.setRootWrappedStream(wrappedStream, INT_STREAM);
    return wrappedStream;
  }

  public WrappedIntStream createWrappedIntStream(RootRemoteRecordStream<K> rootStream) {
    RemoteIntSpliteratorSupplier<K> spliteratorSupplier =
        new RemoteIntSpliteratorSupplier<>(rootStream,
            rootStreamDescriptor -> rootStream.getDataset().intSpliterator(rootStreamDescriptor));
    IntStream nativeStream = StreamSupport.intStream(spliteratorSupplier,
                                                      getSpliteratorCharacteristics(),
                                                      isStreamParallel());
    WrappedIntStream wrappedStream = new ResilientWrappedIntStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  public static <K extends Comparable<K>> WrappedIntStream recreateWrappedIntStream(RootRemoteRecordStream<K> rootStream) {
    RemoteIntSpliteratorSupplier<K> spliteratorSupplier =
            new RemoteIntSpliteratorSupplier<>(rootStream,
                    rootStreamDescriptor -> rootStream.getDataset().intSpliterator(rootStreamDescriptor));
    IntStream nativeStream = StreamSupport.intStream(spliteratorSupplier,
            rootStream.getSourceSpliterator().characteristics(),
            rootStream.isStreamParallel());
    WrappedIntStream wrappedStream = new ResilientWrappedIntStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  private static final class RemoteIntSpliteratorSupplier<K extends Comparable<K>>
      extends AbstractRemoteSpliteratorSupplier<K, Integer, RemoteSpliterator.OfInt> {

    private RemoteIntSpliteratorSupplier(
        RootRemoteRecordStream<K> rootStream,
        Function<RootStreamDescriptor, RemoteSpliterator.OfInt> spliteratorGetter) {
      super(rootStream, spliteratorGetter);
    }

    @Override
    public RemoteSpliterator.OfInt get() {
      return new AutoClosingSpliterator<>(super.get(), getRootStream());
    }

    /**
     * A delegating {@link OfInt} that closes the associated {@link Stream} once the spliterator
     * is exhausted.
     */
    @SuppressWarnings("try")
    private static class AutoClosingSpliterator<R_SPLIT extends OfInt & RemoteSpliterator<Integer>>
        extends PrimitiveAutoClosingSpliterator<Integer, IntConsumer, OfInt, R_SPLIT>
        implements RemoteSpliterator.OfInt {

      private AutoClosingSpliterator(R_SPLIT delegate, BaseStream<?, ?> headStream) {
        super(delegate, headStream);
      }

      private AutoClosingSpliterator(AutoClosingSpliterator<R_SPLIT> owner, Spliterator.OfInt split) {
        super(owner, split);
      }

      @Override
      protected Spliterator.OfInt chainedSpliterator(Spliterator.OfInt split) {
        return new AutoClosingSpliterator<>(this, split);
      }
    }
  }
}
