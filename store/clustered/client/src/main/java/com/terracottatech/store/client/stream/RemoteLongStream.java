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
import com.terracottatech.store.common.dataset.stream.WrappedLongStream;
import com.terracottatech.store.intrinsics.impl.Constant;

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterator.OfLong;
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
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.LONG_STREAM;
import static java.util.Objects.requireNonNull;

/**
 * The TC Store proxy for a (partially) portable {@link LongStream} instance.
 *
 * @param <K> the key type of the dataset anchoring this stream
 */
class RemoteLongStream<K extends Comparable<K>>
    extends AbstractRemoteBaseStream<K, Long, LongStream>
    implements LongStream {

  RemoteLongStream(AbstractRemoteBaseStream<K, ?, ? extends BaseStream<?, ?>> parentStream) {
    super(parentStream);
  }

  @Override
  public LongStream filter(LongPredicate predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LONG_FILTER, predicate)) {
      return this;
    }
    return this.createRootLongStream().filter(predicate);
  }

  @Override
  public LongStream map(LongUnaryOperator mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LONG_MAP, mapper)) {
      return this;
    }
    return this.createRootLongStream().map(mapper);
  }

  @Override
  public <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LONG_MAP_TO_OBJ, mapper)) {
      return remoteReferenceStream();
    }
    return this.createRootLongStream().mapToObj(mapper);
  }

  protected  <U> Stream<U> remoteReferenceStream() {
    return new RemoteReferenceStream<>(this);
  }

  protected IntStream remoteIntStream() {
    return new RemoteIntStream<>(this);
  }

  protected DoubleStream remoteDoubleStream() {
    return new RemoteDoubleStream<>(this);
  }

  @Override
  public IntStream mapToInt(LongToIntFunction mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LONG_MAP_TO_INT, mapper)) {
      return remoteIntStream();
    }
    return this.createRootLongStream().mapToInt(mapper);
  }

  @Override
  public DoubleStream mapToDouble(LongToDoubleFunction mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LONG_MAP_TO_DOUBLE, mapper)) {
      return remoteDoubleStream();
    }
    return this.createRootLongStream().mapToDouble(mapper);
  }

  @Override
  public LongStream flatMap(LongFunction<? extends LongStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LONG_FLAT_MAP, mapper)) {
      return this;
    }
    return this.createRootLongStream().flatMap(mapper);
  }

  @Override
  public LongStream distinct() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DISTINCT)) {
      return this;
    }
    return this.createRootLongStream().distinct();
  }

  @Override
  public LongStream sorted() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SORTED_0)) {
      setStreamIsOrdered(true);
      return this;
    }
    return this.createRootLongStream().sorted();
  }

  @Override
  public LongStream peek(LongConsumer action) {
    // Peek is not portable
    requireNonNull(action);
    checkClosed();
    return this.createRootLongStream().peek(action);
  }

  @Override
  public LongStream limit(long maxSize) {
    if (maxSize < 0) {
      throw new IllegalArgumentException(Long.toString(maxSize));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LIMIT, new Constant<>(maxSize))) {
      return this;
    }
    return this.createRootLongStream().limit(maxSize);
  }

  @Override
  public LongStream skip(long n) {
    if (n < 0) {
      throw new IllegalArgumentException(Long.toString(n));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SKIP, new Constant<>(n))) {
      return this;
    }
    return this.createRootLongStream().skip(n);
  }

  @Override
  public void forEach(LongConsumer action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootLongStream().forEach(action);
  }

  @Override
  public void forEachOrdered(LongConsumer action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootLongStream().forEachOrdered(action);
  }

  @Override
  public long[] toArray() {
    // Not yet portable
    checkClosed();
    return this.createRootLongStream().toArray();
  }

  @Override
  public long reduce(long identity, LongBinaryOperator op) {
    // Not yet portable
    requireNonNull(op);
    checkClosed();
    return this.createRootLongStream().reduce(identity, op);
  }

  @Override
  public OptionalLong reduce(LongBinaryOperator op) {
    // Not yet portable
    requireNonNull(op);
    checkClosed();
    return this.createRootLongStream().reduce(op);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    // Not yet portable
    requireNonNull(supplier);
    requireNonNull(accumulator);
    requireNonNull(combiner);
    checkClosed();
    return this.createRootLongStream().collect(supplier, accumulator, combiner);
  }

  @Override
  public long sum() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.SUM)) {
      return (long) executeTerminated().getValue();
    }
    return this.createRootLongStream().sum();
  }

  @Override
  public OptionalLong min() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MIN_0)) {
      return (OptionalLong) executeTerminated().getValue();
    }
    return this.createRootLongStream().min();
  }

  @Override
  public OptionalLong max() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MAX_0)) {
      return (OptionalLong) executeTerminated().getValue();
    }
    return this.createRootLongStream().max();
  }

  @Override
  public long count() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.COUNT)) {
      return (long) executeTerminated().getValue();
    }
    return this.createRootLongStream().count();
  }

  @Override
  public OptionalDouble average() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.AVERAGE)) {
      return (OptionalDouble) executeTerminated().getValue();
    }
    return this.createRootLongStream().average();
  }

  @Override
  public LongSummaryStatistics summaryStatistics() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.SUMMARY_STATISTICS)) {
      return (LongSummaryStatistics) executeTerminated().getValue();
    }
    return this.createRootLongStream().summaryStatistics();
  }

  @Override
  public boolean anyMatch(LongPredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootLongStream().anyMatch(predicate);
  }

  @Override
  public boolean allMatch(LongPredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootLongStream().allMatch(predicate);
  }

  @Override
  public boolean noneMatch(LongPredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootLongStream().noneMatch(predicate);
  }

  @Override
  public OptionalLong findFirst() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_FIRST)) {
      return (OptionalLong) executeTerminated().getValue();
    }
    return this.createRootLongStream().findFirst();
  }

  @Override
  public OptionalLong findAny() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_ANY)) {
      return (OptionalLong) executeTerminated().getValue();
    }
    return this.createRootLongStream().findAny();
  }

  @Override
  public DoubleStream asDoubleStream() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.AS_DOUBLE_STREAM)) {
      return remoteDoubleStream();
    }
    return this.createRootLongStream().asDoubleStream();
  }

  @Override
  public Stream<Long> boxed() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.BOXED)) {
      return remoteReferenceStream();
    }
    return this.createRootLongStream().boxed();
  }

  @Override
  public PrimitiveIterator.OfLong iterator() {
    // Can not be portable
    checkClosed();
    return this.createRootLongStream().iterator();
  }

  @Override
  public OfLong spliterator() {
    // Can not be portable
    checkClosed();
    return this.createRootLongStream().spliterator();
  }

  @Override
  public LongStream sequential() {
    // Java LongStream.sequential() doesn't check closure
    if (tryAddPortable(IntermediateOperation.SEQUENTIAL)) {
      setStreamIsParallel(false);
      return this;
    }
    return this.createRootLongStream().sequential();
  }

  @Override
  public LongStream parallel() {
    // Java LongStream.parallel() doesn't check closure
    if (tryAddPortable(IntermediateOperation.PARALLEL)) {
      setStreamIsParallel(true);
      return this;
    }
    return this.createRootLongStream().parallel();
  }

  @Override
  public LongStream unordered() {
    // Java LongStream.unordered() checks closure only when the underlying stream is 'ordered'.
    if (isStreamOrdered()) {
      checkClosed();
    }
    if (tryAddPortable(IntermediateOperation.UNORDERED)) {
      setStreamIsOrdered(false);
      return this;
    }
    return this.createRootLongStream().unordered();
  }

  /**
   * Creates the root <i>native</i> stream to handle the client-side, non-portable operations.
   * This method may be called only once within a stream chain.
   * @return a new wrapped, native {@code LongStream} on which client-side, non-portable operations
   *        may be anchored
   */
  private LongStream createRootLongStream() {
    RootRemoteRecordStream<K> rootStream = this.getRootStream();
    if (rootStream.getRootWrappedStream() != null) {
      throw new IllegalStateException("root native stream already created");
    }

    WrappedLongStream wrappedStream = createWrappedLongStream(rootStream);
    rootStream.setRootWrappedStream(wrappedStream, LONG_STREAM);
    return wrappedStream;
  }

  public <K extends Comparable<K>> WrappedLongStream createWrappedLongStream(RootRemoteRecordStream<K> rootStream) {
    RemoteLongSpliteratorSupplier<K> spliteratorSupplier =
        new RemoteLongSpliteratorSupplier<>(rootStream,
            rootStreamDescriptor -> rootStream.getDataset().longSpliterator(rootStreamDescriptor));
    LongStream nativeStream = StreamSupport.longStream(spliteratorSupplier,
                                                        getSpliteratorCharacteristics(),
                                                        isStreamParallel());
    WrappedLongStream wrappedStream = new ResilientWrappedLongStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  public static <K extends Comparable<K>> WrappedLongStream recreateWrappedLongStream(RootRemoteRecordStream<K> rootStream) {
    RemoteLongSpliteratorSupplier<K> spliteratorSupplier =
            new RemoteLongSpliteratorSupplier<>(rootStream,
                    rootStreamDescriptor -> rootStream.getDataset().longSpliterator(rootStreamDescriptor));
    LongStream nativeStream = StreamSupport.longStream(spliteratorSupplier,
            rootStream.getSourceSpliterator().characteristics(),
            rootStream.isStreamParallel());
    WrappedLongStream wrappedStream = new ResilientWrappedLongStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  private static final class RemoteLongSpliteratorSupplier<K extends Comparable<K>>
      extends AbstractRemoteSpliteratorSupplier<K, Long, RemoteSpliterator.OfLong> {

    private RemoteLongSpliteratorSupplier(
        RootRemoteRecordStream<K> rootStream,
        Function<RootStreamDescriptor, RemoteSpliterator.OfLong> spliteratorGetter) {
      super(rootStream, spliteratorGetter);
    }

    @Override
    public RemoteSpliterator.OfLong get() {
      return new AutoClosingSpliterator<>(super.get(), getRootStream());
    }

    /**
     * A delegating {@link OfLong} that closes the associated {@link Stream} once the spliterator is exhausted.
     */
    @SuppressWarnings("try")
    private static class AutoClosingSpliterator<R_SPLIT extends OfLong & RemoteSpliterator<Long>>
        extends PrimitiveAutoClosingSpliterator<Long, LongConsumer, OfLong, R_SPLIT>
        implements RemoteSpliterator.OfLong {

      private AutoClosingSpliterator(R_SPLIT delegate, BaseStream<?, ?> headStream) {
        super(delegate, headStream);
      }

      private AutoClosingSpliterator(AutoClosingSpliterator<R_SPLIT> owner, Spliterator.OfLong split) {
        super(owner, split);
      }

      @Override
      protected Spliterator.OfLong chainedSpliterator(Spliterator.OfLong split) {
        return new AutoClosingSpliterator<>(this, split);
      }
    }
  }
}
