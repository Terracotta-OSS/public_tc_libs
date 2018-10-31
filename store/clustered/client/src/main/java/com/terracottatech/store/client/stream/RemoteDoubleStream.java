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
import com.terracottatech.store.common.dataset.stream.WrappedDoubleStream;
import com.terracottatech.store.intrinsics.impl.Constant;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterator.OfDouble;
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
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.DOUBLE_STREAM;
import static java.util.Objects.requireNonNull;

/**
 * The TC Store proxy for a (partially) portable {@link DoubleStream} instance.
 *
 * @param <K> the key type of the dataset anchoring this stream
 */
class RemoteDoubleStream<K extends Comparable<K>>
    extends AbstractRemoteBaseStream<K, Double, DoubleStream>
    implements DoubleStream {

  RemoteDoubleStream(AbstractRemoteBaseStream<K, ?, ? extends BaseStream<?, ?>> parentStream) {
    super(parentStream);
  }

  @Override
  public DoubleStream filter(DoublePredicate predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DOUBLE_FILTER, predicate)) {
      return this;
    }
    return this.createRootDoubleStream().filter(predicate);
  }

  @Override
  public DoubleStream map(DoubleUnaryOperator mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DOUBLE_MAP, mapper)) {
      return this;
    }
    return this.createRootDoubleStream().map(mapper);
  }

  @Override
  public <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DOUBLE_MAP_TO_OBJ, mapper)) {
      return remoteReferenceStream();
    }
    return this.createRootDoubleStream().mapToObj(mapper);
  }

  @Override
  public IntStream mapToInt(DoubleToIntFunction mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DOUBLE_MAP_TO_INT, mapper)) {
      return remoteIntStream();
    }
    return this.createRootDoubleStream().mapToInt(mapper);
  }

  protected  <U> Stream<U> remoteReferenceStream() {
    return new RemoteReferenceStream<>(this);
  }

  protected IntStream remoteIntStream() {
    return new RemoteIntStream<>(this);
  }

  protected LongStream remoteLongStream() {
    return new RemoteLongStream<>(this);
  }

  @Override
  public LongStream mapToLong(DoubleToLongFunction mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DOUBLE_MAP_TO_LONG, mapper)) {
      return remoteLongStream();
    }
    return this.createRootDoubleStream().mapToLong(mapper);
  }

  @Override
  public DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DOUBLE_FLAT_MAP, mapper)) {
      return this;
    }
    return this.createRootDoubleStream().flatMap(mapper);
  }

  @Override
  public DoubleStream distinct() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DISTINCT)) {
      return this;
    }
    return this.createRootDoubleStream().distinct();
  }

  @Override
  public DoubleStream sorted() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SORTED_0)) {
      setStreamIsOrdered(true);
      return this;
    }
    return this.createRootDoubleStream().sorted();
  }

  @Override
  public DoubleStream peek(DoubleConsumer action) {
    // Peek isn't portable
    requireNonNull(action);
    checkClosed();
    return this.createRootDoubleStream().peek(action);
  }

  @Override
  public DoubleStream limit(long maxSize) {
    if (maxSize < 0) {
      throw new IllegalArgumentException(Long.toString(maxSize));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LIMIT, new Constant<>(maxSize))) {
      return this;
    }
    return this.createRootDoubleStream().limit(maxSize);
  }

  @Override
  public DoubleStream skip(long n) {
    if (n < 0) {
      throw new IllegalArgumentException(Long.toString(n));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SKIP, new Constant<>(n))) {
      return this;
    }
    return this.createRootDoubleStream().skip(n);
  }

  @Override
  public void forEach(DoubleConsumer action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootDoubleStream().forEach(action);
  }

  @Override
  public void forEachOrdered(DoubleConsumer action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    this.createRootDoubleStream().forEachOrdered(action);
  }

  @Override
  public double[] toArray() {
    // Not yet portable
    checkClosed();
    return this.createRootDoubleStream().toArray();
  }

  @Override
  public double reduce(double identity, DoubleBinaryOperator op) {
    // Not yet portable
    requireNonNull(op);
    checkClosed();
    return this.createRootDoubleStream().reduce(identity, op);
  }

  @Override
  public OptionalDouble reduce(DoubleBinaryOperator op) {
    // Not yet portable
    requireNonNull(op);
    checkClosed();
    return this.createRootDoubleStream().reduce(op);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    // Not yet portable
    requireNonNull(supplier);
    requireNonNull(accumulator);
    requireNonNull(combiner);
    checkClosed();
    return this.createRootDoubleStream().collect(supplier, accumulator, combiner);
  }

  @Override
  public double sum() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.SUM)) {
      return (double) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().sum();
  }

  @Override
  public OptionalDouble min() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MIN_0)) {
      return (OptionalDouble) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().min();
  }

  @Override
  public OptionalDouble max() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.MAX_0)) {
      return (OptionalDouble) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().max();
  }

  @Override
  public long count() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.COUNT)) {
      return (long) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().count();
  }

  @Override
  public OptionalDouble average() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.AVERAGE)) {
      return (OptionalDouble) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().average();
  }

  @Override
  public DoubleSummaryStatistics summaryStatistics() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.SUMMARY_STATISTICS)) {
      return (DoubleSummaryStatistics) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().summaryStatistics();
  }

  @Override
  public boolean anyMatch(DoublePredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootDoubleStream().anyMatch(predicate);
  }

  @Override
  public boolean allMatch(DoublePredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootDoubleStream().allMatch(predicate);
  }

  @Override
  public boolean noneMatch(DoublePredicate predicate) {
    // Not yet portable
    requireNonNull(predicate);
    checkClosed();
    return this.createRootDoubleStream().noneMatch(predicate);
  }

  @Override
  public OptionalDouble findFirst() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_FIRST)) {
      return (OptionalDouble) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().findFirst();
  }

  @Override
  public OptionalDouble findAny() {
    checkClosed();
    if (tryAddPortable(PipelineOperation.TerminalOperation.FIND_ANY)) {
      return (OptionalDouble) executeTerminated().getValue();
    }
    return this.createRootDoubleStream().findAny();
  }

  @Override
  public Stream<Double> boxed() {
    checkClosed();
    if (tryAddPortable(IntermediateOperation.BOXED)) {
      return remoteReferenceStream();
    }
    return this.createRootDoubleStream().boxed();
  }

  @Override
  public PrimitiveIterator.OfDouble iterator() {
    // Can not be portable
    checkClosed();
    return this.createRootDoubleStream().iterator();
  }

  @Override
  public OfDouble spliterator() {
    // Can not be portable
    checkClosed();
    return this.createRootDoubleStream().spliterator();
  }

  @Override
  public DoubleStream sequential() {
    // Java DoubleStream.sequential() doesn't check closure
    if (tryAddPortable(IntermediateOperation.SEQUENTIAL)) {
      setStreamIsParallel(false);
      return this;
    }
    return this.createRootDoubleStream().sequential();
  }

  @Override
  public DoubleStream parallel() {
    // Java DoubleStream.parallel() doesn't check closure
    if (tryAddPortable(IntermediateOperation.PARALLEL)) {
      setStreamIsParallel(true);
      return this;
    }
    return this.createRootDoubleStream().parallel();
  }

  @Override
  public DoubleStream unordered() {
    // Java DoubleStream.unordered() checks closure only when the underlying stream is 'ordered'.
    if (isStreamOrdered()) {
      checkClosed();
    }
    if (tryAddPortable(IntermediateOperation.UNORDERED)) {
      setStreamIsOrdered(false);
      return this;
    }
    return this.createRootDoubleStream().unordered();
  }

  /**
   * Creates the root <i>native</i> stream to handle the client-side, non-portable operations.
   * This method may be called only once within a stream chain.
   * @return a new wrapped, native {@code DoubleStream} on which client-side, non-portable operations
   *        may be anchored
   */
  private DoubleStream createRootDoubleStream() {
    RootRemoteRecordStream<K> rootStream = this.getRootStream();
    if (rootStream.getRootWrappedStream() != null) {
      throw new IllegalStateException("root native stream already created");
    }

    WrappedDoubleStream wrappedStream = createWrappedDoubleStream(rootStream);
    rootStream.setRootWrappedStream(wrappedStream, DOUBLE_STREAM);
    return wrappedStream;
  }

  public WrappedDoubleStream createWrappedDoubleStream(RootRemoteRecordStream<K> rootStream) {
    RemoteDoubleSpliteratorSupplier<K> spliteratorSupplier =
        new RemoteDoubleSpliteratorSupplier<>(rootStream,
            rootStreamDescriptor -> rootStream.getDataset().doubleSpliterator(rootStreamDescriptor));
    DoubleStream nativeStream = StreamSupport.doubleStream(spliteratorSupplier,
                                                           getSpliteratorCharacteristics(),
                                                           isStreamParallel());
    WrappedDoubleStream wrappedStream = new ResilientWrappedDoubleStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  public static <K extends Comparable<K>> WrappedDoubleStream recreateWrappedDoubleStream(RootRemoteRecordStream<K> rootStream) {
    RemoteDoubleSpliteratorSupplier<K> spliteratorSupplier =
            new RemoteDoubleSpliteratorSupplier<>(rootStream,
                    rootStreamDescriptor -> rootStream.getDataset().doubleSpliterator(rootStreamDescriptor));
    DoubleStream nativeStream = StreamSupport.doubleStream(spliteratorSupplier,
            rootStream.getSourceSpliterator().characteristics(),
            rootStream.isStreamParallel());
    WrappedDoubleStream wrappedStream = new ResilientWrappedDoubleStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  private static final class RemoteDoubleSpliteratorSupplier<K extends Comparable<K>>
      extends AbstractRemoteSpliteratorSupplier<K, Double, RemoteSpliterator.OfDouble> {

    private RemoteDoubleSpliteratorSupplier(
        RootRemoteRecordStream<K> rootStream,
        Function<RootStreamDescriptor, RemoteSpliterator.OfDouble> spliteratorGetter) {
      super(rootStream, spliteratorGetter);
    }

    @Override
    public RemoteSpliterator.OfDouble get() {
      return new AutoClosingSpliterator<>(super.get(), getRootStream());
    }

    /**
     * A delegating {@link OfDouble} that closes the associated {@link Stream} once the spliterator is exhausted.
     */
    @SuppressWarnings("try")
    private static class AutoClosingSpliterator<R_SPLIT extends OfDouble & RemoteSpliterator<Double>>
        extends PrimitiveAutoClosingSpliterator<Double, DoubleConsumer, OfDouble, R_SPLIT>
        implements RemoteSpliterator.OfDouble {

      private AutoClosingSpliterator(R_SPLIT delegate, BaseStream<?, ?> headStream) {
        super(delegate, headStream);
      }

      private AutoClosingSpliterator(AutoClosingSpliterator<R_SPLIT> owner, Spliterator.OfDouble split) {
        super(owner, split);
      }

      @Override
      protected Spliterator.OfDouble chainedSpliterator(Spliterator.OfDouble split) {
        return new AutoClosingSpliterator<>(this, split);
      }
    }
  }
}
