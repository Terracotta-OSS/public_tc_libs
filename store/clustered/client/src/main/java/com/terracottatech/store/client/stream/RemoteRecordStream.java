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

import com.terracottatech.store.Record;
import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;
import com.terracottatech.store.stream.RecordStream;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.terracottatech.store.client.stream.RootRemoteRecordStream.RemoteStreamKind.RECORD_STREAM;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import static java.util.Objects.requireNonNull;

/**
 * Provides the base of a specialized implementation of {@code Stream<Record<K>>} supporting a Java
 * stream pipeline over a remote {@link Record} stream.
 *
 * @see RootRemoteRecordStream
 */
class RemoteRecordStream<K extends Comparable<K>>
    extends RemoteReferenceStream<K, Record<K>>
    implements RecordStream<K> {

  /**
   * Creates a new {@code RemoteRecordStream}.
   * @param rootStream the {@code RootRemoteRecordStream} at the root of this stream
   */
  RemoteRecordStream(RootRemoteRecordStream<K> rootStream) {
    super(rootStream);
  }

  @Override
  public RecordStream<K> filter(Predicate<? super Record<K>> predicate) {
    return wrapIfNotThis(super.filter(predicate));
  }

  @Override
  public <R> Stream<R> map(Function<? super Record<K>, ? extends R> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP, mapper)) {
      // Result stream is no longer a RemoteRecordStream
      return remoteReferenceStream();
    }
    return this.createRootObjectStream().map(mapper);
  }

  protected <R> Stream<R> remoteReferenceStream() {
    return new RemoteReferenceStream<>(this);
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super Record<K>, ? extends Stream<? extends R>> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP, mapper)) {
      // Result stream is no longer a RemoteRecordStream
      return remoteReferenceStream();
    }
    return this.createRootObjectStream().flatMap(mapper);
  }

  @Override
  public RecordStream<K> distinct() {
    return wrapIfNotThis(super.distinct());
  }

  @Override
  public RecordStream<K> sorted() {
    throw new UnsupportedOperationException("sorted() is not supported - Record is not Comparable, what you mean probably is sorted(keyFunction().asComparator())");
  }

  @Override
  public RecordStream<K> sorted(Comparator<? super Record<K>> comparator) {
    return wrapIfNotThis(super.sorted(comparator));
  }

  @Override
  public RecordStream<K> peek(Consumer<? super Record<K>> action) {
    return wrapIfNotThis(super.peek(action));
  }

  @Override
  public RecordStream<K> limit(long maxSize) {
    return wrapIfNotThis(super.limit(maxSize));
  }

  @Override
  public RecordStream<K> skip(long n) {
    return wrapIfNotThis(super.skip(n));
  }

  @Override
  public RecordStream<K> sequential() {
    return wrapIfNotThis(super.sequential());
  }

  @Override
  public RecordStream<K> parallel() {
    return wrapIfNotThis(super.parallel());
  }

  @Override
  public RecordStream<K> unordered() {
    return wrapIfNotThis(super.unordered());
  }

  @Override
  public RecordStream<K> onClose(Runnable closeHandler) {
     return (RecordStream<K>)super.onClose(closeHandler);
  }

  @Override
  public RecordStream<K> explain(Consumer<Object> consumer) {
    return new RemoteRecordStream<>(getRootStream().explain(consumer));
  }

  @Override
  public RecordStream<K> batch(int sizeHint) {
    return new RemoteRecordStream<>(getRootStream().batch(sizeHint));
  }

  @Override
  public RecordStream<K> inline() {
    return new RemoteRecordStream<>(getRootStream().inline());
  }

  /**
   * Wraps the {@code Stream} supplied in a new {@link LocalRecordStream} instance if it is not {@code this}
   * stream.  This method is used to continue a {@link RecordStream} pipeline when an operation uses a
   * non-portable operation but its element type is known to be a {@code Record<K>}.
   * @param stream the stream to wrap or not
   * @return {@code this} or {@code LocalRecordStream(stream)}
   */
  private RecordStream<K> wrapIfNotThis(Stream<Record<K>> stream) {
    if (stream == this) {
      return this;
    } else {
      return new LocalRecordStream<>(getRootStream(), stream);
    }
  }

  /**
   * Creates the root <i>native</i> stream to handle the client-side, non-portable operations.
   * This method may be called only once within a stream chain.
   * @return a new wrapped, native {@code Stream} on which client-side, non-portable operations
   *        may be anchored
   */
  @Override
  protected Stream<Record<K>> createRootObjectStream() {
    RootRemoteRecordStream<K> rootStream = getRootStream();
    if (rootStream.getRootWrappedStream() != null) {
      throw new IllegalStateException("root native stream already created");
    }

    // TODO: Determine a way to introduce Record locking at the appropriate point in the server-side pipeline
    WrappedReferenceStream<Record<K>> wrappedStream = createWrappedRecordStream(rootStream);
    rootStream.setRootWrappedStream(wrappedStream, RECORD_STREAM);
    return wrappedStream;
  }

  public WrappedReferenceStream<Record<K>> createWrappedRecordStream(RootRemoteRecordStream<K> rootStream) {
    RemoteSpliteratorSupplier<K, Record<K>> spliteratorSupplier =
        new RemoteSpliteratorSupplier<>(rootStream,
            rootStreamDescriptor -> rootStream.getDataset().spliterator(rootStreamDescriptor));
    Stream<Record<K>> nativeStream =
        StreamSupport.stream(spliteratorSupplier,
                              getSpliteratorCharacteristics(),
                              isStreamParallel());
    WrappedReferenceStream<Record<K>> wrappedStream = new ResilientWrappedReferenceStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  public static <K extends Comparable<K>> WrappedReferenceStream<Record<K>> recreateWrappedRecordStream(RootRemoteRecordStream<K> rootStream) {
    RemoteSpliteratorSupplier<K, Record<K>> spliteratorSupplier =
            new RemoteSpliteratorSupplier<>(rootStream,
                    rootStreamDescriptor -> rootStream.getDataset().spliterator(rootStreamDescriptor));
    Stream<Record<K>> nativeStream =
            StreamSupport.stream(spliteratorSupplier,
                    rootStream.getSourceSpliterator().characteristics(),
                    rootStream.isStreamParallel());
    WrappedReferenceStream<Record<K>> wrappedStream = new ResilientWrappedReferenceStream<>(rootStream, nativeStream);
    wrappedStream.appendTerminalAction(spliteratorSupplier);
    return wrappedStream;
  }

  @Override
  protected int getSpliteratorCharacteristics() {
    return super.getSpliteratorCharacteristics() | Spliterator.DISTINCT | Spliterator.NONNULL;
  }
}
