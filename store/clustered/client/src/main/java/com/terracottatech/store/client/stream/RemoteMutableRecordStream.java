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

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreStreamNotFoundException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.client.RecordImpl;
import com.terracottatech.store.client.VoltronDatasetEntity;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.NonPortableTransform;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchApplyResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchConsumedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceMutateMessage;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;
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

import static java.util.Objects.requireNonNull;

/**
 * Provides a specialized implementation of {@code Stream<Record<K>>} supporting a Java stream pipeline over
 * a remote {@link Record} stream.  An instance of this class represents the <i>head</i> of a remote pipeline
 * sequence.  Portable operations chained from this instance are represented solely as metadata held in this
 * instance; non-portable operations result in new {@code Stream} instances chained from this <i>head</i>.
 *
 * <h3>Implementation Notes</h3>
 * The methods in this class assemble a sequence of <i>portable</i> operations "chained" from
 * an instance.  On reaching the first non-portable operation, the portable operation sequence is
 * presented to the {@code Supplier} allocated to provide the {@code Spliterator} for the pipeline
 * then control of pipeline assembly exits this class.  As a consequence, no more than one call to
 * {@link Consumer#accept(Object) remoteSpliteratorSupplier.accept} is made.
 * <p>
 * The designation of portable operations must be consistent with those handled by
 * {@code com.terracottatech.store.server.stream.RemoteStreamTool}.
 *
 * @param <K> the {@code Record} key type
 *
 * @see RootRemoteRecordStream
 */
public class RemoteMutableRecordStream<K extends Comparable<K>>
    extends RootRemoteRecordStream<K>
    implements MutableRecordStream<K> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteMutableRecordStream.class);

  /**
   * The tail of the (wrapped) native {@code Record<K>} stream chain providing the pipeline for client-side
   * (non-portable) pipeline sequence against which a mutative operation might be appended.
   */
  private Stream<Record<K>> tailWrappedStream;

  /**
   * For pipelines using {@link WaypointMarker} instances, this is the next assignable waypoint id.
   */
  private int nextWaypointId = 0;

  /**
   * Constructs a new {@code RootRemoteRecordStream} as the head of a new stream/pipeline chain.
   *
   * @param entity the {@code DatasetEntity} through which access to the backing dataset is gained
   */
  public RemoteMutableRecordStream(VoltronDatasetEntity<K> entity) {
    super(entity);
  }

  /**
   * Updates {@link #tailWrappedStream} with the value obtained from the
   * {@link Supplier Supplier&lt;Stream&lt;Record&lt;K>>>} provided.
   *
   * @param createStream if {@code true} and {@link #tailWrappedStream} is {@code null},
   *                     {@link #createRootObjectStream()} is called to initialize {@link #tailWrappedStream};
   *                     if {@code false} and {@code tailWrappedStream} is {@code null}, an
   *                     {@link IllegalStateException} is thrown; otherwise, {@code tailWrappedStream}
   *                     is updated with the value returned by {@code operationAppender}.  Use {@code false}
   *                     for operations which are always portable.
   * @param operationAppender the {@code Supplier} providing the updated {@code tailWrappedStream} value
   *
   * @throws IllegalStateException if {@code tailWrappedStream} is {@code null}
   */
  private void trackOperation(boolean createStream, Function<Stream<Record<K>>, Stream<Record<K>>> operationAppender) {
    if (tailWrappedStream == null) {
      if (!createStream) {
        throw new IllegalStateException("Unexpected condition: tailWrappedStream == null");
      }
      tailWrappedStream = createRootObjectStream();
    }
    tailWrappedStream = operationAppender.apply(tailWrappedStream);
  }

  /**
   * Ends the scan for a mutative operation by appending the final {@code RootRemoteRecordStream} operation
   * to the pipeline.  Appending a terminal operation results in pipeline evaluation.
   *
   * @param operationAppender the {@code Function} which appends the "final" {@code RootRemoteRecordStream} operation
   * @param <T> the type of the append result; may be a {@link BaseStream} subclass or a terminal operation value
   *
   * @return the result of appending the operation
   */
  private <T> T finishPipeline(Function<Stream<Record<K>>, T> operationAppender) {
    if (tailWrappedStream == null) {
      tailWrappedStream = createRootObjectStream();
    }
    return operationAppender.apply(tailWrappedStream);
  }

  /**
   * Ends the scan for a mutative operation by appending a terminal operation taking a {@link Consumer} to the
   * pipeline.  This results in pipeline evaluation.
   *
   * @param operationAppender the {@code Function} which appends the "final" {@code RootRemoteRecordStream} operation
   */
  private void consumePipeline(Consumer<Stream<Record<K>>> operationAppender) {
    if (tailWrappedStream == null) {
      tailWrappedStream = createRootObjectStream();
    }
    operationAppender.accept(tailWrappedStream);
  }

  /**
   * Releases a server-resident mutative stream holding at a waypoint and waits for and returns the result.
   * This method is used to support a mutative operation using a non-portable transform.
   *
   * @param cells the collection {@link Cell} instances comprising the updated {@link Record} content;
   *              this value may be {@code null} when a mutative, portable pipeline operation is preceded by
   *              one or more non-portable operations
   * @return the updated {@link Record} value; {@code null} is returned if the method is called through
   *          a {@link Consumer}
   */
  private Record<K> sendTryAdvanceMutate(int waypointId, Iterable<Cell<?>> cells) {
    @SuppressWarnings("unchecked")
    RemoteSpliterator<K> sourceSpliterator = (RemoteSpliterator<K>)getSourceSpliterator();
    TryAdvanceMutateMessage mutateMessage = new TryAdvanceMutateMessage(getStreamId(), waypointId, cells);
    DatasetEntityResponse response;
    try {
      response = sourceSpliterator.sendReceive("supplying mutation (" + waypointId + ")", mutateMessage);
    } catch (StoreStreamNotFoundException e) {
      throw new IllegalStateException(STREAM_USED_OR_CLOSED, e);
    }
    if (response instanceof TryAdvanceFetchConsumedResponse) {
      sourceSpliterator.suppressRelease();
      return null;
    } else if (response instanceof TryAdvanceFetchApplyResponse) {
      @SuppressWarnings("unchecked") TryAdvanceFetchApplyResponse<K> applyResponse = (TryAdvanceFetchApplyResponse<K>)response;
      return new RecordImpl<>(applyResponse.getElement().<K>getRecordData());
    } else {
      throw new IllegalStateException("Error supplying record update: unexpected response " + response);
    }
  }

  @Override
  public void mutate(UpdateOperation<? super K> transform) {
    requireNonNull(transform);
    checkClosed();
    /*
     * The _portable_ terminal MUTATE operation gets restated as an intermediate MUTATE_THEN followed
     * by a terminal COUNT.
     */
    if (tryAddPortable(IntermediateOperation.MUTATE_THEN, transform) && tryAddPortable(TerminalOperation.COUNT)) {
      executeTerminated().getValue();
      return;
    }

    /*
     * At this point, we either have a non-portable pipeline or a mutate with a non-portable transform.
     * In either case, a waypoint is used to coordinate the client-side operation with the server-side.
     */
    int waypointId = nextWaypointId++;
    addPortable(IntermediateOperation.FILTER, new WaypointMarker<K>(waypointId));

    if (isPortable(TerminalOperation.MUTATE, transform)) {
      // Transform is portable -- update can be calculated remotely using Record in pipeline
      addPortable(TerminalOperation.MUTATE, transform);

      consumePipeline(s -> s.forEach(oldRecord -> {
        Record<K> record = sendTryAdvanceMutate(waypointId, null);
        if (record != null) {
          throw new IllegalStateException("Unexpected state -- mutate returned a Record");
        }
      }));

    } else {
      // Transform is non-portable -- update must be calculated client-side and shipped to the server waypoint
      addPortable(TerminalOperation.MUTATE, new NonPortableTransform<>(waypointId));

      // Complete the client-side pipeline ...
      consumePipeline(s -> s.forEach(oldRecord -> {
        @SuppressWarnings("unchecked") Record<K> record =
            sendTryAdvanceMutate(waypointId, ((UpdateOperation<K>)transform).apply(oldRecord));
        if (record != null) {
          throw new IllegalStateException("Unexpected state -- mutate returned a Record");
        }
      }));
    }
  }

  @Override
  public Stream<Tuple<Record<K>, Record<K>>> mutateThen(UpdateOperation<? super K> transform) {
    requireNonNull(transform);
    checkClosed();
    setNonRetryable();
    if (tryAddPortable(IntermediateOperation.MUTATE_THEN, transform)) {
      return remoteReferenceStream();
    }

    /*
     * At this point, we either have a non-portable pipeline or a mutateThen with a non-portable transform.
     * In either case, a waypoint is used to coordinate the client-side operation with the server-side.
     */
    int waypointId = nextWaypointId++;
    addPortable(IntermediateOperation.FILTER, new WaypointMarker<K>(waypointId));

    if (isPortable(IntermediateOperation.MUTATE_THEN, transform)) {
      // Transform is portable -- update can be calculated remotely using Record in pipeline
      addPortable(IntermediateOperation.MUTATE_THEN_INTERNAL, transform);

      return finishPipeline(s -> s.map(oldRecord -> {
        Record<K> newRecord = sendTryAdvanceMutate(waypointId, null);
        return Tuple.of(oldRecord, newRecord);
      }));

    } else {
      // Transform is non-portable -- update must be calculated client-side and shipped to the server waypoint
      addPortable(IntermediateOperation.MUTATE_THEN_INTERNAL, new NonPortableTransform<>(waypointId));

      return finishPipeline(s -> s.map(oldRecord -> {
        @SuppressWarnings("unchecked") Record<K> newRecord =
            sendTryAdvanceMutate(waypointId, ((UpdateOperation<K>)transform).apply(oldRecord));
        return Tuple.of(oldRecord, newRecord);
      }));
    }
  }

  @Override
  public void delete() {
    checkClosed();
     /*
     * The _portable_ terminal DELETE operation gets restated as an intermediate DELETE_THEN followed
     * by a terminal COUNT.
     */
    if (tryAddPortable(IntermediateOperation.DELETE_THEN) && tryAddPortable(TerminalOperation.COUNT)) {
      executeTerminated().getValue();
      return;
    }

    /*
     * At this point, we have a non-portable pipeline.  A waypoint is used to coordinate the
     * client-side operation with the server-side.
     */
    int waypointId = nextWaypointId++;
    addPortable(IntermediateOperation.FILTER, new WaypointMarker<K>(waypointId));
    addPortable(TerminalOperation.DELETE);

    consumePipeline((s -> s.forEach(r -> {
      Record<K> record = sendTryAdvanceMutate(waypointId, null);
      if (record != null) {
        throw new IllegalStateException("Unexpected state -- delete returned a Record");
      }
    })));
  }

  @Override
  public Stream<Record<K>> deleteThen() {
    checkClosed();
    setNonRetryable();
    if (tryAddPortable(IntermediateOperation.DELETE_THEN)) {
      return remoteReferenceStream();
    }

    /*
     * At this point, we have a non-portable pipeline.  A waypoint is used to coordinate the
     * client-side operation with the server-side.
     */
    int waypointId = nextWaypointId++;
    addPortable(IntermediateOperation.FILTER, new WaypointMarker<K>(waypointId));
    addPortable(IntermediateOperation.DELETE_THEN);

    // TODO: Optimize this so the server-side operation does not return a Record
    return finishPipeline(s -> s.map(r -> sendTryAdvanceMutate(waypointId, null)));
  }

  @Override
  public RemoteMutableRecordStream<K> explain(Consumer<Object> consumer) {
    return (RemoteMutableRecordStream<K>) super.explain(consumer);
  }

  @Override
  public RemoteMutableRecordStream<K> batch(int sizeHint) {
    return (RemoteMutableRecordStream<K>) super.batch(sizeHint);
  }

  @Override
  public RemoteMutableRecordStream<K> inline() {
    return (RemoteMutableRecordStream<K>) super.inline();
  }

  @Override
  public MutableRecordStream<K> filter(Predicate<? super Record<K>> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FILTER, predicate)) {
      return this;
    }
    trackOperation(true, s -> s.filter(predicate));
    return this;
  }

  @Override
  public <R> Stream<R> map(Function<? super Record<K>, ? extends R> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP, mapper)) {
      // Result stream is no longer a RemoteRecordStream
      return remoteReferenceStream();
    }
    return finishPipeline(s -> s.map(mapper));
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super Record<K>> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP_TO_INT, mapper)) {
      return remoteIntStream();
    }
    return finishPipeline(s -> s.mapToInt(mapper));
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super Record<K>> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP_TO_LONG, mapper)) {
      return remoteLongStream();
    }
    return finishPipeline(s -> s.mapToLong(mapper));
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super Record<K>> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.MAP_TO_DOUBLE, mapper)) {
      return remoteDoubleStream();
    }
    return finishPipeline(s -> s.mapToDouble(mapper));
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super Record<K>, ? extends Stream<? extends R>> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP, mapper)) {
      // Result stream is no longer a RemoteRecordStream
      return remoteReferenceStream();
    }
    return finishPipeline(s -> s.flatMap(mapper));
  }

  @Override
  public IntStream flatMapToInt(Function<? super Record<K>, ? extends IntStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP_TO_INT, mapper)) {
      return remoteIntStream();
    }
    return finishPipeline(s -> s.flatMapToInt(mapper));
  }

  @Override
  public LongStream flatMapToLong(Function<? super Record<K>, ? extends LongStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP_TO_LONG, mapper)) {
      return remoteLongStream();
    }
    return finishPipeline(s -> s.flatMapToLong(mapper));
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super Record<K>, ? extends DoubleStream> mapper) {
    requireNonNull(mapper);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.FLAT_MAP_TO_DOUBLE, mapper)) {
      return remoteDoubleStream();
    }
    return finishPipeline(s -> s.flatMapToDouble(mapper));
  }

  @Override
  public MutableRecordStream<K> distinct() {
    // This operation may be dropped in Sovereign
    checkClosed();
    if (tryAddPortable(IntermediateOperation.DISTINCT)) {
      return this;
    }
    // Can reach here only if a previous operation was non-portable (or someone made distinct non-portable)
    trackOperation(true, Stream::distinct);
    return this;
  }

  @Override
  public RecordStream<K> sorted() {
    throw new UnsupportedOperationException("sorted() is not supported - Record is not Comparable, what you mean probably is sorted(keyFunction().asComparator())");
  }

  @Override
  public RecordStream<K> sorted(Comparator<? super Record<K>> comparator) {
    requireNonNull(comparator);
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SORTED_1, comparator)) {
      setStreamIsOrdered(true);
      // Still portable but can't permit mutative operation
      return remoteRecordStream();
    }
    // No longer portable but still a RecordStream -- "downgrade" by wrapping in a LocalRecordStream
    return finishPipeline(s -> new LocalRecordStream<>(this, s.sorted(comparator)));
  }

  @Override
  public MutableRecordStream<K> peek(Consumer<? super Record<K>> action) {
    requireNonNull(action);
    checkClosed();

    if (tryAddPortable(IntermediateOperation.PEEK, action)) {
      return this;
    }

    LOGGER.warn("A non-portable peek action has made the stream non portable.");
    trackOperation(true, s -> s.peek(action));
    return this;
  }

  @Override
  public MutableRecordStream<K> limit(long maxSize) {
    if (maxSize < 0) {
      throw new IllegalArgumentException(Long.toString(maxSize));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.LIMIT, new Constant<>(maxSize))) {
      return this;
    }
    trackOperation(true, s -> s.limit(maxSize));
    return this;
  }

  @Override
  public MutableRecordStream<K> skip(long n) {
    if (n < 0) {
      throw new IllegalArgumentException(Long.toString(n));
    }
    checkClosed();
    if (tryAddPortable(IntermediateOperation.SKIP, new Constant<>(n))) {
      return this;
    }
    trackOperation(true, s -> s.skip(n));
    return this;
  }

  @Override
  public void forEach(Consumer<? super Record<K>> action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    if (tryAddPortable(TerminalOperation.FOR_EACH)) {
      executeTerminated().getValue();
    } else {
      consumePipeline(s -> s.forEach(action));
    }
  }

  @Override
  public void forEachOrdered(Consumer<? super Record<K>> action) {
    // Not yet portable
    requireNonNull(action);
    checkClosed();
    if (tryAddPortable(TerminalOperation.FOR_EACH_ORDERED)) {
      executeTerminated().getValue();
    } else {
      consumePipeline(s -> s.forEachOrdered(action));
    }
  }

  @Override
  public Object[] toArray() {
    // Not yet portable
    checkClosed();
    return finishPipeline(Stream::toArray);
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    // Not yet portable
    requireNonNull(generator);
    checkClosed();
    return finishPipeline(s -> s.toArray(generator));
  }

  @Override
  public Record<K> reduce(Record<K> identity, BinaryOperator<Record<K>> accumulator) {
    // Not yet portable
    requireNonNull(accumulator);
    checkClosed();
    return finishPipeline(s -> s.reduce(identity, accumulator));
  }

  @Override
  public Optional<Record<K>> reduce(BinaryOperator<Record<K>> accumulator) {
    // Not yet portable
    requireNonNull(accumulator);
    checkClosed();
    return finishPipeline(s -> s.reduce(accumulator));
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super Record<K>, U> accumulator, BinaryOperator<U> combiner) {
    // Not yet portable
    requireNonNull(accumulator);
    requireNonNull(combiner);
    checkClosed();
    return finishPipeline(s -> s.reduce(identity, accumulator, combiner));
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super Record<K>> accumulator, BiConsumer<R, R> combiner) {
    // Not yet portable
    requireNonNull(supplier);
    requireNonNull(accumulator);
    requireNonNull(combiner);
    checkClosed();
    return finishPipeline(s -> s.collect(supplier, accumulator, combiner));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, A> R collect(Collector<? super Record<K>, A, R> collector) {
    requireNonNull(collector);
    checkClosed();
    if (tryAddPortable(TerminalOperation.COLLECT_1, collector)) {
      return (R) executeTerminated().getValue(RecordImpl::toRecord);              // unchecked
    }
    return finishPipeline(s -> s.collect(collector));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<Record<K>> min(Comparator<? super Record<K>> comparator) {
    requireNonNull(comparator);
    checkClosed();
    if (tryAddPortable(TerminalOperation.MIN_1, comparator)) {
      return (Optional<Record<K>>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return finishPipeline(s -> s.min(comparator));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<Record<K>> max(Comparator<? super Record<K>> comparator) {
    requireNonNull(comparator);
    checkClosed();
    if (tryAddPortable(TerminalOperation.MAX_1, comparator)) {
      return (Optional<Record<K>>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return finishPipeline(s -> s.max(comparator));
  }

  @Override
  public long count() {
    checkClosed();
    if (tryAddPortable(TerminalOperation.COUNT)) {
      return (long) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return finishPipeline(Stream::count);
  }

  @Override
  public boolean anyMatch(Predicate<? super Record<K>> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(TerminalOperation.ANY_MATCH, predicate)) {
      return (boolean) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return finishPipeline(s -> s.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(Predicate<? super Record<K>> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(TerminalOperation.ALL_MATCH, predicate)) {
      return (boolean) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return finishPipeline(s -> s.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(Predicate<? super Record<K>> predicate) {
    requireNonNull(predicate);
    checkClosed();
    if (tryAddPortable(TerminalOperation.NONE_MATCH, predicate)) {
      return (boolean) executeTerminated().getValue(RecordImpl::toRecord);
    }
    return finishPipeline(s -> s.noneMatch(predicate));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<Record<K>> findFirst() {
    checkClosed();
    if (tryAddPortable(TerminalOperation.FIND_FIRST)) {
      return (Optional<Record<K>>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return finishPipeline(Stream::findFirst);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<Record<K>> findAny() {
    checkClosed();
    if (tryAddPortable(TerminalOperation.FIND_ANY)) {
      return (Optional<Record<K>>) executeTerminated().getValue(RecordImpl::toRecord);    // unchecked
    }
    return finishPipeline(Stream::findAny);
  }

  @Override
  public MutableRecordStream<K> sequential() {
    // Java Stream.sequential() doesn't check closure
    if (tryAddPortable(IntermediateOperation.SEQUENTIAL)) {
      setStreamIsParallel(false);
      return this;
    }
    // Can reach here only if a previous operation was non-portable (or someone made sequential non-portable)
    trackOperation(false, BaseStream::sequential);
    return this;
  }

  @Override
  public MutableRecordStream<K> parallel() {
    // Java Stream.parallel() doesn't check closure
    if (tryAddPortable(IntermediateOperation.PARALLEL)) {
      setStreamIsParallel(true);
      return this;
    }
    // Can reach here only if a previous operation was non-portable (or someone made parallel non-portable)
    trackOperation(false, BaseStream::parallel);
    return this;
  }

  @Override
  public MutableRecordStream<K> unordered() {
    // Java Stream.unordered() checks closure only when the underlying stream is 'ordered'.
    if (isStreamOrdered()) {
      checkClosed();
    }
    if (tryAddPortable(IntermediateOperation.UNORDERED)) {
      setStreamIsOrdered(false);
      return this;
    }
    // Can reach here only if a previous operation was non-portable (or someone made unordered non-portable)
    trackOperation(false, BaseStream::unordered);
    return this;
  }

  @Override
  public Iterator<Record<K>> iterator() {
    // Can not be portable
    checkClosed();
    return finishPipeline(BaseStream::iterator);
  }

  @Override
  public Spliterator<Record<K>> spliterator() {
    // Can not be portable
    checkClosed();
    return finishPipeline(BaseStream::spliterator);
  }

  @Override
  public MutableRecordStream<K> onClose(Runnable closeHandler) {
    return (MutableRecordStream<K>)super.onClose(closeHandler);
  }

  protected RecordStream<K> remoteRecordStream() {
    return new RemoteRecordStream<>(this);
  }

}
