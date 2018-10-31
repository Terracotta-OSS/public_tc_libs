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
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.WrappedStream;
import com.terracottatech.store.stream.RecordStream;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Spliterator.CONCURRENT;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;

public abstract class AbstractShardedRecordStream<K extends Comparable<K>, RS extends RecordStream<K>> extends ShardedReferenceStream<Record<K>> implements RecordStream<K> {

  private final Collection<? extends RecordStream<K>> initialStreams;
  private BaseStream<?, ?> mergedStream;
  private boolean requiresExplanation = false;

  AbstractShardedRecordStream(Collection<RS> initial) {
    super(initial, null, CONCURRENT | DISTINCT | NONNULL);
    this.initialStreams = initial;
  }

  AbstractShardedRecordStream(Stream<Stream<Record<K>>> shards, Comparator<? super Record<K>> ordering, int characteristics, AbstractShardedRecordStream<K, ?> source) {
    super(shards, ordering, characteristics, source);
    this.initialStreams = source.getInitialStreams();
  }

  @SuppressWarnings("unchecked")
  @Override
  AbstractShardedRecordStream<K, ?> getSource() {
    return (AbstractShardedRecordStream<K, ?>) super.getSource();
  }

  RS wrapAsRecordStream(Stream<Stream<Record<K>>> shards) {
    return wrapAsRecordStream(shards, getOrdering(), with());
  }

  @SuppressWarnings("unchecked")
  private RS applyAndWrap(Function<RS, RecordStream<K>> function) {
    Stream<Stream<Record<K>>> stream = apply(s -> function.apply((RS) s));
    return wrapAsRecordStream(stream);
  }

  abstract RS wrapAsRecordStream(Stream<Stream<Record<K>>> shards, Comparator<? super Record<K>> ordering, int characteristics);

  List<PipelineOperation> getMergedPipeline() {
    AbstractShardedRecordStream<K, ?> source = getSource();
    if (source.mergedStream == null) {
      return emptyList();
    } else if (source.mergedStream instanceof WrappedStream) {
      return ((WrappedStream<?, ?>) source.mergedStream).getPipeline();
    } else {
      throw new AssertionError();
    }
  }

  <T, W extends BaseStream<T, W>> W registerCommonStream(W merged) {
    this.mergedStream = merged;
    return merged;
  }

  public Collection<? extends RecordStream<K>> getInitialStreams() {
    return initialStreams;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RS explain(Consumer<Object> consumer) {
    getSource().requiresExplanation = true;
    MultiStripeExplanation explanation = new MultiStripeExplanation();
    return (RS) wrapAsRecordStream(apply(s -> ((RS) s).explain(explanation::addStripeExplanation))).onClose(() -> {
      explanation.addMergedPipeline(getMergedPipeline());
      consumer.accept(explanation);
    });
  }

  @Override
  public RS batch(int sizeHint) {
    return applyAndWrap(s -> s.batch(sizeHint));
  }

  @Override
  public RS inline() {
    return applyAndWrap(s -> s.inline());
  }

  @Override
  public RS filter(Predicate<? super Record<K>> predicate) {
    return wrapAsRecordStream(apply(s -> s.filter(predicate)));
  }

  @SuppressWarnings("unchecked")
  @Override
  public RS distinct() {
    return (RS) this;
  }

  @Override
  public RecordStream<K> sorted() {
    throw new UnsupportedOperationException("sorted() is not supported - Record is not Comparable, what you mean probably is sorted(keyFunction().asComparator())");
  }

  @Override
  public RecordStream<K> sorted(Comparator<? super Record<K>> comparator) {
    return new DetachedRecordStream<>(getInitialStreams(), super.sorted(comparator));
  }

  @Override
  public RS peek(Consumer<? super Record<K>> action) {
    return wrapAsRecordStream(apply(s -> s.peek(action)));
  }

  @Override
  public abstract RS limit(long maxSize);

  @Override
  public abstract RS skip(long n);

  Stream<Record<K>> rawLimit(long maxSize) {
    return super.limit(maxSize);
  }

  Stream<Record<K>> rawSkip(long n) {
    return super.skip(n);
  }

  @Override
  public RS sequential() {
    return wrapAsRecordStream(apply(Stream::sequential).sequential());
  }

  @Override
  public RS parallel() {
    return wrapAsRecordStream(apply(Stream::parallel).parallel());
  }

  @Override
  public RS unordered() {
    return wrapAsRecordStream(apply(Stream::unordered).unordered(), null, without(ORDERED));
  }

  @SuppressWarnings("unchecked")
  @Override
  public RS onClose(Runnable closeHandler) {
    return (RS) super.onClose(closeHandler);
  }

  boolean requiresExplanation() {
    return getSource().requiresExplanation;
  }
}
