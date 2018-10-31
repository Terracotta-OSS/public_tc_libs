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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.UUID;
import java.util.function.DoubleConsumer;

/**
 * {@link Spliterator.OfDouble} wrapper applying {@link RemoteSpliterator.OfDouble}.
 */
final class TestRemoteSpliteratorOfDouble
    implements RemoteSpliterator.OfDouble {

  private final UUID streamId;
  private final Spliterator.OfDouble delegate;

  private TestRemoteSpliteratorOfDouble(Spliterator.OfDouble delegate, UUID streamId) {
    this.delegate = delegate;
    this.streamId = streamId;
  }

  public TestRemoteSpliteratorOfDouble(Spliterator.OfDouble delegate) {
    this(delegate, UUID.randomUUID());
  }

  @Override
  public UUID getStreamId() {
    return streamId;
  }

  @Override
  public void suppressRelease() {
    // ignored
  }

  @Override
  public Spliterator.OfDouble trySplit() {
    Spliterator.OfDouble split = delegate.trySplit();
    if (split != null) {
      split = new TestRemoteSpliteratorOfDouble(split, streamId);
    }
    return split;
  }

  @Override
  public boolean tryAdvance(DoubleConsumer action) {
    return delegate.tryAdvance(action);
  }

  @Override
  public long estimateSize() {
    return delegate.estimateSize();
  }

  @Override
  public long getExactSizeIfKnown() {
    return delegate.getExactSizeIfKnown();
  }

  @Override
  public int characteristics() {
    return delegate.characteristics();
  }

  @Override
  public boolean hasCharacteristics(int characteristics) {
    return delegate.hasCharacteristics(characteristics);
  }

  @Override
  public Comparator<? super Double> getComparator() {
    return delegate.getComparator();
  }

  @Override
  public DatasetEntityResponse sendReceive(String action, PipelineProcessorMessage pipelineProcessorMessage) {
    throw new UnsupportedOperationException("TestRemoteSpliteratorOfDouble.sendReceive");
  }
}
