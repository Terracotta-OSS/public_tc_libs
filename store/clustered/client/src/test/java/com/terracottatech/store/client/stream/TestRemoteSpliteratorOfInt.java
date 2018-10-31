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
import java.util.function.IntConsumer;

/**
 * {@link Spliterator.OfInt} wrapper applying {@link RemoteSpliterator.OfInt}.
 */
final class TestRemoteSpliteratorOfInt
    implements RemoteSpliterator.OfInt {

  private final UUID streamId;
  private Spliterator.OfInt delegate;

  private TestRemoteSpliteratorOfInt(Spliterator.OfInt delegate, UUID streamId) {
    this.streamId = streamId;
    this.delegate = delegate;
  }

  public TestRemoteSpliteratorOfInt(Spliterator.OfInt delegate) {
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
  public Spliterator.OfInt trySplit() {
    Spliterator.OfInt split = delegate.trySplit();
    if (split != null) {
      split = new TestRemoteSpliteratorOfInt(split, streamId);
    }
    return split;
  }

  @Override
  public boolean tryAdvance(IntConsumer action) {
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
  public Comparator<? super Integer> getComparator() {
    return delegate.getComparator();
  }

  @Override
  public DatasetEntityResponse sendReceive(String action, PipelineProcessorMessage pipelineProcessorMessage) {
    throw new UnsupportedOperationException("TestRemoteSpliteratorOfInt.sendReceive");
  }
}
