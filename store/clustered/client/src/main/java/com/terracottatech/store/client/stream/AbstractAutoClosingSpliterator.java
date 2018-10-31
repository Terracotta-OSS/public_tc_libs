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

import com.terracottatech.store.StoreStreamTerminatedException;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * A delegating {@link RemoteSpliterator} that closes the associated {@link Stream} once the spliterator is exhausted.
 */
abstract class AbstractAutoClosingSpliterator<T, R_SPLIT extends RemoteSpliterator<T>, T_SPLIT extends Spliterator<T>>
    implements RemoteSpliterator<T>, AutoCloseable {

  private final Set<Spliterator<T>> unconsumed;

  protected final T_SPLIT delegate;
  private final R_SPLIT baseDelegate;
  private final BaseStream<?, ?> headStream;

  @SuppressWarnings("unchecked")
  protected AbstractAutoClosingSpliterator(R_SPLIT delegate, BaseStream<?, ?> headStream) {
    this(Collections.newSetFromMap(new ConcurrentHashMap<>(1)), delegate, (T_SPLIT) delegate, headStream);
  }

  protected AbstractAutoClosingSpliterator(AbstractAutoClosingSpliterator<T, R_SPLIT, T_SPLIT> owner, T_SPLIT delegate) {
    this(owner.unconsumed, owner.baseDelegate, delegate, owner.headStream);
  }

  private AbstractAutoClosingSpliterator(Set<Spliterator<T>> unconsumed, R_SPLIT baseDelegate, T_SPLIT delegate, BaseStream<?, ?> headStream) {
    this.unconsumed = unconsumed;
    this.delegate = delegate;
    this.baseDelegate = baseDelegate;
    this.headStream = headStream;

    unconsumed.add(this);
  }

  @Override
  public UUID getStreamId() {
    return baseDelegate.getStreamId();
  }

  @Override
  public DatasetEntityResponse sendReceive(String action, PipelineProcessorMessage pipelineProcessorMessage) {
    return baseDelegate.sendReceive(action, pipelineProcessorMessage);
  }

  @Override
  public void suppressRelease() {
    baseDelegate.suppressRelease();
  }

  @Override
  public final void close() {
    if (unconsumed.remove(this) && unconsumed.isEmpty()) {
      headStream.close();
    }
  }

  @Override
  public final boolean tryAdvance(Consumer<? super T> action) {
    return tryAdvanceInternal(() -> delegate.tryAdvance(action));
  }

  @Override
  public final void forEachRemaining(Consumer<? super T> action) {
    forEachRemainingInternal(() -> delegate.forEachRemaining(action));
  }

  /**
   * Performs a type-appropriate {@link Spliterator#tryAdvance} operation closing the stream once exhausted.
   * @param proc a {@code BooleanSupplier} to invoke the type-specific {@code tryAdvance} operation
   * @return {@code true} if the stream has elements remaining; {@code false} otherwise
   */
  protected boolean tryAdvanceInternal(BooleanSupplier proc) {
    boolean hasRemaining = proc.getAsBoolean();
    if (!hasRemaining) {
      close();
    }
    return hasRemaining;
  }

  /**
   * Performs a type-appropriate {@link Spliterator#forEachRemaining} operation wrapped in with stream closure.
   * @param proc a {@code Procedure} to invoke the {@code forEachRemaining} operation
   */
  protected final void forEachRemainingInternal(Procedure proc) {
    boolean closeRequired = true;
    try {
      proc.invoke();

    } catch (StoreStreamTerminatedException e) {
      closeRequired = false;
      throw e;
    } catch (RuntimeException actionException) {
      try {
        closeRequired = false;
        unconsumed.clear();
        headStream.close();
      } catch (Exception e) {
        actionException.addSuppressed(e);
      }
      throw actionException;

    } finally {
      if (closeRequired) {
        close();
      }
    }
  }

  @Override
  public final T_SPLIT trySplit() {
    @SuppressWarnings("unchecked")
    T_SPLIT split = (T_SPLIT) delegate.trySplit();
    if (split == null) {
      return null;
    } else {
      return chainedSpliterator(split);
    }
  }

  protected abstract T_SPLIT chainedSpliterator(T_SPLIT split);

  @Override
  public final long estimateSize() {
    return delegate.estimateSize();
  }

  @Override
  public final long getExactSizeIfKnown() {
    return delegate.getExactSizeIfKnown();
  }

  @Override
  public final int characteristics() {
    return delegate.characteristics();
  }

  @Override
  public final boolean hasCharacteristics(int characteristics) {
    return delegate.hasCharacteristics(characteristics);
  }

  @Override
  public final Comparator<? super T> getComparator() {
    return delegate.getComparator();
  }

  @FunctionalInterface
  protected interface Procedure {
    void invoke();
  }
}
