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
package com.terracottatech.store.client.stream.inline;

import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.StoreStreamNotFoundException;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.client.stream.AbstractRemoteSpliterator;
import com.terracottatech.store.client.stream.RootStreamDescriptor;
import com.terracottatech.store.common.InterruptHelper;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.ErrorResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchApplyResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchExhaustedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseMessage;

import javax.annotation.concurrent.GuardedBy;

import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static com.terracottatech.store.client.stream.AbstractRemoteBaseStream.STREAM_USED_OR_CLOSED;

/**
 * The foundation for a {@link Spliterator} implementation backed by a remote {@code InlineElementSource}.
 *
 * @param <K> the key type of the dataset providing the elements
 */
abstract class AbstractInlineRemoteSpliterator<K extends Comparable<K>, T> extends AbstractRemoteSpliterator<T> {

  private final MessageSender messageSender;

  private volatile boolean isDrained = false;
  private volatile boolean suppressRelease = false;

  /**
   * Used to combine access to {@link #releaseFuture} with a messaging operation.
   */
  private final ReentrantLock lock = new ReentrantLock();
  @GuardedBy("lock")
  private Future<PipelineProcessorResponse> releaseFuture = null;

  protected AbstractInlineRemoteSpliterator(MessageSender messageSender, UUID streamId, RootStreamDescriptor descriptor,
                                            BooleanSupplier closeUsingServer) {
    super(streamId, descriptor, closeUsingServer);
    this.messageSender = messageSender;
  }

  @Override
  public void suppressRelease() {
    suppressRelease = true;
  }

  /**
   * Fetches the next element and provides it to the {@code Consumer} supplied.  This method is intended
   * to be called from the type-specific {@link #tryAdvance(Consumer)} implementation.  The consumer is
   * not called if the stream is exhausted.
   *
   * @param elementConsumer a {@code Consumer} accepting the {@link TryAdvanceFetchApplyResponse} containing the
   *                        next element to process
   * @return {@code false} if no elements remained; {@code true} otherwise
   */
  protected final boolean consume(Consumer<TryAdvanceFetchApplyResponse<K>> elementConsumer) {
    if (isDrained) {
      return false;
    }
    // TDB-1808 Closure check positioned **after** exhaustion check to prevent issue with _buffered_
    // native Java Spliterator implementation
    checkIfClosed();

    DatasetEntityResponse response;
    try {
      response = sendReceive("fetching next", new TryAdvanceFetchMessage(getStreamId()));
    } catch (StoreStreamNotFoundException e) {
      throw new IllegalStateException(STREAM_USED_OR_CLOSED, e);
    }

    if (response instanceof TryAdvanceFetchExhaustedResponse) {
      // No more elements ...
      isDrained = true;
      return false;

    } else if (!(response instanceof TryAdvanceFetchApplyResponse)) {
      throw new IllegalStateException("Error fetching next " + this.getClass().getSimpleName()
          + " element from remote data source: unexpected response " + response);
    }

    @SuppressWarnings("unchecked")
    TryAdvanceFetchApplyResponse<K> fetchApplyResponse = (TryAdvanceFetchApplyResponse<K>)response;   // unchecked

    /*
     * With a mutative stream, a TryAdvanceFetchWaypointResponse (a subtype of TryAdvanceFetchApplyResponse)
     * is returned.  This message results in an additional message exchange with the server *before* the
     * elementConsumer.accept() method completes.  Once the accept method returns, all processing for the
     * element is complete.
     */
    elementConsumer.accept(fetchApplyResponse);

    /*
     * The response from the TryAdvanceReleaseMessage is not demanded until _after_ the send for the
     * next fetch (or close). This should avoid the need for two round-trips for every element processed.
     * When the client-side elementConsumer involves a mutative terminal operation, the server-side
     * pipeline also ends in a mutative terminal operation and does not need the TryAdvanceReleaseMessage
     * to release the pipeline for further operations -- the elementConsumer receives, internally, a
     * TryAdvanceFetchConsumedResponse signalling a call to suppressRelease() to skip the release message.
     */
    if (suppressRelease) {
      suppressRelease = false;
    } else {
      lock.lock();
      try {
        this.releaseFuture = messageSender.sendMessage(new TryAdvanceReleaseMessage(getStreamId()), SendConfiguration.FIRE_AND_FORGET);
      } finally {
        lock.unlock();
      }
    }

    return true;
  }

  @Override
  public long estimateSize() {
    // The size of a RemoteSpliterator is unknown
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return Spliterator.CONCURRENT;
  }

  /**
   * Sends the {@link PipelineProcessorMessage} provided waiting for the response to the
   * {@link TryAdvanceReleaseMessage} (through {@link #releaseFuture}) if set.  This method
   * converts an {@link ErrorResponse} from the {@code TryAdvanceReleaseMessage} or the
   * {@code PipelineProcessorMessage} into a {@link StoreRuntimeException}.
   *
   * @param action a textual indicator of the {@code pipelineProcessorMessage} operation
   * @param pipelineProcessorMessage the {@code PipelineProcessorMessage} to send
   *
   * @return the response from {@code pipelineProcessorMessage}
   *
   * @throws StoreRuntimeException if an error is encountered while retrieving the response held by
   *      {@link #releaseFuture}, sending {@code pipelineProcessorMessage} or retrieving its response, or
   *      for an {@link ErrorResponse} from either
   */
  @Override
  public DatasetEntityResponse sendReceive(String action, PipelineProcessorMessage pipelineProcessorMessage) {

    Future<PipelineProcessorResponse> capturedReleaseFuture;
    Future<PipelineProcessorResponse> actionFuture;
    lock.lock();
    try {
      capturedReleaseFuture = releaseFuture;
      releaseFuture = null;
      actionFuture = messageSender.sendMessage(pipelineProcessorMessage, SendConfiguration.ONE_SERVER);
    } finally {
      lock.unlock();
    }

    try {
      /*
       * An ErrorResponse from the release message must be returned to the caller; the response from
       * pipelineProcessorMessage is ignored -- since the release failed, the pipelineProcessorMessage response would
       * be bogus.
       */
      if (capturedReleaseFuture != null) {
        InterruptHelper.getUninterruptibly(capturedReleaseFuture);
      }

      return InterruptHelper.getUninterruptibly(actionFuture);
    } catch (ExecutionException e) {
      throw processStreamFailure(action, e.getCause());
    }
  }
}
