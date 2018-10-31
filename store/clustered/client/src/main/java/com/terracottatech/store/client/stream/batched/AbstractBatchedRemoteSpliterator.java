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
package com.terracottatech.store.client.stream.batched;

import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.client.stream.AbstractRemoteSpliterator;
import com.terracottatech.store.client.stream.RootStreamDescriptor;
import com.terracottatech.store.common.InterruptHelper;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchResponse;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The foundation for a {@link Spliterator} implementation backed by a remote batched element source.
 */
abstract class AbstractBatchedRemoteSpliterator<T> extends AbstractRemoteSpliterator<T> {

  private static final int DEFAULT_BATCH_SIZE = 100;

  private final MessageSender messageSender;
  private final IntSupplier batchSizes;

  private volatile boolean isDrained = false;

  protected AbstractBatchedRemoteSpliterator(MessageSender messageSender, UUID streamId, RootStreamDescriptor descriptor,
                                             BooleanSupplier closeUsingServer) {
    super(streamId, descriptor, closeUsingServer);
    this.messageSender = messageSender;

    int batchSize = descriptor.getBatchSizeHint().orElse(DEFAULT_BATCH_SIZE);
    if (downstreamFullyConsumes(descriptor)) {
      this.batchSizes = () -> batchSize;
    } else {
      this.batchSizes = new IntSupplier() {

        int currentBatchSize = 1;
        @Override
        public int getAsInt() {
          if (currentBatchSize < batchSize) {
            int size = currentBatchSize;
            currentBatchSize <<= 1;
            return size;
          } else {
            return batchSize;
          }
        }
      };
    }
  }

  private static final Set<IntermediateOperation> FULLY_CONSUMING_INTERMEDIATES = EnumSet.of(
          IntermediateOperation.SORTED_0, IntermediateOperation.SORTED_1);

  private static boolean downstreamFullyConsumes(RootStreamDescriptor descriptor) {
    List<PipelineOperation> downstreamPipeline = descriptor.getDownstreamPipelineSequence();
    return downstreamPipeline.stream().map(PipelineOperation::getOperation)
            //find the first fully consuming or short circuiting operation
            .filter(o -> FULLY_CONSUMING_INTERMEDIATES.contains(o) || o.isShortCircuit()).findFirst()
            //it it's a short circuit then we may not fully consume, otherwise we will (short exceptions)
            .map(o -> !o.isShortCircuit()).orElse(true);
  }

  @Override
  public void suppressRelease() {
    throw new AssertionError("Can only suppress release on inline remote spliterators");
  }

  @Override
  public boolean tryAdvance(Consumer<? super T> action) {
    requireNonNull(action, "action");
    return consume(e -> action.accept(decodeElement(e)));
  }

  @Override
  public void forEachRemaining(Consumer<? super T> action) {
    requireNonNull(action, "action");
    consumeAll(e -> action.accept(decodeElement(e)));
  }

  /**
   * {@inheritDoc}
   * <p>
   * Batched remote spliterators have an unusual splitting behavior. When asked to split they will return a spliterator
   * containing the already fetched portion of the stream (if any exists).
   * <p>
   * This means a spliterator that has not started traversal is not splittable - thereby presenting an unsplittable
   * nature to the JDK. Yet the stream sharding logic can stil split at batch boundaries to allow for non-blocking batch
   * requests from multiple servers.
   */
  @Override
  public Spliterator<T> trySplit() {
    Stream<Element> available = available();
    if (available == null) {
      return null;
    } else {
      return available.map(this::decodeElement).spliterator();
    }
  }

  private final Queue<Element> buffer = new ArrayDeque<>();
  private Future<BatchFetchResponse> pendingBatch;

  protected final boolean consume(Consumer<Element> elementConsumer) {
    if (isDrained) {
      return false;
    }
    // TDB-1808 Closure check positioned **after** exhaustion check to prevent issue with _buffered_
    // native Java Spliterator implementation
    checkIfClosed();

    if (pendingBatch == null) {
      requestBatch();
    }
    if (buffer.isEmpty()) {
      retrieveBatch();
    }
    if (isDrained) {
      return false;
    } else {
      elementConsumer.accept(buffer.remove());
      return true;
    }
  }

  protected final void consumeAll(Consumer<Element> elementConsumer) {
    if (isDrained) {
      return;
    }
    // TDB-1808 Closure check positioned **after** exhaustion check to prevent issue with _buffered_
    // native Java Spliterator implementation
    checkIfClosed();

    if (pendingBatch == null) {
      requestBatch();
    }
    while (!isDrained) {
      if (buffer.isEmpty() || pendingBatch.isDone()) {
        retrieveBatch();
      }
      while (!buffer.isEmpty()) {
        elementConsumer.accept(buffer.remove());
      }
    }
  }

  protected Stream<Element> available() {
    if (pendingBatch == null) {
      requestBatch();
      //prevent splitting of untraversed spliterators - tricks the JDK in to not splitting
      return null;
    }
    if (pendingBatch.isDone()) {
      retrieveBatch();
    }

    if (buffer.isEmpty()) {
      return null;
    } else {
      ArrayList<Element> available = new ArrayList<>(buffer);
      buffer.clear();
      return available.stream();
    }
  }

  private void requestBatch() {
    pendingBatch = messageSender.sendMessage(new BatchFetchMessage(getStreamId(), batchSizes.getAsInt()), SendConfiguration.ONE_SERVER);
  }

  private void retrieveBatch() {
    buffer.addAll(processResponse("retrieving batch", pendingBatch).getElements());
    if (buffer.isEmpty()) {
      isDrained = true;
    } else {
      requestBatch();
    }
  }

  /**
   * Sends the {@link PipelineProcessorMessage} provided.
   *
   * @param action a textual indicator of the {@code pipelineProcessorMessage} operation
   * @param pipelineProcessorMessage the {@code PipelineProcessorMessage} to send
   *
   * @return the response from {@code pipelineProcessorMessage}
   *
   * @throws StoreRuntimeException if an error is encountered while sending {@code pipelineProcessorMessage} or
   *      retrieving its response.
   */
  @Override
  public DatasetEntityResponse sendReceive(String action, PipelineProcessorMessage pipelineProcessorMessage) {
    return processResponse(action, messageSender.sendMessage(pipelineProcessorMessage, SendConfiguration.ONE_SERVER));
  }

  private <R extends DatasetEntityResponse> R processResponse(String action, Future<R> responseFuture) {
    try {
      return InterruptHelper.getUninterruptibly(responseFuture);
    } catch (ExecutionException e) {
      throw processStreamFailure(action, e.getCause());
    }
  }
}
