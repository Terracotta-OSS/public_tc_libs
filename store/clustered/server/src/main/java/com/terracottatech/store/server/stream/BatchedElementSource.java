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

package com.terracottatech.store.server.stream;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.IEntityMessenger;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.BaseStream;

import static com.terracottatech.store.common.messages.DatasetOperationMessageType.BATCH_FETCH_MESSAGE;
import static java.lang.Integer.min;

public class BatchedElementSource<K extends Comparable<K>, T> extends PipelineProcessor {

  private static final int MAX_BATCH_SIZE = 1000;
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedElementSource.class);

  private final ElementType elementType;
  private final BaseStream<T, ?> serverStream;
  private final Supplier<Spliterator<T>> spliteratorSupplier;

  private volatile Spliterator<T> spliterator;

  /**
   * Creates a new {@code BatchedElementSource} instance reconstructing stream against the designated {@link SovereignDataset}
   * using the portable operations supplied.
   *
   * @param owner the {@code ClientDescriptor} of the owner of this element source
   * @param streamId the identifier of this element source
   * @param elementType the stream type of this element source
   * @param dataset the {@code SovereignDataset} on which this element source is built
   * @param portableOperations the sequence of portable operations for the source stream
   *
   * @throws IllegalArgumentException  if the stream resulting from pipeline reconstruction is not the expected type,
   *        if {@code portableOperations} is not all {@code IntermediateOperation} instances,
   *        if a {@code filter} argument is a {@code WaypointMarker}, if {@code mutateThenInternal} is present,
   *        or if {@code mutateThen} argument is not an {@code IntrinsicUpdateOperation}
   */
  public BatchedElementSource(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger, ClientDescriptor owner, UUID streamId, ElementType elementType,
                              SovereignDataset<K> dataset, List<PipelineOperation> portableOperations, boolean requiresExplanation) {
    super(asyncExecutor, entityMessenger, owner, dataset.getAlias(), streamId, portableOperations);
    this.elementType = elementType;
    this.serverStream = createStream(dataset, portableOperations, requiresExplanation);
    this.spliteratorSupplier = serverStream::spliterator;
  }

  @Override
  public ElementType getElementType() {
    return elementType;
  }

  @Override
  public DatasetEntityResponse handleMessage(PipelineProcessorMessage msg) {
    if (BATCH_FETCH_MESSAGE.equals(msg.getType())) {
      return retrieveBatch((BatchFetchMessage) msg);
    } else {
      return super.handleMessage(msg);
    }
  }

  private DatasetEntityResponse retrieveBatch(BatchFetchMessage message) {
    if (spliterator == null) {
      spliterator = spliteratorSupplier.get();
    }

    int size = min(message.getRequestedSize(), MAX_BATCH_SIZE);
    List<Element> batch = new ArrayList<>(size);
    do {
      if (!spliterator.tryAdvance(i -> batch.add(elementFor(i)))) {
        break;
      }
    } while (batch.size() < size);

    return new BatchFetchResponse(getStreamId(), batch);
  }

  @Override
  public void close() {
    try {
      serverStream.close();
    } catch (Exception e) {
      LOGGER.warn("Exception raised while closing {} - {}", getStreamIdentifier(), e, e);
    } finally {
      super.close();
    }
    LOGGER.debug("Closed {}", getStreamIdentifier());
  }
}
