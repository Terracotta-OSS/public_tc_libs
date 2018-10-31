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

package com.terracottatech.store.client.indexing;

import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateAcceptedResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusResponse;
import com.terracottatech.store.common.messages.indexing.IndexDestroyMessage;
import com.terracottatech.store.common.messages.indexing.IndexListMessage;
import com.terracottatech.store.common.messages.indexing.IndexListResponse;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

import static com.terracottatech.store.common.exceptions.ReflectiveExceptionBuilder.buildThrowable;
import static com.terracottatech.store.indexing.Index.Status.LIVE;
import static java.util.stream.Collectors.toList;

/**
 * The {@link Index} manager for a clustered dataset.
 * <p>
 * The {@code Index} entries returned from this class are "live" -- a call to the {@link Index#status()}
 * method obtains the current state from the server.
 */
public class ClusteredIndexing implements Indexing {

  private final MessageSender messageSender;
  private final BooleanSupplier closed;
  private final UUID stableClientId;

  /**
   * The {@link Executor} to use for {@link #createIndex(CellDefinition, IndexSettings)} requests.
   * This field relies lazy initialization and must be accessed through {@link #getExecutor()}.
   */
  private volatile Executor executor;

  public ClusteredIndexing(MessageSender messageSender, BooleanSupplier parentClosed, UUID stableClientId) {
    this.messageSender = messageSender;
    this.closed = parentClosed;
    this.stableClientId = stableClientId;
  }

  @Override
  public <T extends Comparable<T>> Operation<Index<T>> createIndex(CellDefinition<T> cellDefinition, IndexSettings settings) {
    checkIfClosed();
    CompletableFuture<Index<T>> createFuture = CompletableFuture.supplyAsync(() -> {

      /*
       * The server uses deferred delivery of the response to this message ... once the response is
       * received, index creation is complete.  Unfortunately, the response does not have the creation
       * status -- that must be fetched using a second message.
       */
      DatasetEntityResponse createResponse = messageSender.sendMessageAwaitResponse(
          new IndexCreateMessage<>(cellDefinition, settings, stableClientId), SendConfiguration.FULL);
      if (!(createResponse instanceof IndexCreateAcceptedResponse)) {
        throw new AssertionError("Unexpected response to IndexCreateMessage: " + createResponse);
      }

      /*
       * Index creation is complete; fetch the response status.
       */
      IndexCreateAcceptedResponse acceptedResponse = (IndexCreateAcceptedResponse)createResponse;
      String creationRequestId = acceptedResponse.getCreationRequestId();
      IndexCreateStatusResponse createStatus = getIndexCreateStatus(creationRequestId, messageSender, stableClientId);

      if (createStatus.isRetry()) {
        createStatus = getIndexCreateStatus(creationRequestId, messageSender, stableClientId);
      }
      if (createStatus.getFault() != null) {
        Throwable fault = createStatus.getFault();
        if (fault instanceof IllegalArgumentException) {
          throw (IllegalArgumentException)buildThrowable(fault.getClass(), fault.getMessage(), fault);
        } else if (fault instanceof IllegalStateException) {
          throw (IllegalStateException)buildThrowable(fault.getClass(), fault.getMessage(), fault);
        } else if (fault instanceof Error) {
          throw (Error)buildThrowable(fault.getClass(), fault.getMessage(), fault);
        } else {
          throw new StoreRuntimeException(fault);
        }
      } else if (createStatus.getStatus() == null) {
        throw new AssertionError("Unexpected IndexCreateStatusResponse content");
      }

      return new LiveIndex<>(messageSender, cellDefinition, settings);
    }, getExecutor());

    return Operation.operation(createFuture);
  }

  private static IndexCreateStatusResponse getIndexCreateStatus(String creationRequestId, MessageSender messageSender, UUID stableClientId) {
    DatasetEntityResponse statusResponse = messageSender.sendMessageAwaitResponse(
        new IndexCreateStatusMessage(creationRequestId, stableClientId), SendConfiguration.ONE_SERVER);
    if (!(statusResponse instanceof IndexCreateStatusResponse)) {
      throw new AssertionError("Unexpected response to IndexCreateStatusMessage: " + statusResponse);
    }
    return (IndexCreateStatusResponse) statusResponse;
  }

  @Override
  public void destroyIndex(Index<?> index) throws StoreIndexNotFoundException {
    checkIfClosed();
    messageSender.sendMessageAwaitResponse(
        new IndexDestroyMessage<>(index.on(), index.definition()), SendConfiguration.FULL);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method returns "live" {@link Index} instances -- a call to {@link Index#status()} gets the current
   * status from the serve.
   * @return {@inheritDoc}
   */
  @Override
  public Collection<Index<?>> getLiveIndexes() {
    return getIndexes().stream()
        .filter(i -> i.status().equals(LIVE))
        .map(i -> new LiveIndex<>(messageSender, i.on(), i.definition()))
        .collect(toList());
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method returns "live" {@link Index} instances -- a call to {@link Index#status()} gets the current
   * status from the serve.
   * @return {@inheritDoc}
   */
  @Override
  public Collection<Index<?>> getAllIndexes() {
    return getIndexes().stream()
        .map(i -> new LiveIndex<>(messageSender, i.on(), i.definition()))
        .collect(toList());
  }

  /**
   * Gets a snapshot of the static {@link Index} instances extant for the associated dataset.
   * @return a static view of the existing indexes
   */
  private Collection<Index<?>> getIndexes() {
    checkIfClosed();
    return ((IndexListResponse) messageSender.sendMessageAwaitResponse(IndexListMessage.INSTANCE, SendConfiguration.ONE_SERVER)).getIndexes();
  }

  /**
   * Returns the {@link Executor} to use for {@link #createIndex(CellDefinition, IndexSettings)} requests.
   * @return the {@code Executor}
   */
  private Executor getExecutor() {
    if (executor == null) {
      synchronized (this) {
        if (executor == null) {
          executor = new CreateIndexExecutor();
        }
      }
    }
    return executor;
  }

  private void checkIfClosed() {
    if (closed.getAsBoolean()) {
      throw new StoreRuntimeException("Attempt to use Indexing after Dataset closed");
    }
  }
}
