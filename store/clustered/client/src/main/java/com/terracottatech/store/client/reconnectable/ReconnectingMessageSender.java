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
package com.terracottatech.store.client.reconnectable;

import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;

import com.terracottatech.store.client.VoltronDatasetEntity;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link MessageSender} with a layer to drive reconnection in the event of a connection failure.
 * A {@code MessageSender} is bound to the {@link VoltronDatasetEntity} and {@link EntityClientEndpoint}
 * used to create this message sender -- following a reconnect driven by a {@link ConnectionClosedException}
 * or {@link ConnectionShutdownException}, the {@code VoltronDatasetEntity} which created this
 * {@code MessageSender} is dropped (replaced by a new one) and this message sender instance is
 * marked as failing.
 */
public class ReconnectingMessageSender implements MessageSender {

  private final MessageSender delegate;
  private final AtomicReference<ReconnectController> reconnectController = new AtomicReference<>();
  @SuppressWarnings("FieldCanBeLocal")
  private final Consumer<ReconnectController> reconnectControllerConsumer;

  public ReconnectingMessageSender(MessageSender delegate, ReconnectController reconnectController) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.reconnectController.set(requireNonNull(reconnectController, "reconnectController"));

    /*
     * If disconnected, replace the "active" ReconnectController with a "failing" ReconnectController
     * so any future operations are immediately failed.  A reference to the Consumer is held in this
     * ReconnectingMessageSender instance to prevent the Consumer from getting garbage collected
     * until this ReconnectingMessageSender instance is garbage collected [TDB-2983].
     */
    reconnectControllerConsumer = this.reconnectController::set;
    this.reconnectController.get().addDisconnectListener(reconnectControllerConsumer);
  }

  @Override
  public <R extends DatasetEntityResponse> Future<R> sendMessage(DatasetEntityMessage message, SendConfiguration configuration) {
    return new WrappedFuture<>(reconnectController.get()
        .withConnectionClosedHandlingWithoutRetry(() -> delegate.sendMessage(message, configuration)));
  }

  @Override
  public <R extends DatasetEntityResponse> R sendMessageAwaitResponse(DatasetEntityMessage message, SendConfiguration configuration) {
    return reconnectController.get()
        .withConnectionClosedHandlingWithoutRetry(() -> delegate.sendMessageAwaitResponse(message, configuration));
  }

  private class WrappedFuture<R> implements Future<R> {
    private final Future<R> delegate;

    private WrappedFuture(Future<R> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return reconnectController.get()
          .withConnectionClosedHandlingWithoutRetry(() -> delegate.cancel(mayInterruptIfRunning));
    }

    @Override
    public boolean isCancelled() {
      return reconnectController.get().withConnectionClosedHandlingWithoutRetry(delegate::isCancelled);
    }

    @Override
    public boolean isDone() {
      return reconnectController.get().withConnectionClosedHandlingWithoutRetry(delegate::isDone);
    }

    @Override
    public R get() throws InterruptedException, ExecutionException {
      try {
        return reconnectController.get().withConnectionCloseHandlingForTaskWithoutRetry(delegate::get);
      } catch (TimeoutException e) {
        throw new AssertionError("Unexpected exception from operation", e);
      }
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return reconnectController.get()
          .withConnectionCloseHandlingForTaskWithoutRetry(() -> delegate.get(timeout, unit));
    }
  }
}
