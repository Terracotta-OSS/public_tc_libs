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

import org.terracotta.connection.Connection;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;

import com.terracottatech.store.StoreOperationAbandonedException;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Provides connection failure detection and automatic reconnection for operations.
 */
public interface ReconnectController extends Closeable {
  /**
   * Adds a listener that is notified when a {@link Connection} is observed as disconnected
   * (an operation raised a {@link ConnectionClosedException} or {@link ConnectionShutdownException}).
   * Registered listeners are <b>not</b> called when the connection is explicitly closed. This capability
   * is use primarily to inform {@link ReconnectingMessageSender} instances to abandon themselves.
   * The argument to the listener is a {@link ReconnectController} instance which fails operations
   * with the original disconnection exception.
   * <p>
   * The caller must maintain a field-level reference to the {@code Consumer} provided -- the
   * listener registration is maintained using a {@link java.lang.ref.WeakReference WeakReference}
   * and may be prematurely garbage collected if the caller does not maintain a hard reference.
   * @param listener the runnable to invoke at disconnect
   */
  void addDisconnectListener(Consumer<ReconnectController> listener);

  /**
   * Perform an operation watching for and handling connection failures.  If the connection
   * was closed and reconnect in progress before the operation is invoked, the operation is
   * held pending reconnect completion.
   * @param operation the operation to perform
   * @param <T> the return type of the operation
   * @return the value returned by the operation
   * @throws StoreOperationAbandonedException if an operation was aborted due to a connection failure
   */
  <T> T withConnectionClosedHandling(Supplier<T> operation);

  /**
   * Perform an operation watching for and handling connection failures.
   * @param operation the operation to perform
   * @param <T> the return type of the operation
   * @return the value returned by the operation
   * @throws StoreOperationAbandonedException if an operation was aborted due to a connection failure
   */
  <T> T withConnectionClosedHandlingWithoutRetry(Supplier<T> operation);

  /**
   * Perform an operation watching for and handling connection failures.  If the connection
   * was closed and reconnect in progress before the operation is invoked, the operation is
   * held pending reconnect completion.
   * <p>
   * This method is used for operations that throw <i>first-level</i> checked exceptions.  On receipt, these
   * exceptions are wrapped in an {@link ExecutionException} which the caller is expected to deconstruct.
   * @param operation the operation to perform
   * @param <T> the return type of the operation
   * @return the value returned by the operation
   * @throws StoreOperationAbandonedException if an operation was aborted due to a connection failure
   * @throws ExecutionException if {@code operation} threw a checked exception
   */
  <T> T withConnectionClosedHandlingExceptionally(ThrowingSupplier<T> operation) throws ExecutionException;

  /**
   * Perform an operation watching for and handling connection failures.
   * <p>
   * This method is used for operations involving {@link Future#get} operations (and similar methods).  With these
   * methods, exceptions from the code underlying the {@code get} are wrapped in an {@link ExecutionException}.
   * This method peers inside the {@code ExecutionException} for evidence of a connection issue.
   * @param operation the operation to perform
   * @param <T> the return type of the operation
   * @return the value returned by the operation
   * @throws InterruptedException if the operation was interrupted
   * @throws ExecutionException if {@code operation} threw a checked exception
   * @throws TimeoutException if {@code operation}, when timed, timed out
   * @throws StoreOperationAbandonedException if an operation was aborted due to a connection failure
   */
  <T> T withConnectionCloseHandlingForTaskWithoutRetry(TaskGetSupplier<T> operation)
      throws InterruptedException, ExecutionException, TimeoutException;

  @Override
  void close();

  @FunctionalInterface
  interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  @FunctionalInterface
  interface TaskGetSupplier<T> {
    T get() throws InterruptedException, ExecutionException, TimeoutException;
  }
}
