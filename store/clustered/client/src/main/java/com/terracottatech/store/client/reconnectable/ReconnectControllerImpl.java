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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;

import com.terracottatech.store.StoreOperationAbandonedException;
import com.terracottatech.store.StoreReconnectFailedException;
import com.terracottatech.store.StoreReconnectInterruptedException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;
import static java.util.Objects.requireNonNull;

/**
 * The per-{@link Connection} object through which re-connections driven by a
 * {@link ConnectionClosedException} or {@link ConnectionShutdownException} are managed.
 */
class ReconnectControllerImpl implements ReconnectController {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectControllerImpl.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final OperationExecutionStrategy defaultOperationStrategy = new DefaultOperationStrategy();
  private final AtomicReference<OperationExecutionStrategy> operationStrategy =
      new AtomicReference<>(this.defaultOperationStrategy);

  private final Set<Consumer<ReconnectController>> disconnectListeners = synchronizedSet(newSetFromMap(new WeakHashMap<>()));
  private final Set<Consumer<ReconnectController>> stagedListeners = new HashSet<>();

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition reconnected = this.lock.newCondition();

  private final ReconnectableLeasedConnection connection;

  ReconnectControllerImpl(ReconnectableLeasedConnection connection) {
    this.connection = connection;
  }

  /**
   * Closes this {@code ReconnectController}, sets a {@link FailedReconnectStrategy} and awakens
   * any tasks awaiting reconnection.
   */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      lock.lock();
      try {
        Supplier<ConnectionClosedException> exceptionSupplier =
            () -> new ConnectionClosedException("Connection explicitly closed");
        operationStrategy.set(new FailedReconnectStrategy(exceptionSupplier));
        notifyDisconnected(exceptionSupplier);
        // Awaken all sleepers
        reconnected.signalAll();
      } finally {
        lock.unlock();
      }
      disconnectListeners.clear();
    }
  }

  @Override
  public void addDisconnectListener(Consumer<ReconnectController> listener) {
    disconnectListeners.add(requireNonNull(listener, "listener"));
  }

  /**
   * Invokes each of the disconnect listeners.  This method presents each listener with a
   * {@link FailingReconnectController} which can replace their local instance.
   * @param failureSupplier the supplier of a {@code ConnectionClosedException} to be thrown by the controller
   */
  private void notifyDisconnected(Supplier<ConnectionClosedException> failureSupplier) {
    FailingReconnectController controller = new FailingReconnectController(failureSupplier);
    synchronized (disconnectListeners) {
      Iterator<Consumer<ReconnectController>> iterator = disconnectListeners.iterator();
      while (iterator.hasNext()) {
        Consumer<ReconnectController> listener = iterator.next();
        if (listener == null) {
          iterator.remove();
        } else {
          try {
            listener.accept(controller);
          } catch (Exception e) {
            LOGGER.debug("Disconnect listener threw", e);
          }
        }
      }

      completeDisconnected(failureSupplier);
    }
  }

  /**
   * Moves each of the disconnect listeners to the {@link #stagedListeners} set.
   * Because the reconnect process can be interrupted, this method is used to stage the existing
   * listeners until reconnect is complete.  Once complete, {@link #completeDisconnected} is called
   * to finish the job.
   */
  private void stageDisconnectedListeners() {
    synchronized (disconnectListeners) {
      disconnectListeners.stream().filter(Objects::nonNull).forEach(stagedListeners::add);
      disconnectListeners.clear();
    }
  }

  /**
   * Invokes each of the <i>staged</i> disconnect listeners.  This method presents each listener with a
   * {@link FailingReconnectController} which can replace their local instance.
   * @param failureSupplier the supplier of {@code StoreOperationAbandonedException} or
   *        {@code ConnectionClosedException} to be thrown by the controller
   */
  private void completeDisconnected(Supplier<? extends RuntimeException> failureSupplier) {
    FailingReconnectController controller = new FailingReconnectController(failureSupplier);
    synchronized (disconnectListeners) {
      stagedListeners.forEach(l -> {
        try {
          l.accept(controller);
        } catch (Exception e) {
          LOGGER.debug("Disconnect listener threw", e);
        }
      });
      stagedListeners.clear();
    }
  }

  @Override
  public <T> T withConnectionClosedHandling(Supplier<T> operation) {
    while (true) {
      try {
        return operationStrategy.get().execute(operation);
      } catch (RetryOperationException e) {
        // Retry operation ...
      }
    }
  }

  @Override
  public <T> T withConnectionClosedHandlingWithoutRetry(Supplier<T> operation) {
    try {
      return operationStrategy.get().execute(operation);
    } catch (RetryOperationException e) {
      throw new StoreOperationAbandonedException(e.getCause());
    }
  }

  @Override
  public <T> T withConnectionClosedHandlingExceptionally(ThrowingSupplier<T> operation) throws ExecutionException {
    while (true) {
      try {
        return operationStrategy.get().execute(operation);
      } catch (RetryOperationException e) {
        // Retry operation ...
      }
    }
  }

  @Override
  public <T> T withConnectionCloseHandlingForTaskWithoutRetry(TaskGetSupplier<T> operation)
      throws InterruptedException, ExecutionException, TimeoutException {
    try {
      return operationStrategy.get().execute(operation);
    } catch (RetryOperationException e) {
      throw new StoreOperationAbandonedException(e.getCause());
    }
  }

  /**
   * The base {@link ConnectionClosedException}-sensitive operation execution strategy.
   */
  private abstract class OperationExecutionStrategy {
    /**
     * Indicates if a reconnection is in progress.
     * @return {@code true} if a reconnection is in progress; {@code false} otherwise
     */
    abstract boolean reconnecting();

    <R> R execute(Supplier<R> operation) {
      try {
        return this.execute((TaskGetSupplier<R>)operation::get);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        throw new AssertionError("Unexpected exception from operation", e);
      }
    }

    abstract <R> R execute(ThrowingSupplier<R> operation) throws ExecutionException;

    abstract <R> R execute(TaskGetSupplier<R> operation) throws ExecutionException, TimeoutException, InterruptedException;

    protected void awaitReconnection() throws InterruptedException {
      ReconnectControllerImpl.this.lock.lock();
      try {
        while (ReconnectControllerImpl.this.operationStrategy.get().reconnecting()) {
          ReconnectControllerImpl.this.reconnected.await();
        }
      } finally {
        ReconnectControllerImpl.this.lock.unlock();
      }
    }
  }

  /**
   * The default operation execution strategy.  This strategy executes the operation
   * and performs a reconnection if a {@link ConnectionClosedException} is thrown by the
   * operation.
   */
  private class DefaultOperationStrategy extends OperationExecutionStrategy {
    @Override
    public boolean reconnecting() {
      return false;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is used for operations that throw <i>first-level</i> checked exceptions.  On receipt, these
     * exceptions are wrapped in an {@link ExecutionException} which the caller is expected to deconstruct.
     * @param <R> {@inheritDoc}
     * @param operation {@inheritDoc}
     * @return {@inheritDoc}
     * @throws ExecutionException {@inheritDoc}
     */
    @Override
    <R> R execute(ThrowingSupplier<R> operation) throws ExecutionException {
      try {
        return operation.get();

      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        return handleReconnect(e);

      } catch (IllegalStateException e) {
        // If a closed connection is detected at EntityClientEndpoint.beginInvoke(), an IllegalStateException is thrown
        if (e.getMessage().equals("Endpoint closed")) {
          return handleReconnect(new ConnectionClosedException(e.getMessage(), e));
        } else {
          throw e;
        }

      } catch (RuntimeException e) {
        throw e;

      } catch (Exception e) {
        throw new ExecutionException(e);
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is used for operations involving {@link Future#get} operations (and similar methods).  With these
     * methods, exceptions from the code underlying the {@code get} are wrapped in an {@link ExecutionException}.
     * This method peers inside the {@code ExecutionException} for evidence of a connection issue.
     * @param <R> {@inheritDoc}
     * @param operation {@inheritDoc}
     * @return {@inheritDoc}
     * @throws ExecutionException {@inheritDoc}
     * @throws TimeoutException {@inheritDoc}
     * @throws InterruptedException {@inheritDoc}
     */
    @Override
    <R> R execute(TaskGetSupplier<R> operation)
        throws ExecutionException, TimeoutException, InterruptedException {
      try {
        return operation.get();

      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        return handleReconnect(e);

      } catch (IllegalStateException e) {
        // If a closed connection is detected at EntityClientEndpoint.beginInvoke(), an IllegalStateException is thrown
        if (e.getMessage().equals("Endpoint closed")) {
          return handleReconnect(new ConnectionClosedException(e.getMessage(), e));
        } else {
          throw e;
        }

      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ConnectionClosedException || cause instanceof ConnectionShutdownException) {
          return handleReconnect(cause);

        } else if ((cause instanceof IllegalStateException) && cause.getMessage().equals("Endpoint closed")) {
          // If a closed connection is detected at EntityClientEndpoint.beginInvoke(), an IllegalStateException is thrown
          return handleReconnect(new ConnectionClosedException(cause.getMessage(), cause));
        } else {
          throw e;
        }
      }
    }

    private <R> R handleReconnect(Throwable operationException) {
      Supplier<StoreOperationAbandonedException> abandonedExceptionSupplier =
          () -> new StoreOperationAbandonedException(operationException);
      RetryableOperationStrategy retryableStrategy = new RetryableOperationStrategy(operationException);

      if (ReconnectControllerImpl.this.operationStrategy.compareAndSet(this, retryableStrategy)) {
        LOGGER.trace("Swapped in {} replacing {}", retryableStrategy, this);
        LOGGER.warn("Attempting reconnection following server connection failure", operationException);

        /*
         * Preserve the current set of disconnect listeners -- the reconnect process, if successful,
         * registers new listeners and the old ones must be called to record the completed disconnect
         * but preserved in the event the reconnect was interrupted.
         */
        ReconnectControllerImpl.this.stageDisconnectedListeners();

        Supplier<OperationExecutionStrategy> strategySupplier = () -> this;
        try {
          ReconnectControllerImpl.this.connection.reconnect();
          ReconnectControllerImpl.this.completeDisconnected(abandonedExceptionSupplier);
          LOGGER.warn("Connection re-established; resuming operations");

        } catch (InterruptedException e) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.warn("Interrupted during reconnection; reconnection aborted in thread {}", Thread.currentThread(), e);
          } else {
            LOGGER.warn("Interrupted during reconnection; reconnection aborted in thread {}", Thread.currentThread());
          }

          /*
           * The operation "hosting" the reconnection is failed; waiting operations are
           * released to try again.
           */
          Thread.currentThread().interrupt();   // Restore interruption since we're not throwing InterruptedException
          StoreReconnectInterruptedException abandonedException =
              new StoreReconnectInterruptedException(e);
          abandonedException.addSuppressed(operationException);
          throw abandonedException;

        } catch (Exception reconnectFailure) {
          LOGGER.error("Reconnection attempt failed; all operations against "
              + ReconnectControllerImpl.this.connection + " will fail", reconnectFailure);

          /*
           * The operation "hosting" the reconnection, all waiting operations, and all
           * future operations are failed.
           */
          ReconnectControllerImpl.this.completeDisconnected(abandonedExceptionSupplier);
          Supplier<StoreReconnectFailedException> failedReconnectExceptionSupplier = () -> {
            StoreReconnectFailedException exception = new StoreReconnectFailedException(reconnectFailure);
            exception.addSuppressed(operationException);
            return exception;
          };
          strategySupplier = () -> new FailedReconnectStrategy(failedReconnectExceptionSupplier);
          throw failedReconnectExceptionSupplier.get();

        } finally {
          /*
           * Resume operations following reconnect -- successful or failed ...
           */
          ReconnectControllerImpl.this.lock.lock();
          try {
            OperationExecutionStrategy desiredOperationStrategy = strategySupplier.get();
            if (!ReconnectControllerImpl.this.operationStrategy.compareAndSet(retryableStrategy, desiredOperationStrategy)) {
              LOGGER.trace("Failed to swap in {}; retaining {}", desiredOperationStrategy, ReconnectControllerImpl.this.operationStrategy.get());
            } else {
              LOGGER.trace("Swapped in {} replacing {}", desiredOperationStrategy, retryableStrategy);
            }
            ReconnectControllerImpl.this.reconnected.signalAll();
          } finally {
            ReconnectControllerImpl.this.lock.unlock();
          }
        }
      } else {
        LOGGER.trace("Failed to swap in {}; retaining {}", retryableStrategy, ReconnectControllerImpl.this.operationStrategy.get());
      }

      throw abandonedExceptionSupplier.get();
    }
  }

  /**
   * The execution strategy used when the reconnection attempt has failed.  This strategy
   * throws an exception for all operations attempted.
   */
  private class FailedReconnectStrategy extends OperationExecutionStrategy {
    private final Supplier<? extends RuntimeException> cause;

    FailedReconnectStrategy(Supplier<? extends RuntimeException> cause) {
      this.cause = cause;
    }

    @Override
    boolean reconnecting() {
      return false;
    }

    @Override
    <R> R execute(ThrowingSupplier<R> operation) {
      throw cause.get();
    }

    @Override
    <R> R execute(TaskGetSupplier<R> operation) {
      throw cause.get();
    }
  }

  /**
   * The execution strategy used for operations started <i>after</i> a {@link ConnectionClosedException}
   *  or {@link ConnectionShutdownException} kicks off reconnection.  This strategy throws a
   *  {@link RetryOperationException} after the reconnection is complete to permit the operation to be
   *  reattempted.  Recursive calls made by the thread handling the reconnect are executed <i>without</i>
   *  the reconnect handling observed in the {@link DefaultOperationStrategy} -- exceptions thrown by
   *  such a call are passed along to the caller.
   */
  private class RetryableOperationStrategy extends OperationExecutionStrategy {
    private final Thread reconnectingThread = Thread.currentThread();
    private final Throwable cause;

    private RetryableOperationStrategy(Throwable cause) {
      this.cause = cause;
    }

    @Override
    public boolean reconnecting() {
      return true;
    }

    @Override
    <R> R execute(ThrowingSupplier<R> operation) throws ExecutionException {
      if (Thread.currentThread() == reconnectingThread) {
        try {
          return operation.get();
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new ExecutionException(e);
        }
      } else {
        awaitReconnectionInterruptibly();
        throw new RetryOperationException(cause);
      }
    }

    @Override
    <R> R execute(TaskGetSupplier<R> operation) throws InterruptedException, ExecutionException, TimeoutException {
      if (Thread.currentThread() == reconnectingThread) {
        return operation.get();
      } else {
        awaitReconnectionInterruptibly();
        throw new RetryOperationException(cause);
      }
    }

    private void awaitReconnectionInterruptibly() {
      try {
        this.awaitReconnection();
      } catch (InterruptedException e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.warn("Interrupted waiting for reconnection; wait ended in thread {}", Thread.currentThread(), e);
        } else {
          LOGGER.warn("Interrupted waiting for reconnection; wait ended in thread {}", Thread.currentThread());
        }
        Thread.currentThread().interrupt();   // Restore interruption since we're not throwing InterruptedException
        StoreReconnectInterruptedException interruptedException = new StoreReconnectInterruptedException(e);
        interruptedException.addSuppressed(cause);
        throw interruptedException;
      }
    }
  }

  /**
   * A signal from {@link RetryableOperationStrategy} that the operation may be
   * re-attempted following a reconnect.
   */
  private static final class RetryOperationException extends RuntimeException {
    private static final long serialVersionUID = -4436627351534002604L;
    private RetryOperationException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * An always failing {@code ReconnectController}.  This controller throws the exception provided
   * by the {@code Supplier} provided.
   */
  private static class FailingReconnectController implements ReconnectController {
    private final Supplier<? extends RuntimeException> cause;

    private FailingReconnectController(Supplier<? extends RuntimeException> causeSupplier) {
      this.cause = causeSupplier;
    }

    @Override
    public void addDisconnectListener(Consumer<ReconnectController> listener) {
    }

    @Override
    public <T> T withConnectionClosedHandling(Supplier<T> operation) {
      throw cause.get();
    }

    @Override
    public <T> T withConnectionClosedHandlingWithoutRetry(Supplier<T> operation) {
      throw cause.get();
    }

    @Override
    public <T> T withConnectionClosedHandlingExceptionally(ThrowingSupplier<T> operation) {
      throw cause.get();
    }

    @Override
    public <T> T withConnectionCloseHandlingForTaskWithoutRetry(TaskGetSupplier<T> operation) {
      throw cause.get();
    }

    @Override
    public void close() {
    }
  }
}
