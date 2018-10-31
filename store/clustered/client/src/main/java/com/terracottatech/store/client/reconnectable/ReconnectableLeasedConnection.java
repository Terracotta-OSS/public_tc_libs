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

import com.terracottatech.store.client.DatasetEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.lease.connection.LeasedConnection;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Wraps a {@link LeasedConnection} instance to provide support for reconnection.
 */
public class ReconnectableLeasedConnection implements LeasedConnection {

  /**
   * Maximum retry delay following a reconnect failure, in milliseconds.
   */
  private static final long MAXIMUM_RECONNECT_PAUSE = TimeUnit.SECONDS.toMillis(30);
  /**
   * Minimum reconnect retry delay, in milliseconds.
   */
  private static final long MINIMUM_RECONNECT_PAUSE = TimeUnit.SECONDS.toMillis(2);

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectableLeasedConnection.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Deque<WeakReference<AbstractReconnectableEntityRef<? extends Entity, ?, ?, ?, ?>>> entityRefs =
      new ConcurrentLinkedDeque<>();

  private final ReconnectController reconnectController = new ReconnectControllerImpl(this);

  private final ConnectionSupplier connectionSupplier;
  private final long reconnectTimeout;

  private final long maximumReconnectPause;
  private final long minimumReconnectPause;

  private volatile Runnable reconnectListener;

  @GuardedBy("lock")
  private LeasedConnection delegate;

  /**
   * Creates a new {@code ReconnectableLeasedConnection} using a supplier of {@link LeasedConnection}s.
   * @param connectionSupplier provides the {@link LeasedConnection} to be used by this connection;
   *                           the supplier is expected to provide a <b>new</b> connection each time it is called
   * @param reconnectTimeout the time limit, in milliseconds, permitted for a reconnect attempt.  If smaller
   *                         than the connection timeout, the reconnect is dominated by the connection timeout.
   * @throws ConnectionException if thrown by {@code connectionSupplier}
   */
  public ReconnectableLeasedConnection(ConnectionSupplier connectionSupplier, long reconnectTimeout) throws ConnectionException {
    this.connectionSupplier = connectionSupplier;
    this.reconnectTimeout = reconnectTimeout;
    if (reconnectTimeout == 0) {
      this.maximumReconnectPause = MAXIMUM_RECONNECT_PAUSE;
      this.minimumReconnectPause = MINIMUM_RECONNECT_PAUSE;
    } else {
      this.minimumReconnectPause = reconnectTimeout >= MINIMUM_RECONNECT_PAUSE ? MINIMUM_RECONNECT_PAUSE : TimeUnit.MILLISECONDS.toMillis(500L);
      this.maximumReconnectPause = reconnectTimeout >= MAXIMUM_RECONNECT_PAUSE ? MAXIMUM_RECONNECT_PAUSE : Math.max(reconnectTimeout, this.minimumReconnectPause);
    }
    this.delegate = requireNonNull(requireNonNull(connectionSupplier, "connectionSupplier").get(), "connectionSupplier.get()");
  }

  // For testing only
  public long reconnectTimeout() {
    return reconnectTimeout;
  }

  public void setReconnectListener(Runnable reconnectListener) {
    if (this.reconnectListener != null && reconnectListener != null) {
      throw new IllegalArgumentException("Reconnect listener is already set. It can only be removed (with a null parameter) before being set to another value).");
    }
    this.reconnectListener = reconnectListener;
  }

  /**
   * Drives a reconnect.  This method replaces the current connection with a new one obtained from
   * the {@link Supplier Supplier<LeasedConnection>} provided to the constructor.  This method
   * is called by {@link ReconnectController#withConnectionClosedHandling} when an operation using
   * this connection fails with a {@link ConnectionClosedException} or {@link ConnectionShutdownException}.
   * @throws ConnectionClosedException if this connection was closed via {@link #close()}
   * @throws InterruptedException if the thread attempting the reconnect is interrupted
   * @throws TimeoutException if the reconnect time limit is exceeded
   * @see ReconnectControllerImpl.DefaultOperationStrategy
   */
  void reconnect()
      throws ConnectionClosedException, InterruptedException, TimeoutException, EntityNotProvidedException, EntityNotFoundException, EntityVersionMismatchException {
    doReconnect();
    Runnable reconnectListener = this.reconnectListener;
    if (reconnectListener != null) {
      reconnectListener.run();
    }
  }

  private void doReconnect()
      throws ConnectionClosedException, InterruptedException, TimeoutException, EntityNotProvidedException, EntityNotFoundException, EntityVersionMismatchException {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      long deadline = System.nanoTime() + MILLISECONDS.toNanos(reconnectTimeout);
      for (int retryCount = 0; ; retryCount++) {
        /*
         * An explicitly closed connection terminates reconnection.
         */
        if (closed.get()) {
          throw new ConnectionClosedException("Connection explicitly closed");
        }

        /*
         * If this thread has been interrupted, the current reconnect effort is abandoned.
         */
        if (Thread.interrupted()) {
          throw new InterruptedException("Interrupted during reconnection");
        }

        try {
          this.delegate = requireNonNull(connectionSupplier.get(), "connectionSupplier.get()");

          // Remove any stale entries and refresh any outstanding EntityRef instances.
          Iterator<WeakReference<AbstractReconnectableEntityRef<? extends Entity, ?, ?, ?, ?>>> iterator = entityRefs.iterator();
          while (iterator.hasNext()) {
            AbstractReconnectableEntityRef<? extends Entity, ?, ?, ?, ?> entityRef = iterator.next().get();
            if (entityRef == null) {
              iterator.remove();
            } else {
              refresh(entityRef);
            }
          }
          return;

        } catch (ConnectionException | ConnectionClosedException | ConnectionShutdownException e) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reconnect failed [{}] - waiting to retry", retryCount, e);
          } else {
            LOGGER.info("Reconnect failed [{}] - waiting to retry: {}", retryCount, e.toString());
          }

          checkDeadline(deadline);

          // Retry connection after a little wait; if interrupted, abandon reconnection effort
          jitteredSleep(retryCount);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void checkDeadline(long deadline) throws TimeoutException {
    if (reconnectTimeout != 0 && (deadline - System.nanoTime()) <= 0) {
      throw new TimeoutException(
          "Reconnection abandoned; interval permitted for reconnect exceeded - " + Duration.ofMillis(reconnectTimeout));
    }
  }

  /**
   * Performs an interruptible {@code Thread.sleep} for a random number of milliseconds
   * between {@link #minimumReconnectPause} and {@link #maximumReconnectPause}.
   * @param attempt the zero-based attempt/iteration number used in jitter calculations for the sleep time
   * @throws InterruptedException if this thread is interrupted while sleeping
   */
  private void jitteredSleep(int attempt) throws InterruptedException {
    long delayBound = maximumReconnectPause - minimumReconnectPause;
    if (attempt < 6) {
      delayBound = Math.min(delayBound, minimumReconnectPause * (1 << attempt));
    }
    NANOSECONDS.sleep(MILLISECONDS.toNanos(minimumReconnectPause + ThreadLocalRandom.current().nextLong(delayBound)));
  }

  private <T extends Entity, C, U, TT extends ReconnectableEntity<T>, UU extends U>
  void refresh(AbstractReconnectableEntityRef<T, C, U, TT, UU> entityRef)
      throws EntityNotProvidedException, EntityVersionMismatchException, EntityNotFoundException {
    entityRef.swap(getEntityRefInternal(entityRef.getRefreshData()));
  }

  private LeasedConnection getDelegate() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      return this.delegate;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T extends Entity, C, U> EntityRef<T, C, U> getEntityRef(Class<T> cls, long version, String name)
      throws EntityNotProvidedException {

    // Remove any stale entries
    entityRefs.removeIf(r -> r.get() == null);

    EntityRefParms<T> entityRefParms = new EntityRefParms<>(cls, version, name);
    EntityRef<T, C, U> realEntityRef;
    try {
      realEntityRef = reconnectController.withConnectionClosedHandlingExceptionally(
          () -> getEntityRefInternal(entityRefParms));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EntityNotProvidedException) {
        throw (EntityNotProvidedException)cause;
      } else {
        LOGGER.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }
    AbstractReconnectableEntityRef<T, C, U, ?, ?> entityRef = createReconnectableEntityRef(
            cls, entityRefParms, realEntityRef);
    entityRefs.add(new WeakReference<>(entityRef));
    return entityRef;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <T extends Entity, C, U> AbstractReconnectableEntityRef<T, C, U, ?, ?> createReconnectableEntityRef(
          Class<T> cls, EntityRefParms<T> entityRefParms, EntityRef<T, C, U> realEntityRef) {
    return DatasetEntity.class.isAssignableFrom(cls)
            ? new ReconnectableDatasetEntityRef(realEntityRef, reconnectController, entityRefParms)
            : new ReconnectableEntityRef<>(realEntityRef, reconnectController, entityRefParms);
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      try {
        getDelegate().close();
      } finally {
        reconnectController.close();
      }
    }
  }

  private <T extends Entity, C, U> EntityRef<T, C, U> getEntityRefInternal(EntityRefParms<T> entityRefParms)
      throws EntityNotProvidedException {
    return getDelegate().getEntityRef(entityRefParms.entityClass, entityRefParms.version, entityRefParms.name);
  }

  private static final class EntityRefParms<T extends Entity> {
    private final Class<T> entityClass;
    private final long version;
    private final String name;

    private EntityRefParms(Class<T> entityClass, long version, String name) {
      this.entityClass = entityClass;
      this.version = version;
      this.name = name;
    }
  }

  @FunctionalInterface
  public interface ConnectionSupplier {
    LeasedConnection get() throws ConnectionException;
  }
}
