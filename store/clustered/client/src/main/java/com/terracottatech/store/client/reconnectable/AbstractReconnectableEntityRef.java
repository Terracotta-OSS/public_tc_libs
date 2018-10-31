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
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.exception.PermanentEntityException;

import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import static java.util.Collections.synchronizedMap;

/**
 * An {@link EntityRef} wrapper that permits reconnection/replacement of the wrapped entity in the event of a
 * connection recovery.  An {@code EntityRef} instance, which is bound to a
 * {@link org.terracotta.connection.Connection Connection} can be held by a caller of
 * {@link org.terracotta.connection.Connection#getEntityRef(Class, long, String) connection.getEntityRef}.
 * During a reconnection, the wrapped entityRef must be replaced with one obtained from the new connection using
 * the {@link #swap(EntityRef)} method.
 * @param <T> The entity type underlying this reference
 * @param <C> The configuration type to use when creating this entity
 * @param <U> User-data type to be passed to the {@link #fetchEntity(Object)}
 * @param <TT> the reconnectable wrapper for {@code <T>}
 * @param <UU> user-data subclass required by the reconnectable wrapper
 */
abstract class AbstractReconnectableEntityRef<T extends Entity, C, U, TT extends ReconnectableEntity<T>, UU extends U>
    implements EntityRef<T, C, U> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReconnectableEntityRef.class);

  protected final Map<TT, EntityParameters<UU>> fetchedEntities = synchronizedMap(new WeakHashMap<>());
  protected final ReconnectController reconnectController;
  private final Object refreshData;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  @GuardedBy("lock")
  private EntityRef<T, C, U> delegate;

  AbstractReconnectableEntityRef(EntityRef<T, C, U> delegate, ReconnectController reconnectController, Object refreshData) {
    this.delegate = delegate;
    this.reconnectController = reconnectController;
    this.refreshData = refreshData;
  }

  @SuppressWarnings("unchecked")
  <R> R getRefreshData() {
    return (R)refreshData;
  }

  protected EntityRef<T, C, U> getDelegate() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      return delegate;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Replace the delegate {@link EntityRef} with a new one.
   */
  void swap(EntityRef<T, C, U> newDelegate) throws EntityNotFoundException, EntityVersionMismatchException {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      this.delegate = newDelegate;
    } finally {
      lock.unlock();
    }
    refresh();
  }

  /**
   * Perform entity-specific refresh activity.  This is performed under a lock held by {@link #swap(EntityRef)} which
   * prevents use of the delegate while refresh is in progress.
   */
  private void refresh() throws EntityVersionMismatchException, EntityNotFoundException {
    // Update all fetched Entity instances & remove stale ones
    synchronized (fetchedEntities) {
      Iterator<Map.Entry<TT, EntityParameters<UU>>> iterator = fetchedEntities.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<TT, EntityParameters<UU>> entry = iterator.next();
        TT entity = entry.getKey();
        if (entity == null) {
          iterator.remove();
        } else {
          entity.swap(fetchEntityInternal(entry.getValue().parameters()));
        }
      }
    }
  }

  @Override
  public void create(C configuration)
      throws EntityNotProvidedException, EntityAlreadyExistsException, EntityVersionMismatchException, EntityConfigurationException {
    try {
      reconnectController.withConnectionClosedHandlingExceptionally(() -> {
        getDelegate().create(configuration);
        return null;
      });
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EntityNotProvidedException) {
        throw (EntityNotProvidedException)cause;
      } else if (cause instanceof EntityAlreadyExistsException) {
        throw (EntityAlreadyExistsException)cause;
      } else if (cause instanceof EntityVersionMismatchException) {
        throw (EntityVersionMismatchException)cause;
      } else if (cause instanceof EntityConfigurationException) {
        throw (EntityConfigurationException)cause;
      } else {
        LOGGER.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public C reconfigure(C configuration)
      throws EntityNotProvidedException, EntityNotFoundException, EntityConfigurationException {
    try {
      return reconnectController.withConnectionClosedHandlingExceptionally(() -> getDelegate().reconfigure(configuration));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EntityNotProvidedException) {
        throw (EntityNotProvidedException)cause;
      } else if (cause instanceof EntityNotFoundException) {
        throw (EntityNotFoundException)cause;
      } else if (cause instanceof EntityConfigurationException) {
        throw (EntityConfigurationException)cause;
      } else {
        LOGGER.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean destroy() throws EntityNotProvidedException, EntityNotFoundException, PermanentEntityException {
    try {
      return reconnectController.withConnectionClosedHandlingExceptionally(() -> getDelegate().destroy());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EntityNotProvidedException) {
        throw (EntityNotProvidedException)cause;
      } else if (cause instanceof EntityNotFoundException) {
        throw (EntityNotFoundException)cause;
      } else {
        LOGGER.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public String getName() {
    return getDelegate().getName();
  }

  @Override
  public abstract T fetchEntity(U userData) throws EntityNotFoundException, EntityVersionMismatchException;

  protected final T fetchRealEntity(UU parameters)
      throws EntityNotFoundException, EntityVersionMismatchException {
    T realEntity;
    try {
      realEntity = reconnectController.withConnectionClosedHandlingExceptionally(() -> fetchEntityInternal(parameters));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EntityNotFoundException) {
        throw (EntityNotFoundException)cause;
      } else if (cause instanceof EntityVersionMismatchException) {
        throw (EntityVersionMismatchException)cause;
      } else {
        LOGGER.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }
    return realEntity;
  }

  protected abstract T fetchEntityInternal(UU reconnectParameters)
      throws EntityNotFoundException, EntityVersionMismatchException;


  /**
   * A non-null carrier of {@code userData} for {@link Entity} creation.
   * @param <P> the {@code userData} type
   */
  static class EntityParameters<P> {
    private final P parameters;

    EntityParameters(P parameters) {
      this.parameters = parameters;
    }

    P parameters() {
      return parameters;
    }
  }
}
