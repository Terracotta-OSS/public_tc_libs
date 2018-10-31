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
package com.terracottatech.store.coordination;

import com.terracottatech.store.StoreOperationAbandonedException;
import com.terracottatech.store.client.reconnectable.ReconnectableLeasedConnection;
import com.terracottatech.store.common.coordination.StateResponse;
import com.terracottatech.store.common.coordination.StateResponse.LeadershipState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.exception.PermanentEntityException;
import org.terracotta.lease.connection.LeasedConnection;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static java.time.Instant.now;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

class CoordinatorImpl implements Coordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorImpl.class);

  private final Connection connection;
  private final CoordinationEntity entity;
  private final String token;

  private final ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock();

  CoordinatorImpl(ReconnectableLeasedConnection.ConnectionSupplier connectionSupplier, String token, String name, Executor executor, Listener listener) throws ConnectionException {
    this(new ReconnectableLeasedConnection(() -> autoCreatingEntityConnection(connectionSupplier.get()), 0L), token, name, executor, listener);
  }

  private static LeasedConnection autoCreatingEntityConnection(LeasedConnection delegate) {
    return new LeasedConnection() {
      @Override
      public <T extends Entity, C, U> EntityRef<T, C, U> getEntityRef(Class<T> cls, long version, String name) throws EntityNotProvidedException {
        return autoCreatingEntityRef(cls, delegate.getEntityRef(cls, version, name));
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  private static <U, C, T extends Entity> EntityRef<T,C,U> autoCreatingEntityRef(Class<T> clazz, EntityRef<T,C,U> delegate) {
    return new EntityRef<T, C, U>() {
      @Override
      public void create(C configuration) throws EntityNotProvidedException, EntityAlreadyExistsException, EntityVersionMismatchException, EntityConfigurationException {
        throw new UnsupportedOperationException();
      }

      @Override
      public C reconfigure(C configuration) throws EntityNotProvidedException, EntityNotFoundException, EntityConfigurationException {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean destroy() throws EntityNotProvidedException, EntityNotFoundException, PermanentEntityException {
        return delegate.destroy();
      }

      @Override
      public T fetchEntity(U userData) throws EntityNotFoundException, EntityVersionMismatchException {
        while (true) {
          try {
            return delegate.fetchEntity(userData);
          } catch (EntityNotFoundException e) {
            try {
              delegate.create(null);
            } catch (EntityNotProvidedException | EntityConfigurationException f) {
              throw new EntityNotFoundException(clazz.getName(), delegate.getName(), f);
            } catch (EntityAlreadyExistsException f) {
              //ignored
            }
          }
        }
      }

      @Override
      public String getName() {
        return delegate.getName();
      }
    };
  }

  CoordinatorImpl(Connection connection, String token, String name, Executor executor, Listener listener) {
    this.connection = connection;
    this.token = token;
    Consumer<StateResponse> stateConsumer;
    if (listener == null) {
      stateConsumer = state -> {};
    } else {
      stateConsumer = new Consumer<StateResponse>() {
        StateResponse last = new StateResponse(emptySet(), null);

        @Override
        public synchronized void accept(StateResponse state) {
          try {
            calculateFirings(last, state).forEach(firing -> {
              try {
                executor.execute(() -> firing.accept(listener));
              } catch (RejectedExecutionException e) {
                LOGGER.warn("Event firing dropped by executor", e);
              }
            });
          } finally {
            last = state;
          }
        }
      };
    }
    try {
      this.entity = getEntity(connection, token, name, UUID.randomUUID(), stateConsumer, () -> {
        try {
          executor.execute(() -> getMembers());
        } catch (RejectedExecutionException e) {
          LOGGER.warn("Reconnect attempt prevented by executor - membership will be lost");
        }
      });
    } catch (Throwable t) {
      try {
        connection.close();
      } catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  private static List<Consumer<Listener>> calculateFirings(StateResponse last, StateResponse next) {
    List<Consumer<Listener>> firings = new ArrayList<>();

    Set<String> oldMembers = new HashSet<>(last.getMembers());
    Set<String> newMembers = new HashSet<>(next.getMembers());
    Optional<String> oldLeader = last.getLeader().map(LeadershipState::getName);
    Optional<String> newLeader = next.getLeader().map(LeadershipState::getName);

    if (!oldLeader.equals(newLeader)) {
      oldLeader.ifPresent(member -> firings.add(listener -> listener.leadershipRelinquishedBy(member)));
    }

    firings.addAll(oldMembers.stream().filter(m -> !newMembers.contains(m))
        .map(member -> (Consumer<Listener>) listener -> listener.memberLeft(member)).collect(toList()));
    firings.addAll(newMembers.stream().filter(m -> !oldMembers.contains(m))
        .map(member -> (Consumer<Listener>) listener -> listener.memberJoined(member)).collect(toList()));

    if (!oldLeader.equals(newLeader)) {
      newLeader.ifPresent(member -> firings.add(listener -> listener.leadershipAcquiredBy(member)));
    }

    return firings;
  }

  private static CoordinationEntity getEntity(Connection connection, String token, String name, UUID identity, Consumer<StateResponse> stateSink, Runnable reconnectTrigger) {
    try {
      return getEntityRef(connection, token).fetchEntity(new Object[] {name, identity, stateSink, reconnectTrigger});
    } catch (EntityException e) {
      throw new AssertionError(e);
    }
  }

  private static EntityRef<CoordinationEntity, Object, Object[]> getEntityRef(Connection connection, String token) throws EntityNotProvidedException {
    return connection.getEntityRef(CoordinationEntity.class, 1L, token);
  }

  @Override
  public Set<String> getMembers() {
    return shared(() -> {
      while (true) {
        try {
          return entity.getMembers();
        } catch (StoreOperationAbandonedException e) {
          //retry - nullipotent operation
        }
      }
    });
  }

  @Override
  public Optional<String> leader() {
    return shared(() -> {
      while (true) {
        try {
          return entity.leader();
        } catch (StoreOperationAbandonedException e) {
          //retry - nullipotent operation
        }
      }
    });
  }

  @Override
  public Optional<Leadership> tryAcquireLeadership() {
    return shared(() -> {
      while (true) {
        try {
          if (entity.acquireLeadership()) {
            return Optional.of(new LeadershipImpl(entity));
          } else {
            return Optional.empty();
          }
        } catch (StoreOperationAbandonedException e) {
          //retry - idempotent operation
        }
      }
    });
  }

  @Override
  public Leadership acquireLeadership(Duration timeout) throws TimeoutException, InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    Optional<Leadership> leadership = tryAcquireLeadership();
    if (leadership.isPresent()) {
      return leadership.get();
    } else {
      Duration waitPeriod = timeout.dividedBy(10);
      Instant start = now();
      Instant end = start.plus(timeout);

      while (now().isBefore(end)) {
        try {
          entity.waitForChange(waitPeriod);
        } catch (StoreOperationAbandonedException e) {
          //ignored
        }
        leadership = tryAcquireLeadership();
        if (leadership.isPresent()) {
          return leadership.get();
        }
      }
      throw new TimeoutException();
    }
  }

  @Override
  public void close() throws IOException {
    exclusively(() -> {
      try {
        entity.close();
        return null;
      } finally {
        try {
          if (getEntityRef(connection, token).destroy()) {
            LOGGER.debug("Destroyed entity on coordination client close");
          } else {
            LOGGER.trace("Entity still in use");
          }
        } catch (EntityException | PermanentEntityException e) {
          LOGGER.info("Failed to destroy coordination entity: {}", token, e);
        } finally {
          connection.close();
        }
      }
    });
  }

  private <T, E extends Throwable> T shared(Task<T, E> task) throws E {
    ReentrantReadWriteLock.ReadLock lock = accessLock.readLock();
    lock.lock();
    try {
      return task.run();
    } finally {
      lock.unlock();
    }
  }
  private <T, E extends Throwable> T exclusively(Task<T, E> task) throws E {
    ReentrantReadWriteLock.WriteLock lock = accessLock.writeLock();
    lock.lock();
    try {
      return task.run();
    } finally {
      lock.unlock();
    }
  }

  interface Task<T, E extends Throwable> {
    T run() throws E;
  }
}
