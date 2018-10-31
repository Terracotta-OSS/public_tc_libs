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
package com.terracottatech.connection;

import com.tc.util.ManagedServiceLoader;
import com.terracottatech.connection.disconnect.DisconnectListener;
import com.terracottatech.entity.AggregateEndpoint;
import com.terracottatech.entity.EntityAggregatingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.connection.Connection;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class BasicMultiConnection implements LeasedConnection, DisconnectListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicMultiConnection.class);

  private final List<Connection> connections;
  private final SystemCatalog catalog;

  public BasicMultiConnection(List<Connection> connections) {
    this.connections = connections;
    try {
//  always use the first stripe for the system catalog
      catalog = connections.iterator().next().getEntityRef(SystemCatalog.class, SystemCatalog.VERSION, SystemCatalog.ENTITY_NAME).fetchEntity(null);
    } catch (EntityNotFoundException | EntityNotProvidedException | EntityVersionMismatchException ee) {
      throw new RuntimeException(ee);
    }
  }

  public int connectionCount() {
    return connections.size();
  }

  @Override
  public void disconnected() {
    close();
  }

  @Override
  public synchronized <T extends Entity, C, U> EntityRef<T, C, U> getEntityRef(Class<T> cls, long version, String name) throws EntityNotProvidedException {
    return new AggregateEntityRef<>(cls, version, name);
  }

  public void close() {
    for (Connection connection : connections) {
      try {
        connection.close();
      } catch (IllegalStateException e) {
        // We could get one of these in several cases, but particularly when close() is called from disconnected()
      } catch (Exception e) {
        LOGGER.error("Failed to close a stripe connection", e);
      }
    }
  }

  private class AggregateEntityRef<T extends Entity, C, U> implements EntityRef<T, C, U> {

    private final ManagedServiceLoader serviceLoader;
    private final Class<T> type;
    private final long version;
    private final String name;
    private final Map<EntityRef<T, C, U>, Connection> mapped = new LinkedHashMap<>();
    private final EntityAggregatingService<T, C> agg;

    @SuppressWarnings("rawtypes")
    AggregateEntityRef(Class<T> type, long version, String name) throws EntityNotProvidedException {
      this.serviceLoader = new ManagedServiceLoader();
      this.type = type;
      this.version = version;
      this.name = name;

      EntityAggregatingService<T, C> verify = null;
      List<Class<? extends EntityAggregatingService>> classes = serviceLoader.getImplementations(EntityAggregatingService.class, type.getClassLoader());
      for (Class<? extends EntityAggregatingService> serviceClass : classes) {
        try {
          @SuppressWarnings("unchecked")
          EntityAggregatingService<T, C> check = serviceClass.getDeclaredConstructor().newInstance();
          if (check.handlesEntityType(type)) {
            verify = check;
            break;
          }
        } catch (ReflectiveOperationException e) {
          // Ignore if we fail to instantiate
          LOGGER.debug("Failed to instantiate entity aggregating service - " + serviceClass, e);
        }
      }
      if (verify == null) {
        throw new EntityNotProvidedException(type.getName(), name);
      }
      agg = verify;
    }

    private void lock() {
      while (!catalog.tryLock(type, name)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    }

    private void unlock() {
      catalog.unlock(type, name);
    }

    @Override
    public void create(C configuration) throws EntityConfigurationException, EntityNotProvidedException, EntityAlreadyExistsException, EntityVersionMismatchException {
      lock();
      try {
        Object exists = catalog.getConfiguration(type, name);
        if (exists != null) {
          throw new EntityAlreadyExistsException(type.getName(), name);
        }
        int index = 0;
        ArrayList<EntityRef<T, C, U>> inflight = new ArrayList<>();
        for (Connection connection : connections) {
          if (agg.targetConnectionForLifecycle(index, connections.size(), name, configuration)) {
            C stripeConfiguration = agg.formulateConfigurationForStripe(index, connections.size(), name, configuration);
            try {
              EntityRef<T, C, U> ref = connection.getEntityRef(type, version, name);
              ref.create(stripeConfiguration);
              mapped.put(ref, connection);
              inflight.add(ref);
            } catch (EntityException ee) {
              for (EntityRef<T, C, U> r : inflight) {
                try {
                  mapped.remove(r);
                  r.destroy();
                } catch (Throwable t) {
                  LOGGER.warn("Exception seen while cleaning up failed create entity " + type + " " + name, t);
                }
              }
              handleCreateException(ee);
            }
          }
          index += 1;
        }
        byte[] data = agg.serializeConfiguration(configuration);
        if (data == null) {
          data = new byte[0];
        }
        catalog.storeConfiguration(type, name, data);
      } finally {
        unlock();
      }
    }

    @Override
    public C reconfigure(C configuration) throws EntityConfigurationException, EntityNotFoundException, EntityNotProvidedException {
      lock();
      byte[] replace = agg.serializeConfiguration(configuration);
      byte[] raw = catalog.storeConfiguration(type, name, replace);
      if (raw == null) {
        throw new EntityNotFoundException(type.getName(), name);
      }
      C original = agg.deserializeConfiguration(raw);
      try {
        int index = 0;
        Iterator<Connection> check = connections.iterator();
        for (Map.Entry<EntityRef<T,C, U>, Connection> entry : mapConnections(original).entrySet()) {
          while (entry.getValue() != check.next()) {
            // there should be no way for the iterator to overrun the
            // map iteration,  they are in the same order.  If not, big trouble
            index += 1;
          }
          C stripeConfiguration = agg.formulateConfigurationForStripe(index, connections.size(), name, configuration);
          EntityRef<T, C, U> ref = entry.getKey();
          try {
            ref.reconfigure(stripeConfiguration);
          } catch (EntityException ee) {
            if  (ee instanceof EntityConfigurationException) {
              throw (EntityConfigurationException)ee;
            } else if (ee instanceof EntityNotProvidedException) {
              throw (EntityNotProvidedException)ee;
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      } finally {
        unlock();
      }
      return original;
    }

    @Override
    public boolean destroy() throws PermanentEntityException, EntityNotProvidedException, EntityNotFoundException {
      lock();
      try {
        byte[] raw = catalog.getConfiguration(type, name);
        if (raw == null) {
          throw new EntityNotFoundException(type.getName(), name);
        }
        C config = agg.deserializeConfiguration(raw);
        boolean first = true;
        for (EntityRef<?, ?, ?> c : mapConnections(config).keySet()) {
          try {
            if (!c.destroy()) {
              if (first) {
                return false;
              } else {
                IOException exc = new IOException();
                LOGGER.error("Error destroying entity " + type + " " + name, exc);
                throw new MultiConnectionDestroyException(type.getName(), name, exc);
              }
            }
          } catch (Exception e) {
            LOGGER.error("Error destroying entity " + type + " " + name, e);
            handleDestroyException(e);
          }
          first = false;
        }
        raw = catalog.removeConfiguration(type, name);
        if (raw == null) {
          throw new AssertionError();
        }
      } finally {
        unlock();
      }
      return true;
    }

    @Override
    public T fetchEntity(U userData) throws EntityNotFoundException, EntityVersionMismatchException {
      lock();
      final List<T> map = new ArrayList<>();
      final C config;
      try {
        byte[] raw = catalog.getConfiguration(type, name);
        if (raw == null) {
          throw new EntityNotFoundException(type.getName(), name);
        }
        config = agg.deserializeConfiguration(raw);
        for (Map.Entry<EntityRef<T,C, U>, Connection> c : mapConnections(config).entrySet()) {
          T f = c.getKey().fetchEntity(userData);
          map.add(f);
        }
      } catch (EntityNotProvidedException not) {
        throw new RuntimeException(not);
      } finally {
        unlock();
      }
      final List<T> entities = Collections.unmodifiableList(map);
      return agg.aggregateEntities(new AggregateEndpoint<T>() {
        @Override
        public List<T> getEntities() {
          return entities;
        }

        @Override
        public String getName() {
          return name;
        }

        @Override
        public long getVersion() {
          return version;
        }

        @Override
        public void close() {
          lock();
          try {
            for (T e : entities) {
              e.close();
            }
          } finally {
            unlock();
          }
        }
      });
    }

    private void handleCreateException(EntityException ee)  throws EntityConfigurationException, EntityNotProvidedException, EntityAlreadyExistsException, EntityVersionMismatchException {
      if (ee instanceof EntityConfigurationException) {
        throw (EntityConfigurationException)ee;
      } else if (ee instanceof EntityNotProvidedException) {
        throw (EntityNotProvidedException)ee;
      } else if (ee instanceof EntityAlreadyExistsException) {
        throw (EntityAlreadyExistsException)ee;
      } else if (ee instanceof EntityVersionMismatchException) {
        throw (EntityVersionMismatchException)ee;
      } else {
        throw new RuntimeException(ee);
      }
    }

    private void handleDestroyException(Exception ee) throws PermanentEntityException, EntityNotProvidedException, EntityNotFoundException {
      if (ee instanceof PermanentEntityException) {
        throw (PermanentEntityException) ee;
      } else if (ee instanceof EntityNotProvidedException) {
        throw (EntityNotProvidedException) ee;
      } else if (ee instanceof EntityNotFoundException) {
        throw (EntityNotFoundException) ee;
      } else if (ee instanceof RuntimeException) {
        throw (RuntimeException) ee;
      } else {
        throw new RuntimeException(ee);
      }
    }

    private Map<EntityRef<T, C, U>, Connection> mapConnections(C configuration) throws EntityNotProvidedException {
      if (mapped.isEmpty()) {
        int index = 0;
        for (Connection named : connections) {
          if (agg.targetConnectionForLifecycle(index, connections.size(), name, configuration)) {
            EntityRef<T, C, U> ref = named.getEntityRef(type, version, name);
            if (ref == null) {
              throw new EntityNotProvidedException(type.getName(), name);
            }
            mapped.put(ref, named);
          }
          index += 1;
        }
      }
      return mapped;
    }

    @Override
    public String getName() {
      return name;
    }

  }
}
