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

import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityVersionMismatchException;

/**
 * A generic {@link EntityRef} wrapper.
 * @param <T> The entity type underlying this reference
 * @param <C> The configuration type to use when creating this entity
 * @param <U> User-data type to be passed to the {@link #fetchEntity(Object)}
 */
class ReconnectableEntityRef<T extends Entity, C, U> extends AbstractReconnectableEntityRef<T, C, U, ReconnectableEntity<T>, U> {

  ReconnectableEntityRef(EntityRef<T, C, U> delegate, ReconnectController reconnectController, Object refreshData) {
    super(delegate, reconnectController, refreshData);
  }

  @Override
  public T fetchEntity(U userData) throws EntityNotFoundException, EntityVersionMismatchException {

    T realEntity = fetchRealEntity(userData);
    ReconnectableEntity<T> reconnectableEntity = ReconnectableProxyEntity.getProxy(realEntity, reconnectController);
    fetchedEntities.put(reconnectableEntity, new EntityParameters<>(userData));
    reconnectableEntity.onClose(() -> fetchedEntities.remove(reconnectableEntity));

    @SuppressWarnings("unchecked") T wrappedEntity = (T)reconnectableEntity;
    return wrappedEntity;
  }

  @Override
  protected T fetchEntityInternal(U reconnectParameters) throws EntityNotFoundException, EntityVersionMismatchException {
    // This method is called from reconnect -- an exception is allowed to percolate up the stack
    return getDelegate().fetchEntity(reconnectParameters);
  }
}
