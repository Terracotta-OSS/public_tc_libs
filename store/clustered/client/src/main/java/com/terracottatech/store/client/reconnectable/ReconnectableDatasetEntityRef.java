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

import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityVersionMismatchException;

import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.common.DatasetEntityConfiguration;

/**
 * An {@link EntityRef} wrapper that permits reconnection/replacement of a wrapped {@link DatasetEntity} in the
 * event of a connection recovery.
 *
 * @param <K> the key type of the dataset
 */
class ReconnectableDatasetEntityRef<K extends Comparable<K>>
    extends AbstractReconnectableEntityRef<DatasetEntity<K>, DatasetEntityConfiguration<K>, DatasetEntity.Parameters,
    ReconnectableDatasetEntity<K>, ReconnectableDatasetEntity.Parameters> {

  ReconnectableDatasetEntityRef(
      EntityRef<DatasetEntity<K>, DatasetEntityConfiguration<K>, DatasetEntity.Parameters> delegate,
      ReconnectController reconnectController, Object refreshData) {
    super(delegate, reconnectController, refreshData);
  }

  @Override
  public DatasetEntity<K> fetchEntity(DatasetEntity.Parameters userData) throws EntityNotFoundException, EntityVersionMismatchException {
    if (userData != null) {
      throw new IllegalArgumentException("userData expected to be null");
    }

    EntityParameters<ReconnectableDatasetEntity.Parameters> entityParameters =
        new EntityParameters<>(new ReconnectableDatasetEntity.Parameters(reconnectController, null));

    DatasetEntity<K> realEntity = fetchRealEntity(entityParameters.parameters());
    ReconnectableDatasetEntity<K> datasetEntity = new ReconnectableDatasetEntity<>(realEntity, reconnectController);
    fetchedEntities.put(datasetEntity, entityParameters);
    datasetEntity.onClose(() -> fetchedEntities.remove(datasetEntity));

    return datasetEntity;
  }

  @Override
  protected DatasetEntity<K> fetchEntityInternal(ReconnectableDatasetEntity.Parameters reconnectParameters)
      throws EntityNotFoundException, EntityVersionMismatchException {
    // This method is called from reconnect -- an exception is allowed to percolate up the stack
    return getDelegate().fetchEntity(reconnectParameters);
  }
}
