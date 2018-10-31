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
package com.terracottatech.store.client;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.builder.datasetconfiguration.ClusteredDatasetConfigurationBuilder;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.DatasetEntityInfo;
import com.terracottatech.store.common.messages.ConfigurationEncoder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration.ClientSideConfiguration;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.exception.PermanentEntityException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of DatasetManager that uses the Voltron API to manage Voltron entities in the cluster.
 */
public class ClusteredDatasetManager implements DatasetManager {

  private final Connection connection;
  private final ClientSideConfiguration clientSideConfiguration;

  /**
   * Constructs a new ClusteredDatasetManager.
   *
   * @param connection the connection to Voltron - ownership is transferred to the constructed object
   */
  public ClusteredDatasetManager(Connection connection, ClientSideConfiguration clientSideConfiguration) {
    this.connection = connection;
    this.clientSideConfiguration = clientSideConfiguration;
  }

  public Connection getConnection() {
    return connection;
  }

  @Override
  public DatasetManagerConfiguration getDatasetManagerConfiguration() {
    try {
      Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasetInfoMap = new HashMap<>();
      Map<String, DatasetEntityConfiguration<?>> existingDatasets = getExistingDatasets();
      for (Map.Entry<String, DatasetEntityConfiguration<?>> entry : existingDatasets.entrySet()) {
        String datasetName = entry.getKey();
        DatasetEntityConfiguration<?> datasetEntityConfiguration = entry.getValue();
        datasetInfoMap.put(datasetName,
                           new DatasetManagerConfiguration.DatasetInfo<>(datasetEntityConfiguration.getKeyType(),
                                                                         datasetEntityConfiguration.getDatasetConfiguration()));
      }
      return this.clientSideConfiguration.withServerSideConfiguration(datasetInfoMap);
    } catch (EntityNotProvidedException | EntityVersionMismatchException | EntityNotFoundException e) {
      throw new StoreRuntimeException("Unable to retrieve existing datasets from cluster", e);
    }
  }

  @Override
  public <K extends Comparable<K>> boolean newDataset(String name, Type<K> keyType, DatasetConfiguration configuration) throws StoreException {
    if (isInvalidKeyType(keyType)) {
      throw new StoreRuntimeException("Keys of type " + keyType + " are not allowed");
    }

    DatasetEntityConfiguration<K> entityConfiguration = createEntityConfiguration(name, keyType, configuration);

    @SuppressWarnings("rawtypes")
    EntityRef<DatasetEntity, DatasetEntityConfiguration, DatasetEntity.Parameters> datasetEntityRef = getEntityRef(name);
    boolean created = createEntity(datasetEntityRef, entityConfiguration);

    if (!created) {
      getDataset(name, keyType, "Just created requested Dataset but now: ").close();
    }
    return created;
  }

  private boolean isInvalidKeyType(Type<?> keyType) {
    return Type.BYTES.equals(keyType);
  }

  private <K extends Comparable<K>> DatasetEntityConfiguration<K> createEntityConfiguration(String name, Type<K> keyType,
                                                                                         DatasetConfiguration configuration) {
    if (!(configuration instanceof ClusteredDatasetConfiguration)) {
      throw new IllegalArgumentException("The configuration passed in was not obtained through the API: " + configuration.getClass());
    }

    ClusteredDatasetConfiguration typedConfiguration = (ClusteredDatasetConfiguration) configuration;
    return new DatasetEntityConfiguration<>(keyType, name, typedConfiguration);
  }

  @Override
  public Map<String, Type<?>> listDatasets() throws StoreException {
    try {
      Map<String, Type<?>> result = new HashMap<>();
      Map<String, DatasetEntityConfiguration<?>> existingDatasets = getExistingDatasets();
      for (Map.Entry<String, DatasetEntityConfiguration<?>> entry : existingDatasets.entrySet()) {
        result.put(entry.getKey(), entry.getValue().getKeyType());
      }

      return Collections.unmodifiableMap(result);
    } catch (EntityNotFoundException | EntityVersionMismatchException | EntityNotProvidedException e) {
      throw new StoreException("Cannot list existing datasets", e);
    }
  }

  private Map<String, DatasetEntityConfiguration<?>> getExistingDatasets() throws EntityNotProvidedException, EntityVersionMismatchException, EntityNotFoundException {
    Map<String, DatasetEntityConfiguration<?>> datasets = new HashMap<>();
    EntityRef<SystemCatalog, Object, Object> entityRef = connection.getEntityRef(SystemCatalog.class, SystemCatalog.VERSION, SystemCatalog.ENTITY_NAME);
    try (SystemCatalog systemCatalog = entityRef.fetchEntity(null)) {
      Map<String, byte[]> list = systemCatalog.listByType(DatasetEntity.class);
      for (Map.Entry<String, byte[]> entry : list.entrySet()) {
        String name = entry.getKey();
        byte[] configAsBytes = entry.getValue();
        DatasetEntityConfiguration<?> datasetEntityConfiguration = ConfigurationEncoder.decode(configAsBytes);
        datasets.put(name, datasetEntityConfiguration);
      }
    }
    return datasets;
  }

  @Override
  public <K extends Comparable<K>> Dataset<K> getDataset(String name, Type<K> keyType) throws StoreException {
    return getDataset(name, keyType, "");
  }

  private <K extends Comparable<K>> Dataset<K> getDataset(String name, Type<K> keyType,
                                                          String extraError) throws StoreException {
    @SuppressWarnings("rawtypes")
    EntityRef<DatasetEntity, DatasetEntityConfiguration, DatasetEntity.Parameters> datasetEntityRef = getEntityRef(name);

    try {
      @SuppressWarnings("unchecked")
      DatasetEntity<K> datasetEntity = datasetEntityRef.fetchEntity(null);
      try {
        Type<K> entityKeyType = datasetEntity.getKeyType();
        if (!entityKeyType.equals(keyType)) {
          throw new DatasetKeyTypeMismatchException(extraError + "Wrong type for Dataset '" + name
              + "'; requested: " + keyType + ", found: " + entityKeyType);
        }

        return createClusteredDataset(datasetEntity);
      } catch (Throwable t) {
        try {
          datasetEntity.close();
        } catch (Throwable tt) {
          t.addSuppressed(tt);
        }
        throw t;
      }
    } catch (EntityVersionMismatchException e) {
      throw new StoreRuntimeException(e);
    } catch (EntityNotFoundException e) {
      throw new DatasetMissingException(extraError + "Dataset '" + name + "' not found");
    }
  }

  private <K extends Comparable<K>> Dataset<K> createClusteredDataset(DatasetEntity<K> datasetEntity) {
    return new ClusteredDataset<>(datasetEntity);
  }

  @Override
  public boolean destroyDataset(String name) throws StoreException {
    @SuppressWarnings("rawtypes")
    EntityRef<DatasetEntity, DatasetEntityConfiguration, DatasetEntity.Parameters> datasetEntityRef = getEntityRef(name);
    try {
      boolean success = datasetEntityRef.destroy();

      if (!success) {
        throw new StoreException("Unable to destroy Dataset as other clients were using it");
      }
      return true;
    } catch (EntityNotProvidedException e) {
      throw new StoreRuntimeException(e);
    } catch (EntityNotFoundException | PermanentEntityException e) {
      return false;
    }
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (IOException e) {
      throw new StoreRuntimeException(e);
    }
  }

  @Override
  public DatasetConfigurationBuilder datasetConfiguration() {
    return new ClusteredDatasetConfigurationBuilder();
  }

  @SuppressWarnings("rawtypes")
  private EntityRef<DatasetEntity, DatasetEntityConfiguration, DatasetEntity.Parameters> getEntityRef(String datasetName) {
    try {
      String entityName = datasetName;
      return connection.getEntityRef(DatasetEntity.class, DatasetEntityInfo.DATASET_ENTITY_VERSION, entityName);
    } catch (EntityNotProvidedException e) {
      throw new StoreRuntimeException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  private boolean createEntity(
      EntityRef<DatasetEntity, DatasetEntityConfiguration, DatasetEntity.Parameters> datasetEntityRef, DatasetEntityConfiguration configuration)
      throws StoreException {
    try {
      datasetEntityRef.create(configuration);
      return true;
    } catch (EntityNotProvidedException | EntityVersionMismatchException e) {
      throw new StoreRuntimeException(e);
    } catch (EntityAlreadyExistsException e) {
      return false;
    } catch (EntityConfigurationException e) {
      // TODO - get rid of this once https://github.com/Terracotta-OSS/terracotta-apis/issues/208 is resolved
      String message = FirstServerSideExceptionHelper.getMessage(e);
      throw new StoreException(message, e);
    }
  }
}
