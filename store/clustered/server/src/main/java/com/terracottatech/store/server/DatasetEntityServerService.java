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
package com.terracottatech.store.server;

import com.terracottatech.licensing.services.LicenseEnforcer;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.DatasetEntityInfo;
import com.terracottatech.store.common.messages.ConfigurationEncoder;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.server.concurrency.ConcurrencyShardMapper;
import com.terracottatech.store.server.concurrency.KeyConcurrencyStrategy;
import com.terracottatech.store.server.concurrency.MutationConcurrencyStrategy;
import com.terracottatech.store.server.concurrency.ReplicationConcurrencyStrategy;
import com.terracottatech.store.server.messages.DatasetServerMessageCodec;
import com.terracottatech.store.server.messages.SyncDatasetCodec;
import com.terracottatech.store.server.storage.configuration.StorageConfigurationFactory;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import com.terracottatech.store.server.storage.factory.StorageFactoryException;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;

public class DatasetEntityServerService implements EntityServerService<DatasetEntityMessage, DatasetEntityResponse> {

  @Override
  public long getVersion() {
    return DatasetEntityInfo.DATASET_ENTITY_VERSION;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return "com.terracottatech.store.client.DatasetEntity".equals(typeName);
  }

  @Override
  public DatasetActiveEntity<?> createActiveEntity(ServiceRegistry registry, byte[] configurationBytes) throws ConfigurationException {
    try {
      LicenseEnforcer.ensure(registry, "TCStore");
      DatasetEntityConfiguration<?> decoded = ConfigurationEncoder.decode(configurationBytes);
      DatasetEntityConfiguration<?> configuration = checkConfiguration(registry, decoded);
      return DatasetActiveEntity.create(registry, configuration);
    } catch (ConfigurationException e) {
      throw e;
    } catch (Throwable t) {
      throw new ConfigurationException(t.getMessage(), t);
    }
  }

  @Override
  public DatasetPassiveEntity<?> createPassiveEntity(ServiceRegistry registry, byte[] configurationBytes) throws ConfigurationException {
    try {
      LicenseEnforcer.ensure(registry, "TCStore");
      DatasetEntityConfiguration<?> decoded = ConfigurationEncoder.decode(configurationBytes);
      DatasetEntityConfiguration<?> configuration = checkConfiguration(registry, decoded);
      return DatasetPassiveEntity.create(registry, configuration);
    } catch (ConfigurationException e) {
      throw e;
    } catch (Throwable t) {
      throw new ConfigurationException(t.getMessage(), t);
    }
  }

  private static <K extends Comparable<K>> DatasetEntityConfiguration<K> checkConfiguration(ServiceRegistry registry, DatasetEntityConfiguration<K> configuration) throws ConfigurationException {
    StorageFactory storageFactory;
    try {
      storageFactory = registry.getService(() -> StorageFactory.class);
    } catch (ServiceException e) {
      throw new AssertionError("Failed to obtain the singleton StorageFactory instance", e);
    }

    if (storageFactory == null) {
      throw new AssertionError("A StorageFactory is not available, but there should always be one");
    }

    StorageConfiguration storageConfiguration = StorageConfigurationFactory.create(configuration);
    try {
      storageFactory.getStorage(storageConfiguration);
    } catch (StorageFactoryException e) {
      throw new ConfigurationException(e.getMessage(), e);
    }
    return configuration;
  }

  @Override
  public ConcurrencyStrategy<DatasetEntityMessage> getConcurrencyStrategy(byte[] configurationBytes) {
    DatasetEntityConfiguration<?> configuration = ConfigurationEncoder.decode(configurationBytes);
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(configuration);
    MutationConcurrencyStrategy<?> mutationConcurrencyStrategy = new MutationConcurrencyStrategy<>(concurrencyShardMapper);
    ReplicationConcurrencyStrategy replicationConcurrencyStrategy = new ReplicationConcurrencyStrategy(concurrencyShardMapper);
    return new KeyConcurrencyStrategy<>(mutationConcurrencyStrategy, replicationConcurrencyStrategy);
  }

  @Override
  public ExecutionStrategy<DatasetEntityMessage> getExecutionStrategy(byte[] configuration) {
    return new DatasetExecutionStrategy();
  }

  @Override
  public DatasetServerMessageCodec getMessageCodec() {
    IntrinsicCodec intrinsicCodec = new IntrinsicCodec(ServerIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS);
    return new DatasetServerMessageCodec(intrinsicCodec);
  }

  @Override
  public SyncDatasetCodec getSyncMessageCodec() {
    return new SyncDatasetCodec(new IntrinsicCodec(ServerIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS));
  }
}
