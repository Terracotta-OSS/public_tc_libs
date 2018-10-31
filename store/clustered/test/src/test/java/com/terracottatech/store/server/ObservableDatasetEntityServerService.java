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

import com.terracottatech.store.server.messages.DatasetServerMessageCodec;
import com.terracottatech.store.server.messages.SyncDatasetCodec;
import com.terracottatech.store.server.stream.PipelineProcessor;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.CommonServerEntity;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.ServiceRegistry;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * Provides an alternative to {@link DatasetEntityServerService} for unit tests to enable observing
 * the state of the {@link DatasetActiveEntity}.
 */
public class ObservableDatasetEntityServerService
    implements EntityServerService<DatasetEntityMessage, DatasetEntityResponse> {

  private final DatasetEntityServerService delegate = new DatasetEntityServerService();

  private final List<DatasetActiveEntity<?>> servedActiveEntities = new ArrayList<>();
  private final List<DatasetPassiveEntity<?>> servedPassiveEntities = new ArrayList<>();

  /**
   * Gets a list of {@link ObservableDatasetActiveEntity} instances wrapping the
   * {@link DatasetActiveEntity} instances served by this {@link EntityServerService}.
   *
   * @return an unmodifiable list of {@link ObservableDatasetActiveEntity} instances
   */
  public List<ObservableDatasetActiveEntity<? extends Comparable<?>>> getServedActiveEntities() {
    return Collections.unmodifiableList(servedActiveEntities.stream()
        .map((Function<DatasetActiveEntity<?>, ? extends ObservableDatasetActiveEntity<?>>)ObservableDatasetActiveEntity::new)
        .collect(toList()));
  }

  /**
   * Gets a list of {@link ObservableDatasetPassiveEntity} instances wrapping the
   * {@link DatasetPassiveEntity} instances served by this {@link EntityServerService}.
   *
   * @return an unmodifiable list of {@link ObservableDatasetPassiveEntity} instances
   */
  public List<ObservableDatasetPassiveEntity> getServedPassiveEntities() {
    return Collections.unmodifiableList(servedPassiveEntities.stream()
        .map(ObservableDatasetPassiveEntity::new).collect(toList()));
  }

  @Override
  public long getVersion() {
    return delegate.getVersion();
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return delegate.handlesEntityType(typeName);
  }

  @Override
  public DatasetActiveEntity<?> createActiveEntity(ServiceRegistry registry, byte[] configurationBytes)
      throws ConfigurationException {
    DatasetActiveEntity<?> activeEntity = delegate.createActiveEntity(registry, configurationBytes);
    servedActiveEntities.add(activeEntity);
    return activeEntity;
  }

  @Override
  public DatasetPassiveEntity<?> createPassiveEntity(ServiceRegistry registry, byte[] configurationBytes) throws ConfigurationException {
    DatasetPassiveEntity<?> passiveEntity = delegate.createPassiveEntity(registry, configurationBytes);
    servedPassiveEntities.add(passiveEntity);
    return passiveEntity;
  }

  @Override
  public ConcurrencyStrategy<DatasetEntityMessage> getConcurrencyStrategy(byte[] configurationBytes) {
    return delegate.getConcurrencyStrategy(configurationBytes);
  }

  @Override
  public DatasetServerMessageCodec getMessageCodec() {
    return delegate.getMessageCodec();
  }

  @Override
  public SyncDatasetCodec getSyncMessageCodec() {
    return delegate.getSyncMessageCodec();
  }

  @Override
  public <AP extends CommonServerEntity<DatasetEntityMessage, DatasetEntityResponse>>
  AP reconfigureEntity(ServiceRegistry registry, AP oldEntity, byte[] configuration)
      throws ConfigurationException {
    return delegate.reconfigureEntity(registry, oldEntity, configuration);
  }

  @Override
  public ExecutionStrategy<DatasetEntityMessage> getExecutionStrategy(byte[] configuration) {
    return delegate.getExecutionStrategy(configuration);
  }

  public static final class ObservableDatasetActiveEntity<K extends Comparable<K>> {
    private final DatasetActiveEntity<K> activeEntity;

    public ObservableDatasetActiveEntity(DatasetActiveEntity<K> activeEntity) {
      this.activeEntity = activeEntity;
    }

    public DatasetActiveEntity<K> getActiveEntity() {
      return activeEntity;
    }

    public Collection<ClientDescriptor> getClientIds() {
      return activeEntity.getConnectedClients();
    }

    public Collection<PipelineProcessor> getOpenStreams(ClientDescriptor client) {
      return activeEntity.getOpenStreams(client);
    }
  }

  public static final class ObservableDatasetPassiveEntity {
    private final DatasetPassiveEntity<?> passiveEntity;

    public ObservableDatasetPassiveEntity(DatasetPassiveEntity<?> passiveEntity) {
      this.passiveEntity = passiveEntity;
    }

    public DatasetPassiveEntity<?> getPassiveEntity() {
      return passiveEntity;
    }
  }
}
