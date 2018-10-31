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
package com.terracottatech.store.server.management;

import com.tc.classloader.CommonComponent;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.server.stream.PipelineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.service.monitoring.EntityManagementRegistry;
import org.terracotta.management.service.monitoring.EntityManagementRegistryConfiguration;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.terracottatech.store.server.management.ServerNotification.DATASET_ENTITY_CREATED;
import static com.terracottatech.store.server.management.ServerNotification.DATASET_ENTITY_RELOADED;
import static com.terracottatech.store.server.management.ServerNotification.DATASET_INDEX_CREATED;
import static com.terracottatech.store.server.management.ServerNotification.DATASET_INDEX_DESTROYED;
import static java.util.Comparator.comparing;

@CommonComponent
public class DatasetManagement implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetManagement.class);

  private final EntityManagementRegistry managementRegistry;
  private DatasetEntityConfiguration<?> entityConfiguration;

  private final StreamTable streamStatistics = new StreamTable(Duration.ofHours(24), 0.01f, comparing(StreamStatistics::getServerTime));

  public DatasetManagement(ServiceRegistry serviceRegistry, DatasetEntityConfiguration<?> datasetEntityConfiguration, boolean active) throws ServiceException {
    this.entityConfiguration = datasetEntityConfiguration;
    // create an entity monitoring service that allows this entity to push some management information into voltron monitoring service
    managementRegistry = serviceRegistry.getService(new EntityManagementRegistryConfiguration(serviceRegistry, active));
    if (managementRegistry != null) {
      if (active) {
        managementRegistry.addManagementProvider(new DatasetClientStateSettingsManagementProvider());
      }
      managementRegistry.addManagementProvider(new DatasetSettingsManagementProvider());
      managementRegistry.addManagementProvider(new DatasetStatisticsManagementProvider());
      managementRegistry.addManagementProvider(new DatasetIndexStatisticsManagementProvider());
    }
  }

  public void streamCompleted(PipelineProcessor stream) {
    streamStatistics.increment(stream);
  }

  public void indexCreated(SovereignIndex<?> index) {
    DatasetIndexBinding binding = new DatasetIndexBinding(index, entityConfiguration);
    if (managementRegistry != null) {
      LOGGER.trace("[{}] indexCreated()", entityConfiguration.getDatasetName());
      managementRegistry.registerAndRefresh(binding)
          .thenRun(() -> managementRegistry.pushServerEntityNotification(binding, DATASET_INDEX_CREATED.name()));
    }
  }

  public void indexDestroyed(SovereignIndex<?> index) {
    DatasetIndexBinding binding = new DatasetIndexBinding(index, entityConfiguration);
    if (managementRegistry != null) {
      LOGGER.trace("[{}] indexDestroyed()", entityConfiguration.getDatasetName());
      managementRegistry.registerAndRefresh(binding)
          .thenRun(() -> managementRegistry.pushServerEntityNotification(binding, DATASET_INDEX_DESTROYED.name()));
    }
  }

  // the goal of the following code is to send the management metadata from the entity into the monitoring tre AFTER the entity creation
  public void entityCreated(SovereignDataset<?> dataset) {
    if (managementRegistry != null) {
      LOGGER.trace("[{}] entityCreated()", entityConfiguration.getDatasetName());
      managementRegistry.entityCreated();
      DatasetBinding binding = new DatasetBinding(dataset, streamStatistics, entityConfiguration);
      datasetRefresh(DATASET_ENTITY_CREATED, dataset, binding);
    }
  }

  public void entityPromotionCompleted(SovereignDataset<?> dataset) {
    if (managementRegistry != null) {
      LOGGER.trace("[{}] entityPromotionCompleted()", entityConfiguration.getDatasetName());
      managementRegistry.entityPromotionCompleted();
      DatasetBinding binding = new DatasetBinding(dataset, streamStatistics, entityConfiguration);
      datasetRefresh(DATASET_ENTITY_RELOADED, dataset, binding);
    }
  }

  private void datasetRefresh(ServerNotification notification, SovereignDataset<?> dataset, DatasetBinding binding) {
    Stream<CompletableFuture<Void>> futureOfDataset = Stream.of(managementRegistry.register(binding));
    Stream<CompletableFuture<Void>> futureOfIndexes = dataset.getIndexing().getIndexes()
        .stream()
        .map(index -> new DatasetIndexBinding(index, entityConfiguration))
        .map(managementRegistry::register);

    CompletableFuture<?>[] futures = Stream.concat(futureOfDataset, futureOfIndexes).toArray(CompletableFuture<?>[]::new);

    CompletableFuture.allOf(futures)
        .thenRun(managementRegistry::refresh)
        .thenRun(() -> managementRegistry.pushServerEntityNotification(binding, notification.name()));
  }

  @Override
  public void close() {
    if (managementRegistry != null) {
      LOGGER.trace("[{}] close()", entityConfiguration.getDatasetName());
      managementRegistry.close();
    }
  }

  public void clientConnected(ClientDescriptor clientDescriptor) {
    if (managementRegistry != null) {
      LOGGER.trace("[{}] clientConnected({})", entityConfiguration.getDatasetName(), clientDescriptor);
      managementRegistry.registerAndRefresh(new ClientStateBinding(clientDescriptor));
    }
  }


  public void clientDisconnected(ClientDescriptor clientDescriptor) {
    if (managementRegistry != null) {
      LOGGER.trace("[{}] clientDisconnected({})", entityConfiguration.getDatasetName(), clientDescriptor);
      managementRegistry.unregisterAndRefresh(new ClientStateBinding(clientDescriptor));
    }
  }
}
