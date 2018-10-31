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
package com.terracottatech.store.server.state;

import com.tc.classloader.CommonComponent;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.exceptions.ClientSideOnlyException;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.server.RawDataset;
import com.terracottatech.store.server.execution.ExecutionService;
import com.terracottatech.store.server.indexing.IndexCreationRequest;
import com.terracottatech.store.server.management.DatasetManagement;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfigurationFactory;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import com.terracottatech.store.server.storage.factory.StorageFactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ExplicitRetirementHandle;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.StateDumpCollector;
import org.terracotta.entity.StateDumpable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.terracottatech.store.server.indexing.IndexingUtilities.convertSettings;

/**
 * Holds the state for DatasetPassiveEntity, which gets transferred
 * to new DatasetActiveEntity after fail-over
 */
@CommonComponent
public class DatasetEntityStateService<K extends Comparable<K>> implements StateDumpable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetEntityStateService.class);

  private final Lock creationRequestLock = new ReentrantLock();
  private final Map<String, IndexCreationRequest<?>> pendingCreationRequests = new ConcurrentHashMap<>();
  private final ConcurrentMap<UUID, Map<String, IndexCreationRequest<?>>> clientIdxState = new ConcurrentHashMap<>();

  private final DatasetEntityConfiguration<K> entityConfiguration;
  private final StorageFactory storageFactory;
  private final StorageConfiguration storageConfiguration;
  private final DestroyCallback callback;
  private final ExecutorService indexingExecutor;

  private volatile SovereignDataset<K> dataset;
  private volatile boolean wasPassive = false;
  private volatile Map<String, IndexCreationRequest<?>> pendingCreationRequestsFromFailover;

  public static <K extends Comparable<K>> DatasetEntityStateService<K> create(DatasetEntityConfiguration<K> entityConfiguration, ServiceRegistry serviceRegistry, DestroyCallback callback) throws ServiceException {
    StorageFactory storageFactory = serviceRegistry.getService(() -> StorageFactory.class);
    ExecutorService indexingExecutor = getUnorderedExecutor(entityConfiguration.getDatasetName(), serviceRegistry);
    return new DatasetEntityStateService<>(entityConfiguration, callback, storageFactory, indexingExecutor);
  }

  public DatasetEntityStateService(DatasetEntityConfiguration<K> entityConfiguration, DestroyCallback callback, StorageFactory storageFactory, ExecutorService indexingExecutor) throws ServiceException {
    this.storageFactory = storageFactory;
    this.entityConfiguration = entityConfiguration;
    this.callback = callback;
    this.indexingExecutor = indexingExecutor;
    this.storageConfiguration = StorageConfigurationFactory.create(entityConfiguration);
  }

  @Override
  public void addStateTo(StateDumpCollector serviceDump) {
    {
      StateDumpCollector configurationDump = serviceDump.subStateDumpCollector("configuration");
      configurationDump.addState("datasetName", entityConfiguration.getDatasetName());
      configurationDump.addState("keyType", String.valueOf(entityConfiguration.getKeyType()));
      configurationDump.addState("offheapResource", String.valueOf(entityConfiguration.getDatasetConfiguration().getOffheapResource()));
      entityConfiguration.getDatasetConfiguration().getDiskResource().ifPresent(val -> configurationDump.addState("diskResource", val));
      entityConfiguration.getDatasetConfiguration().getConcurrencyHint().ifPresent(val -> configurationDump.addState("concurrencyHint", String.valueOf(val)));
      dump(configurationDump, "cells", entityConfiguration.getDatasetConfiguration().getIndexes().entrySet(), (cellDump, cellEntry) -> {
        CellDefinition<?> definition = cellEntry.getKey();
        cellDump.addState("name", definition.name());
        cellDump.addState("type", String.valueOf(definition.type()));
        cellDump.addState("indexSettings", String.valueOf(cellEntry.getValue()));
      });
    }
    {
      SovereignDatasetImpl<?> dataset = (SovereignDatasetImpl<?>) getDataset();
      StateDumpCollector datasetDump = serviceDump.subStateDumpCollector("dataset");
      datasetDump.addState("alias", String.valueOf(dataset.getAlias()));
      datasetDump.addState("uuid", String.valueOf(dataset.getUUID()));
      datasetDump.addState("pullStats", dataset.getStatsDump());
    }
  }

  private static <T> void dump(StateDumpCollector rootDump, String key, Collection<T> list, BiConsumer<StateDumpCollector, T> consumer) {
    StateDumpCollector collectionDump = rootDump.subStateDumpCollector(key);
    int idx = 0;
    for (T item : list) {
      StateDumpCollector itemDump = collectionDump.subStateDumpCollector(String.valueOf(idx++));
      consumer.accept(itemDump, item);
    }
  }

  public SovereignDataset<K> getDataset() {
    if (this.dataset == null) {
      throw new IllegalStateException("Dataset cannot be null");
    }
    return this.dataset;
  }

  public void createDataset(boolean isActive) throws ConfigurationException {

    String datasetName = entityConfiguration.getDatasetName();
    Type<K> keyType = entityConfiguration.getKeyType();

    LOGGER.info("Creating dataset {} with key type {}", datasetName, keyType);

    ClusteredDatasetConfiguration datasetConfiguration = entityConfiguration.getDatasetConfiguration();
    String offheapResource = datasetConfiguration.getOffheapResource();

    SovereignStorage<?, ?> storage = getStorage();

    SovereignBuilder<K, SystemTimeReference> sovereignBuilder = new SovereignBuilder<>(keyType, SystemTimeReference.class)
        .alias(datasetName)
        .storage(storage)
        .offheapResourceName(offheapResource);
    datasetConfiguration.getConcurrencyHint().ifPresent(sovereignBuilder::concurrency);
    datasetConfiguration.getDiskDurability()
      .ifPresent((d) -> sovereignBuilder.diskDurability(SovereignDatasetDiskDurability.convert(d)));

    SovereignDataset<K> sovereignDataset;
    try {
      sovereignDataset = sovereignBuilder.build();
    } catch (RuntimeException e) {
      // The presumption for the moment is that dataset creation failures are due either to configuration or
      // environmental irregularities.  They're all recast as a ConfigurationException here to avoid crashing
      // the Voltron server ...
      ConfigurationException fault = new ConfigurationException("Unable to create dataset '" + datasetName + "': " + e, e);
      LOGGER.error("Failed to create dataset '{}'", datasetName, fault);
      throw fault;
    }

    try {
      for (Map.Entry<CellDefinition<?>, IndexSettings> index : datasetConfiguration.getIndexes().entrySet()) {
        createPredefinedIndex(sovereignDataset, index.getKey(), index.getValue());
      }
    } catch (Throwable t) {
      try {
        storage.destroyDataSet(sovereignDataset.getUUID());
      } catch (IOException e) {
        LOGGER.error("Failed to destroy dataset '{}' (UUID: {}) after indexing failed", datasetName, sovereignDataset.getUUID(), e);
      }
      if (t instanceof ConfigurationException) {
        // Voltron understands Configuration exceptions and returns those without killing the server
        throw t;
      }
      throw new RuntimeException(t);
    }

    if (isActive) {
      dataset = sovereignDataset;
    } else {
      dataset = new RawDataset<>(sovereignDataset);
    }
  }

  private <T extends Comparable<T>>
  void createPredefinedIndex(SovereignDataset<?> dataset, CellDefinition<?> cellDefinition, IndexSettings indexDefinition)
      throws ConfigurationException {
    try {
      @SuppressWarnings("unchecked") CellDefinition<T> definition = (CellDefinition<T>)cellDefinition;
      dataset.getIndexing().createIndex(definition, convertSettings(indexDefinition)).call();
    } catch (Exception e) {
      ConfigurationException fault = new ConfigurationException("Failed to create index for '" + dataset.getAlias()
              + "' [" + indexDefinition + ':' + cellDefinition + ']', e);
      LOGGER.error(fault.getMessage(), fault);
      throw fault;
    }
  }

  private SovereignStorage<?, ?> getStorage() {
    try {
      return storageFactory.getStorage(storageConfiguration);
    } catch (StorageFactoryException e) {
      LOGGER.error("Entity storage failure", e);
      throw new RuntimeException(e);
    }
  }

  private static ExecutorService getUnorderedExecutor(String datasetName, ServiceRegistry serviceRegistry) {
    ExecutionService executionService = getExecutionService(serviceRegistry);
    ExecutorService unorderedExecutor = executionService.getUnorderedExecutor("Indexing:" + datasetName);
    if (unorderedExecutor == null) {
      throw new RuntimeException("Error getting ElementSource executor for 'Indexing:" + datasetName + "'");
    }
    return unorderedExecutor;
  }

  private static ExecutionService getExecutionService(ServiceRegistry serviceRegistry) {
    ExecutionService executionService;
    try {
      executionService = serviceRegistry.getService(() -> ExecutionService.class);
    } catch (ServiceException e) {
      throw new AssertionError("Failed to obtain the singleton ExecutionService instance", e);
    }
    return executionService;
  }

  public void loadExistingDataset() {

    if (dataset instanceof RawDataset) {
      LOGGER.info("Promoting passive dataset to active");
      this.dataset = ((RawDataset<K>) this.dataset).promoteToActive();
      this.wasPassive = true;
      creationRequestLock.lock();
      try {
        pendingCreationRequestsFromFailover = new ConcurrentHashMap<>(pendingCreationRequests);
        LOGGER.info("The requests paending after failover {}", pendingCreationRequestsFromFailover);
      } finally {
        creationRequestLock.unlock();
      }
      return;
    }

    String targetAlias = entityConfiguration.getDatasetName();
    Type<?> targetType = entityConfiguration.getKeyType();

    SovereignStorage<?, ?> storage = getStorage();

    for (SovereignDataset<?> sovereignDataset : storage.getManagedDatasets()) {
      String datasetAlias = sovereignDataset.getAlias();
      Type<?> datasetType = sovereignDataset.getType();

      if (targetAlias.equals(datasetAlias)) {
        if (targetType.equals(datasetType)) {
          @SuppressWarnings("unchecked")
          SovereignDataset<K> kSovereignDataset = (SovereignDataset<K>) sovereignDataset;
          dataset = kSovereignDataset;
          return;
        }

        throw new RuntimeException("Sovereign dataset with alias: " + datasetAlias + " has type: " + datasetType + " but expected: " + targetType);
      }
    }

    if (!entityConfiguration.getDatasetConfiguration().getDiskResource().isPresent()) {
      // Create transient dataset
      try {
        createDataset(true);
        return;
      } catch (ConfigurationException e) {
        throw new RuntimeException("Creation of transient dataset with key type: " + targetType + " and alias: " + targetAlias + " failed", e);
      }
    }

    throw new RuntimeException("Unable to find sovereign dataset with key type: " + targetType + " and alias: " + targetAlias);
  }

  public void destroy() {
    SovereignStorage<?, ?> storage = getStorage();

    LOGGER.info("Destroying dataset {} with key type {}", dataset.getAlias(), dataset.getType());

    // TODO: Address completeness -- TDB-1477
    UUID uuid = dataset.getUUID();
    try {
      storage.destroyDataSet(uuid);

      if (storage.getManagedDatasets().isEmpty()) {
        storageFactory.shutdownStorage(storageConfiguration, storage);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.callback.destroy();
  }

  @CommonComponent
  public interface DestroyCallback {
    void destroy();
  }

  public <T extends Comparable<T>> void indexCreate(UUID stableClientId, CellDefinition<T> cellDefinition,
                                                    IndexSettings indexSettings, Supplier<ExplicitRetirementHandle<?>> supplier,
                                                    DatasetManagement datasetManagement) {

    SovereignIndexing indexing = dataset.getIndexing();

    Map.Entry<CellDefinition<T>, IndexSettings> requestKey =
            new AbstractMap.SimpleImmutableEntry<>(cellDefinition, indexSettings);
    String indexRequestId = dataset.getAlias() + '[' + indexSettings + ':' + cellDefinition + ']';

    creationRequestLock.lock();
    try {
      @SuppressWarnings("unchecked")
      IndexCreationRequest<T> pendingCreationRequest = (IndexCreationRequest<T>) pendingCreationRequests.get(indexRequestId);
      if (pendingCreationRequest == null) {
        Callable<SovereignIndex<T>> indexCallable =
                indexing.createIndex(cellDefinition, convertSettings(indexSettings));
        pendingCreationRequest = new IndexCreationRequest<>(requestKey, dataset.getAlias());
        pendingCreationRequests.put(indexRequestId, pendingCreationRequest);

        IndexCreationRequest<T> creationRequest = pendingCreationRequest;
        indexingExecutor.execute(() -> {
          LOGGER.info("Beginning index creation for '{}'", indexRequestId);
          try {
            SovereignIndex<T> sovereignIndex = indexCallable.call();
            datasetManagement.indexCreated(sovereignIndex);
            creationRequest.complete(sovereignIndex);
            LOGGER.info("Completed index creation for '{}': state={}", indexRequestId, sovereignIndex.getState());

          } catch (Exception e) {
            creationRequest.complete(e);
            LOGGER.info("Failed index creation for '{}'", indexRequestId, e);

          } finally {
            /*
             * The request is done -- no one else can "join".
             */
            creationRequestLock.lock();
            try {
              pendingCreationRequests.remove(indexRequestId);
              if (pendingCreationRequestsFromFailover != null) {
                pendingCreationRequestsFromFailover.remove(indexRequestId);
              }
            } finally {
              creationRequestLock.unlock();
            }
            creationRequest.release();
          }
        });

      } else {
        if (pendingCreationRequest.isPendingFor(stableClientId)) {
          throw new IllegalStateException("Index creation request for '{"
                  + indexRequestId + "}' already pending for {" + stableClientId + "}");
        }
      }

      if (supplier != null) {
        pendingCreationRequest.reference(stableClientId, supplier.get());
      }

      final IndexCreationRequest<?> creationRequest = pendingCreationRequest;
      clientIdxState.compute(stableClientId, (uuid, stringIndexCreationRequestMap) -> {
        if (stringIndexCreationRequestMap == null) {
          stringIndexCreationRequestMap = new HashMap<>();
        }
        stringIndexCreationRequestMap.put(indexRequestId, creationRequest);
        return stringIndexCreationRequestMap;
      });
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new ClientSideOnlyException(e);
    } finally {
      creationRequestLock.unlock();
    }
  }

  public Map<String, IndexCreationRequest<?>> getPendingCreationRequests() {
    return pendingCreationRequests;
  }

  public IndexCreationRequest<?> removeClientIdxStateIfComplete(String indexRequestId, UUID stableClientId,
                                                                Supplier<ExplicitRetirementHandle<?>> supplierWhenNotComplete) {
    if (wasPassive && pendingCreationRequestsFromFailover.containsKey(indexRequestId)) {
      creationRequestLock.lock();
      try {
        IndexCreationRequest<?> creationRequest = pendingCreationRequests.get(indexRequestId);
        if(creationRequest != null) {
          creationRequest.reference(stableClientId, supplierWhenNotComplete.get());
          creationRequest.setRetry();
        }
        return creationRequest;
      } finally {
        creationRequestLock.unlock();
      }
    }
    final AtomicReference<IndexCreationRequest<?>> ref = new AtomicReference<>();
    clientIdxState.computeIfPresent(stableClientId, (uuid, stringIndexCreationRequestMap) -> {
      ref.set(stringIndexCreationRequestMap.remove(indexRequestId));
      if(stringIndexCreationRequestMap.isEmpty()) {
        stringIndexCreationRequestMap = null;
      }
      return stringIndexCreationRequestMap;
    });
    return ref.get();
  }

  public void disconnected(UUID stableClientId) {
    clientIdxState.remove(stableClientId);
    clearState(pendingCreationRequests, stableClientId);
    clearState(pendingCreationRequestsFromFailover, stableClientId);
  }

  private static void clearState(Map<String, IndexCreationRequest<?>> pendingCreationRequests, UUID stableClientId) {
    if (pendingCreationRequests != null) {
      pendingCreationRequests.values().forEach(indexCreationRequest -> {
        try {
          ExplicitRetirementHandle<?> retirementHandle = indexCreationRequest.dereference(stableClientId);
          if (retirementHandle != null) {
            retirementHandle.release();
          }
        } catch (MessageCodecException e) {
          LOGGER.error("Message Codec Failure", e);
        }
      });
    }
  }

}
