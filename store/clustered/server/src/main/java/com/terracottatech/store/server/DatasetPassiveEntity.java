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

import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.exceptions.ClientSideOnlyException;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetOperationMessage;
import com.terracottatech.store.common.messages.ErrorResponse;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.SuccessResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateAcceptedResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateMessage;
import com.terracottatech.store.common.messages.indexing.IndexDestroyMessage;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.server.concurrency.ConcurrencyShardMapper;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.messages.replication.DataReplicationMessage;
import com.terracottatech.store.server.messages.replication.MetadataReplicationMessage;
import com.terracottatech.store.server.management.DatasetManagement;
import com.terracottatech.store.server.messages.ServerServerMessage;
import com.terracottatech.store.server.messages.ServerServerMessageType;
import com.terracottatech.store.server.messages.replication.BatchedDataSyncMessage;
import com.terracottatech.store.server.messages.replication.MessageTrackerSyncMessage;
import com.terracottatech.store.server.messages.replication.MetadataSyncMessage;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;
import com.terracottatech.store.server.messages.replication.SyncBoundaryMessage;
import com.terracottatech.store.server.replication.DatasetReplicationListener;
import com.terracottatech.store.server.state.DatasetEntityStateService;
import com.terracottatech.store.server.state.DatasetEntityStateServiceConfig;
import com.terracottatech.store.server.sync.DatasetLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.client.message.tracker.OOOMessageHandlerConfiguration;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityUserException;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

import static com.terracottatech.store.server.concurrency.ConcurrencyShardMapper.concurrencyKeyToShardIndex;
import static com.terracottatech.store.server.indexing.IndexingUtilities.convertSettings;

public class DatasetPassiveEntity<K extends Comparable<K>> implements PassiveServerEntity<DatasetEntityMessage, DatasetEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetPassiveEntity.class);

  private final OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler;
  private final DatasetEntityStateService<K> datasetEntityStateService;

  private final DatasetManagement datasetManagement;

  private volatile RawDataset<K> dataset;
  private volatile DatasetLoader<K> loader = null;
  private volatile DatasetReplicationListener<K> replicator = null;

  private final ConcurrentMap<Long, List<UUID>> clientSourceIdToRequesterIdMapping = new ConcurrentHashMap<>();

  public static <K extends Comparable<K>> DatasetPassiveEntity<K> create(ServiceRegistry serviceRegistry, DatasetEntityConfiguration<K> entityConfiguration) throws ServiceException {
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(entityConfiguration);
    OOOMessageHandlerConfiguration<DatasetEntityMessage, DatasetEntityResponse> messageHandlerConfiguration =
            new OOOMessageHandlerConfiguration<>(entityConfiguration.getDatasetName(), new DatasetTrackerPolicy(),
                    concurrencyShardMapper.getShardCount() + 1, //The extra segment is to track all non-CRUD ops
                    new DatasetSegmentationStrategy(concurrencyShardMapper));
    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = serviceRegistry.getService(messageHandlerConfiguration);
    DatasetEntityStateService<K> stateService = serviceRegistry.getService(new DatasetEntityStateServiceConfig<>(entityConfiguration, serviceRegistry));
    DatasetManagement management = new DatasetManagement(serviceRegistry, entityConfiguration, false);

    return new DatasetPassiveEntity<>(stateService, management, messageHandler);
  }

  protected DatasetPassiveEntity(DatasetEntityStateService<K> stateService, DatasetManagement management,
                                 OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler) {
    this.datasetEntityStateService = stateService;
    this.datasetManagement = management;
    this.messageHandler = messageHandler;
  }

  @Override
  public void invokePassive(InvokeContext context, DatasetEntityMessage message) throws EntityUserException {
    BiFunction<InvokeContext, DatasetEntityMessage, DatasetEntityResponse> invokeFunction = (ctxt, msg) -> {
      try {
        if (msg instanceof ServerServerMessage) {
          ServerServerMessage serverMessage = (ServerServerMessage) msg;
          ServerServerMessageType type = serverMessage.getType();
          switch (type) {
            case BATCHED_SYNC_MESSAGE:
              invokeDatasyncMessage(serverMessage);
              break;
            case METADATA_SYNC_MESSAGE:
              invokeMetasyncMessage(serverMessage);
              break;
            case MESSAGE_TRACKER_SYNC_MESSAGE:
              loadMessageTrackerData(ctxt, (MessageTrackerSyncMessage) serverMessage);
              break;
            default:
              throw new UnsupportedOperationException("Unsupported message type: " + type);
          }
        } else if (msg instanceof ReplicationMessage) {
          ReplicationMessageType type = ((ReplicationMessage) msg).getType();
          switch (type) {
            case METADATA_REPLICATION_MESSAGE:
              this.replicator.handleReplicatedMetadata((MetadataReplicationMessage) msg);
              break;
            case DATA_REPLICATION_MESSAGE:
              this.replicator.handleReplicatedData((DataReplicationMessage) msg);
              break;
            case CRUD_DATA_REPLICATION_MESSAGE:
              return this.replicator.handleReplicatedData((CRUDDataReplicationMessage) msg);
            case SYNC_BOUNDARY_MESSAGE:
              replicator.startAcceptingCRUDReplicationForShard(((SyncBoundaryMessage)message).getShardIndex());
              break;
            default:
              throw new UnsupportedOperationException("Unsupported message type: " + type);
          }
        } else if (msg instanceof DatasetOperationMessage) {
          DatasetOperationMessageType type = ((DatasetOperationMessage) msg).getType();
          switch (type) {
            case INDEX_CREATE_MESSAGE:
              return indexCreate(ctxt, (IndexCreateMessage<?>) message);
            case INDEX_DESTROY_MESSAGE:
              indexDestroy((IndexDestroyMessage<?>) message);
              return new SuccessResponse();
            case EXECUTE_TERMINATED_PIPELINE_MESSAGE:
              return new ErrorResponse(new StoreRuntimeException("Mutative Stream execution was prematurely terminated due to an active server failover"));
            default:
              throw new UnsupportedOperationException("Unsupported replication message type: " + type);
          }
        } else {
          throw new UnsupportedOperationException("Unsupported Message encountered at Passive");
        }
      } catch (ClientSideOnlyException e) {
        return new ErrorResponse(e.getCause());
      }
      return null;
    };

    InvokeContext realContext = context;
    if (message instanceof CRUDDataReplicationMessage) {
      CRUDDataReplicationMessage dataReplicationMessage = (CRUDDataReplicationMessage) message;
      realContext = new InvokeContext() {

        @Override
        public ClientSourceId getClientSource() {
          return context.makeClientSourceId(dataReplicationMessage.getClientId());
        }

        @Override
        public long getCurrentTransactionId() {
          return dataReplicationMessage.getCurrentTransactionId();
        }

        @Override
        public long getOldestTransactionId() {
          return dataReplicationMessage.getOldestTransactionId();
        }

        @Override
        public boolean isValidClientInformation() {
          return true;
        }

        @Override
        public ClientSourceId makeClientSourceId(long l) {
          return context.makeClientSourceId(l);
        }

        @Override
        public int getConcurrencyKey() {
          return context.getConcurrencyKey();
        }
      };
    }
    this.messageHandler.invoke(realContext, message, invokeFunction);
  }

  @Override
  public void startSyncEntity() {
    if (loader != null) {
      throw new IllegalStateException("Loader is already initialized");
    }
    loader = new DatasetLoader<>(this.dataset);
    replicator.prepareForSync();
    LOGGER.info("Sync started for dataset : {}", this.dataset.getAlias());
  }

  @Override
  public void endSyncEntity() {
    LOGGER.info("Sync completed for dataset : {}", this.dataset.getAlias());
    startIndexingAfterSync(loader.getPendingIndexDescriptions());
    this.loader = null;
  }

  @Override
  public void startSyncConcurrencyKey(int concurrencyKey) {
    LOGGER.info("Sync started for shard {} of dataset : {}", concurrencyKeyToShardIndex(concurrencyKey), this.dataset.getAlias());
  }

  @Override
  public void endSyncConcurrencyKey(int concurrencyKey) {
    this.loader.endSync(concurrencyKey);
    LOGGER.info("Sync completed for shard {} of dataset : {}", concurrencyKeyToShardIndex(concurrencyKey), this.dataset.getAlias());
  }

  private void invokeDatasyncMessage(ServerServerMessage serverMessage) {
    BatchedDataSyncMessage batchedDataSyncMessage = (BatchedDataSyncMessage) serverMessage;
    this.loader.load(batchedDataSyncMessage);
  }

  private void invokeMetasyncMessage(ServerServerMessage serverMessage) {
    MetadataSyncMessage metadataSyncMessage = (MetadataSyncMessage) serverMessage;
    this.loader.loadMetaData(metadataSyncMessage);
  }

  private void loadMessageTrackerData(InvokeContext context, MessageTrackerSyncMessage syncMessage) {
    this.messageHandler.loadTrackedResponsesForSegment(syncMessage.getSegmentIndex(),
        context.makeClientSourceId(syncMessage.getClientId()), syncMessage.getTrackedResponses());
  }

  @Override
  public void createNew() throws ConfigurationException {
    this.datasetEntityStateService.createDataset(false);
    this.dataset = (RawDataset<K>) this.datasetEntityStateService.getDataset();
    this.replicator = new DatasetReplicationListener<>(dataset);
    datasetManagement.entityCreated(dataset);
  }

  @Override
  public void destroy() {
    this.datasetEntityStateService.destroy();
    this.datasetManagement.close();
  }

  private void startIndexingAfterSync(Map<CellDefinition<? extends Comparable<?>>, IndexSettings> indexDescriptions) {
    SovereignIndexing indexing = this.dataset.getIndexing();
    indexDescriptions.forEach((cellDefinition, settings) -> {
      try {
        LOGGER.info("Creating index on passive for {}[{}:{}]", dataset.getAlias(), settings, cellDefinition);
        SovereignIndex<?> sovereignIndex = getSovereignIndex(indexing, cellDefinition, settings);
        datasetManagement.indexCreated(sovereignIndex);
      } catch (Exception e) {
        LOGGER.error("Failed to create index for dataset {} on passive", this.dataset.getAlias());
        throw new StoreRuntimeException("Failed to create index for dataset " + this.dataset.getAlias() + " on passive");
      }
    });
  }

  private <T extends Comparable<T>> SovereignIndex<?> getSovereignIndex(
          SovereignIndexing indexing, CellDefinition<? extends Comparable<?>> definition, IndexSettings settings) throws Exception {
    @SuppressWarnings("unchecked")
    CellDefinition<T> key = (CellDefinition<T>) definition;
    Callable<SovereignIndex<T>> indexTask = indexing.createIndex(key, convertSettings(settings));
    return indexTask.call();
  }

  private <T extends Comparable<T>> DatasetEntityResponse indexCreate(InvokeContext invokeContext, IndexCreateMessage<T> indexCreateMessage) {
    clientSourceIdToRequesterIdMapping.compute(invokeContext.getClientSource().toLong(), (clientSourceId, uuids) -> {
      if (uuids == null) {
        uuids = new LinkedList<>();
      }
      uuids.add(indexCreateMessage.getStableClientId());
      return uuids;
    });
    if (this.loader != null) {
      loader.addIndex(indexCreateMessage.getCellDefinition(), indexCreateMessage.getIndexSettings());
      LOGGER.info("Ignoring Index Creation replication message while passive sync is going on for dataset {}", dataset.getAlias());
    } else {
      LOGGER.info("Creating index on passive for {}[{}:{}]", dataset.getAlias(), indexCreateMessage.getIndexSettings(),
              indexCreateMessage.getCellDefinition());
      this.datasetEntityStateService.indexCreate(indexCreateMessage.getStableClientId(), indexCreateMessage.getCellDefinition(),
              indexCreateMessage.getIndexSettings(), null, datasetManagement);
    }
    String indexRequestId = dataset.getAlias() + '[' + indexCreateMessage.getIndexSettings() + ':' + indexCreateMessage.getCellDefinition() + ']';
    return new IndexCreateAcceptedResponse(indexRequestId);
  }

  private <T extends Comparable<T>> void indexDestroy(IndexDestroyMessage<T> indexDestroyMessage) {
    if (this.loader != null) {
      loader.deleteIndex(indexDestroyMessage.getCellDefinition(), indexDestroyMessage.getIndexSettings());
      LOGGER.info("Ignoring Index Destroy replication message while passive sync is going on for dataset {}", dataset.getAlias());
    } else {
      SovereignIndexing indexing = dataset.getIndexing();
      SovereignIndex<T> index = indexing
              .getIndex(indexDestroyMessage.getCellDefinition(), convertSettings(indexDestroyMessage.getIndexSettings()));
      if (index == null) {
        throw new ClientSideOnlyException(new StoreIndexNotFoundException("Cannot destroy "
                + indexDestroyMessage.getIndexSettings() + ":" + indexDestroyMessage.getCellDefinition() + " index : does not exist"));
      } else {
        // TODO: Handle potentially long-running destruction
        LOGGER.info("Destroying index on passive for {}[{}:{}]", dataset.getAlias(), indexDestroyMessage.getIndexSettings(),
                indexDestroyMessage.getCellDefinition());
        indexing.destroyIndex(index);
        datasetManagement.indexDestroyed(index);
      }
    }
  }

  @Override
  public void notifyDestroyed(ClientSourceId sourceId) {
    clientSourceIdToRequesterIdMapping.computeIfPresent(sourceId.toLong(), (id, uuids) -> {
      uuids.forEach(datasetEntityStateService::disconnected);
      return null;
    });
    messageHandler.untrackClient(sourceId);
  }

  //Only for testing
  DatasetLoader<K> getDatasetLoader() {
    return this.loader;
  }
}
