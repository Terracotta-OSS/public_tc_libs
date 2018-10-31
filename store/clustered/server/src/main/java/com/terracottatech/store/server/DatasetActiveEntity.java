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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.exceptions.RecordLockedException;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.StoreStreamNotFoundException;
import com.terracottatech.store.StoreStreamRegisteredException;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.exceptions.ClientIdCollisionException;
import com.terracottatech.store.common.exceptions.ClientSideOnlyException;
import com.terracottatech.store.common.exceptions.RemoteStreamException;
import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.common.messages.BusyResponse;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetOperationMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.ErrorResponse;
import com.terracottatech.store.common.messages.IdentifyClientMessage;
import com.terracottatech.store.common.messages.SuccessResponse;
import com.terracottatech.store.common.messages.UniversalNopMessage;
import com.terracottatech.store.common.messages.crud.AddRecordFullResponse;
import com.terracottatech.store.common.messages.crud.AddRecordMessage;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.GetRecordMessage;
import com.terracottatech.store.common.messages.crud.GetRecordResponse;
import com.terracottatech.store.common.messages.crud.MutationMessage;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.event.ChangeEventResponse;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateAcceptedResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusResponse;
import com.terracottatech.store.common.messages.indexing.IndexDestroyMessage;
import com.terracottatech.store.common.messages.indexing.IndexListResponse;
import com.terracottatech.store.common.messages.indexing.IndexStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexStatusResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorRequestResultMessage;
import com.terracottatech.store.common.messages.stream.PipelineRequestProcessingResponse;
import com.terracottatech.store.common.messages.stream.RemoteStreamType;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.common.reconnect.ReconnectCodec;
import com.terracottatech.store.common.reconnect.ReconnectState;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.server.concurrency.ConcurrencyShardMapper;
import com.terracottatech.store.server.event.EventSender;
import com.terracottatech.store.server.execution.ExecutionService;
import com.terracottatech.store.server.indexing.IndexCreationRequest;
import com.terracottatech.store.server.management.DatasetManagement;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.replication.MessageTrackerSyncMessage;
import com.terracottatech.store.server.messages.replication.SyncBoundaryMessage;
import com.terracottatech.store.server.replication.DatasetReplicator;
import com.terracottatech.store.server.state.DatasetEntityStateService;
import com.terracottatech.store.server.state.DatasetEntityStateServiceConfig;
import com.terracottatech.store.server.stream.BatchedElementSource;
import com.terracottatech.store.server.stream.InlineElementSource;
import com.terracottatech.store.server.stream.PipelineProcessor;
import com.terracottatech.store.server.stream.PlanRenderer;
import com.terracottatech.store.server.stream.TerminatedPipelineProcessor;
import com.terracottatech.store.server.sync.DatasetSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.client.message.tracker.OOOMessageHandlerConfiguration;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityUserException;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.platform.ServerInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.terracottatech.store.common.messages.BusyResponse.Reason.EXEC;
import static com.terracottatech.store.common.messages.BusyResponse.Reason.KEY;
import static com.terracottatech.store.server.indexing.IndexingUtilities.convertSettings;
import static com.terracottatech.store.server.indexing.IndexingUtilities.convertStatus;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class DatasetActiveEntity<K extends Comparable<K>> implements ActiveServerEntity<DatasetEntityMessage, DatasetEntityResponse> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetActiveEntity.class);

  private final IEntityMessenger<EntityMessage, ?> entityMessenger;
  private final ServerInfo serverInfo;
  private final OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler;
  private final EventSender eventSender;
  private final Map<ClientDescriptor, ClientState> clientMap = new ConcurrentHashMap<>();
  private final DatasetEntityStateService<K> datasetEntityStateService;
  private final ExecutorService pipelineProcessorExecutor;
  private final DatasetManagement datasetManagement;
  private final ConcurrencyShardMapper concurrencyShardMapper;

  private volatile SovereignDataset<K> dataset;

  private final DatasetSynchronizer<K> datasetSynchronizer = new DatasetSynchronizer<>(() -> this.dataset);
  private volatile DatasetReplicator<K> datasetReplicator = null;

  public static <K extends Comparable<K>> DatasetActiveEntity<K> create(ServiceRegistry serviceRegistry, DatasetEntityConfiguration<K> entityConfiguration) throws ServiceException {
    ConcurrencyShardMapper shardMapper = new ConcurrencyShardMapper(entityConfiguration);
    ClientCommunicator clientCommunicator = serviceRegistry.getService(() -> ClientCommunicator.class);

    @SuppressWarnings("unchecked")
    IEntityMessenger<EntityMessage, ?> entityMessenger = serviceRegistry.getService(() -> IEntityMessenger.class);
    ServerInfo serverInfo = serviceRegistry.getService(() -> ServerInfo.class);
    if (serverInfo == null) {
      throw new RuntimeException("Failed to get ServerInfo from service registry");
    }

    OOOMessageHandlerConfiguration<DatasetEntityMessage, DatasetEntityResponse> messageHandlerConfiguration =
            new OOOMessageHandlerConfiguration<>(entityConfiguration.getDatasetName(), new DatasetTrackerPolicy(),
                    shardMapper.getShardCount() + 1, //The extra segment is to track all non-CRUD ops
                    new DatasetSegmentationStrategy(shardMapper));
    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = serviceRegistry.getService(messageHandlerConfiguration);

    @SuppressWarnings("unchecked")
    DatasetEntityStateService<K> datasetEntityStateService = serviceRegistry.getService(new DatasetEntityStateServiceConfig<>(entityConfiguration, serviceRegistry));

    ExecutionService executionService = serviceRegistry.getService(() -> ExecutionService.class);
    ExecutorService pipelineExecutor = executionService.getPipelineProcessorExecutor(entityConfiguration.getDatasetName());
    if (pipelineExecutor == null) {
      throw new RuntimeException("Error getting a pipeline processor executor for '" + entityConfiguration.getDatasetName() + "'");
    }

    DatasetManagement datasetManagement = new DatasetManagement(serviceRegistry, entityConfiguration, true);

    return new DatasetActiveEntity<>(clientCommunicator, entityMessenger, serverInfo, messageHandler, datasetEntityStateService, pipelineExecutor, datasetManagement, shardMapper);
  }

  protected DatasetActiveEntity(ClientCommunicator clientCommunicator, IEntityMessenger<EntityMessage, ?> entityMessenger, ServerInfo serverInfo,
                                OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler,
                                DatasetEntityStateService<K> stateService, ExecutorService pipelineExecutor,
                                DatasetManagement management, ConcurrencyShardMapper shardMapper) {
    this.entityMessenger = entityMessenger;
    this.serverInfo = serverInfo;
    this.messageHandler = messageHandler;
    this.eventSender = new EventSender(clientCommunicator);
    this.datasetEntityStateService = stateService;
    this.pipelineProcessorExecutor = pipelineExecutor;
    this.datasetManagement = management;
    this.concurrencyShardMapper = shardMapper;
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    if (!clientMap.containsKey(clientDescriptor)) {
      clientMap.put(clientDescriptor, new ClientState(clientDescriptor));
      LOGGER.info("Client " + clientDescriptor + " connected to dataset '" + this.dataset.getAlias() + "'");
      datasetManagement.clientConnected(clientDescriptor);
    } else {
      // This is logically an AssertionError
      LOGGER.error("Client {} already registered as connected to dataset '{}'", clientDescriptor, this.dataset.getAlias());
    }
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    eventSender.disconnected(clientDescriptor);

    clientMap.remove(clientDescriptor).disconnect();
  }

  @Override
  public void notifyDestroyed(ClientSourceId sourceId) {
    messageHandler.untrackClient(sourceId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public DatasetEntityResponse invokeActive(ActiveInvokeContext<DatasetEntityResponse> context, DatasetEntityMessage message) throws EntityUserException {
    BiFunction<InvokeContext, DatasetEntityMessage, DatasetEntityResponse> invokeFunction = (ctxt, msg) -> {
      ClientDescriptor clientDescriptor = context.getClientDescriptor();
      try {
        if (message instanceof DatasetOperationMessage) {
          DatasetOperationMessageType messageType = ((DatasetOperationMessage) message).getType();
          try {
            switch (messageType) {
              //LIFECYCLE
              case IDENTIFY_CLIENT_MESSAGE:
                return validateClientConnected(clientDescriptor).identify((IdentifyClientMessage) message);

              //CRUD
              case ADD_RECORD_MESSAGE:
                return addRecord(ctxt, (AddRecordMessage<K>) msg);
              case GET_RECORD_MESSAGE:
                return getRecord((GetRecordMessage<K>) msg);
              case PREDICATED_UPDATE_RECORD_MESSAGE:
                return predicatedUpdateRecord(ctxt, (PredicatedUpdateRecordMessage<K>) msg);
              case PREDICATED_DELETE_RECORD_MESSAGE:
                return predicatedDeleteRecord(ctxt, (PredicatedDeleteRecordMessage<K>) msg);

              //STREAMS
              case PIPELINE_PROCESSOR_OPEN_MESSAGE:
                return validateClientConnected(clientDescriptor).openPipelineProcessor((PipelineProcessorOpenMessage) msg);
              case PIPELINE_PROCESSOR_CLOSE_MESSAGE:
                return validateClientConnected(clientDescriptor).closePipelineProcessor((PipelineProcessorCloseMessage) msg);
              case TRY_ADVANCE_FETCH_MESSAGE:
              case TRY_ADVANCE_MUTATE_MESSAGE:
              case TRY_ADVANCE_RELEASE_MESSAGE:
              case BATCH_FETCH_MESSAGE:
              case PIPELINE_REQUEST_RESULT_MESSAGE:
                return validateClientConnected(clientDescriptor).onPipelineProcessor((PipelineProcessorMessage) msg);
              case EXECUTE_TERMINATED_PIPELINE_MESSAGE:
                return validateClientConnected(clientDescriptor).executeTerminatedPipeline((ExecuteTerminatedPipelineMessage) msg);

              //OTHERS
              case SEND_CHANGE_EVENTS_MESSAGE:
                return handleSendChangeEvents(clientDescriptor, (SendChangeEventsMessage) msg);
              case UNIVERSAL_NOP_MESSAGE:
                return new SuccessResponse();

              //INDEXING
              case INDEX_LIST_MESSAGE:
                return indexList();
              case INDEX_CREATE_MESSAGE:
                return indexCreate(clientDescriptor, (IndexCreateMessage<?>) msg);
              case INDEX_CREATE_STATUS_MESSAGE:
                return indexCreateStatus(clientDescriptor, (IndexCreateStatusMessage) msg);
              case INDEX_DESTROY_MESSAGE:
                return indexDestroy((IndexDestroyMessage<?>) msg);
              case INDEX_STATUS_MESSAGE:
                return indexStatus((IndexStatusMessage<?>) msg);
              default:
                LOGGER.error("Received unsupported message type from " + clientDescriptor + ": " + messageType);
                throw new UnsupportedOperationException("Unsupported message type: " + messageType);
            }
          } catch (ClientSideOnlyException e) {
            return new ErrorResponse(e.getCause());
          } catch (RejectedExecutionException e) {
            return new BusyResponse(EXEC);
          } catch (StoreRuntimeException | NoSuchElementException e) {
            LOGGER.error("Exception raised for {}: {}", messageType, e, e);
            return new ErrorResponse(e);
          }
        } else if (message.getType() == ReplicationMessageType.DATA_REPLICATION_MESSAGE) {
          return new SuccessResponse();
        } else {
          throw new UnsupportedOperationException("Unsupported message");
        }
      } catch (RuntimeException e) {
        LOGGER.error("Exception raised {}", e, e);
        return new ErrorResponse(e);
      }
    };

    recreateStateFromTrackedResponse(context, message);
    return messageHandler.invoke(context, message, invokeFunction);
  }

  private void recreateStateFromTrackedResponse(ActiveInvokeContext<?> context, DatasetEntityMessage message) {
    if (context.isValidClientInformation() && message instanceof MutationMessage) {
      @SuppressWarnings("unchecked")
      MutationMessage<K> mutationMessage = (MutationMessage<K>) message;
      K key = mutationMessage.getKey();
      int index = concurrencyShardMapper.getShardIndex(key);

      Map<Long, DatasetEntityResponse> trackedResponses = messageHandler.getTrackedResponsesForSegment(index, context.getClientSource());
      DatasetEntityResponse trackedResponse = trackedResponses.get(context.getCurrentTransactionId());

      if (trackedResponse != null && !(trackedResponse instanceof BusyResponse) && !(trackedResponse instanceof ErrorResponse)) {
        sendEvent(mutationMessage);
      }
    }
  }

  private DatasetEntityResponse handleSendChangeEvents(ClientDescriptor clientDescriptor, SendChangeEventsMessage message) {
    boolean sendChangeEvents = message.sendChangeEvents();
    eventSender.sendChangeEvents(clientDescriptor, sendChangeEvents);
    return new SuccessResponse();
  }

  private DatasetEntityResponse predicatedDeleteRecord(InvokeContext context, PredicatedDeleteRecordMessage<K> message) {
    return performMutation(context, message, () -> {
      SovereignRecord<K> deletedRecord =
          this.dataset.tryDelete(SovereignDataset.Durability.IMMEDIATE, message.getKey(), message.getPredicate());

      boolean deleted = deletedRecord != null;
      fireEvent(deleted, message);
      return createDeleteResponse(message, deleted, deletedRecord);
    });
  }

  private DatasetEntityResponse createDeleteResponse(PredicatedDeleteRecordMessage<K> message, boolean deleted, SovereignRecord<K> deletedRecord) {
    if (message.isRespondInFull()) {
      if (deleted) {
        return new PredicatedDeleteRecordFullResponse<>(deletedRecord.getKey(), getMSN(deletedRecord), deletedRecord);
      } else {
        return new PredicatedDeleteRecordFullResponse<>();
      }
    } else {
      return new PredicatedDeleteRecordSimplifiedResponse(deleted);
    }
  }

  private DatasetEntityResponse predicatedUpdateRecord(InvokeContext context, PredicatedUpdateRecordMessage<K> message) {
    return performMutation(context, message, () -> {
      K key = message.getKey();
      Optional<PredicatedUpdateResult> predicatedUpdateResultOpt = this.dataset.tryApplyMutation(
          SovereignDataset.Durability.IMMEDIATE,
          key,
          message.getPredicate(),
              r -> {
                @SuppressWarnings("unchecked")
                IntrinsicUpdateOperation<K> updateOperation = (IntrinsicUpdateOperation<K>) message.getUpdateOperation();
                return updateOperation.apply(r);
              },
          (oldRecord, newRecord) -> {
            if (newRecord == null) {
              return new PredicatedUpdateResult(null, null, null, null);
            }
            return new PredicatedUpdateResult(getMSN(oldRecord), oldRecord, getMSN(newRecord), newRecord);
          });

      boolean updated = predicatedUpdateResultOpt.isPresent() && predicatedUpdateResultOpt.get().applied();
      fireEvent(updated, message);
      return createUpdateResponse(message, key, updated, predicatedUpdateResultOpt);
    });
  }

  private DatasetEntityResponse createUpdateResponse(PredicatedUpdateRecordMessage<K> message, K key, boolean updated, Optional<PredicatedUpdateResult> possibleResult) {
    if (message.isRespondInFull()) {
      if (updated) {
        assert(possibleResult.isPresent());
        PredicatedUpdateResult result = possibleResult.get();
        return new PredicatedUpdateRecordFullResponse<>(key, result.getBeforeMsn(), result.getBeforeCells(), result.getAfterMsn(), result.getAfterCells());
      } else {
        return new PredicatedUpdateRecordFullResponse<K>();
      }
    } else {
      return new PredicatedUpdateRecordSimplifiedResponse(updated);
    }
  }

  private IndexListResponse indexList() {
    return new IndexListResponse(this.dataset.getIndexing().getIndexes().stream()
            .map(si -> new ImmutableIndex<>(si.on(), convertSettings(si.definition()), convertStatus(si.getState())))
            .collect(toList()));
  }

  /**
   * Initiates background creation of a Sovereign cell index.
   * The {@link IndexCreateAcceptedResponse} returned from this method is marked for
   * deferred presentation -- the client request does not complete until the original
   * message is retired and retirement of the original {@link IndexCreateMessage}
   * is delayed until the index creation is complete.  Once complete, the response is
   * released to the client and the client needs to use a {@link IndexCreateStatusMessage}
   * to obtain the index completion status.
   */
  private <T extends Comparable<T>>
  DatasetEntityResponse indexCreate(ClientDescriptor clientDescriptor, IndexCreateMessage<T> message) {
    ClientState clientState = validateClientConnected(clientDescriptor);
    UUID stableClientId = clientState.getStableClientId();
    if (!stableClientId.equals(message.getStableClientId())) {
      throw new IllegalStateException("Index creation request from " + clientDescriptor + " uses an unexpected stableClientId:"
          + " expected: " + stableClientId + "; found: " + message.getStableClientId() + " - " + message);
    }

    CellDefinition<T> cellDefinition = message.getCellDefinition();
    IndexSettings indexSettings = message.getIndexSettings();

    String indexRequestId = dataset.getAlias() + '[' + indexSettings + ':' + cellDefinition + ']';

    datasetEntityStateService.indexCreate(stableClientId,
            cellDefinition, indexSettings, () -> entityMessenger.deferRetirement(indexRequestId, message, new UniversalNopMessage()),
            datasetManagement);

    return new IndexCreateAcceptedResponse(indexRequestId);

  }

  private DatasetEntityResponse indexCreateStatus(ClientDescriptor clientDescriptor, IndexCreateStatusMessage message) {
    ClientState clientState = validateClientConnected(clientDescriptor);
    UUID stableClientId = clientState.getStableClientId();
    if (!stableClientId.equals(message.getStableClientId())) {
      throw new IllegalStateException("Index creation status request from " + clientDescriptor + " uses an unexpected stableClientId:"
          + " expected: " + stableClientId + "; found: " + message.getStableClientId() + " - " + message);
    }

    String creationRequestId = message.getCreationRequestId();
    IndexCreationRequest<?> creationRequest = datasetEntityStateService.removeClientIdxStateIfComplete(creationRequestId,
        stableClientId, () -> entityMessenger.deferRetirement(creationRequestId, message, new UniversalNopMessage()));
    if (creationRequest != null) {
      SovereignIndex<?> sovereignIndex = creationRequest.getSovereignIndex();
      Throwable fault = creationRequest.getFault();

      IndexCreateStatusResponse statusResponse;
      if (sovereignIndex != null) {
        statusResponse = new IndexCreateStatusResponse(convertStatus(sovereignIndex.getState()));
      } else if (fault != null) {
        statusResponse = new IndexCreateStatusResponse(fault);
      } else if (creationRequest.isRetry()) {
        statusResponse = new IndexCreateStatusResponse(true);
        return statusResponse;
      } else {
        throw new IllegalStateException("Index creation request " + creationRequestId + " not complete: " + creationRequest);
      }

      creationRequest.dereference(stableClientId);
      return statusResponse;

    } else {
      throw new IllegalStateException("Index creation request " + creationRequestId + " not pending");
    }
  }

  private <T extends Comparable<T>> DatasetEntityResponse indexDestroy(IndexDestroyMessage<T> message) {
    SovereignIndexing indexing = dataset.getIndexing();
    SovereignIndex<T> index = indexing
        .getIndex(message.getCellDefinition(), convertSettings(message.getIndexSettings()));
    if (index == null) {
      throw new ClientSideOnlyException(new StoreIndexNotFoundException("Cannot destroy "
              + message.getIndexSettings() + ":" + message.getCellDefinition() + " index : does not exist"));
    } else {
      // TODO: Handle potentially long-running destruction
      indexing.destroyIndex(index);
      datasetManagement.indexDestroyed(index);
      return new SuccessResponse();
    }
  }

  private <T extends Comparable<T>> DatasetEntityResponse indexStatus(IndexStatusMessage<T> message) {
    SovereignIndexing indexing = dataset.getIndexing();
    SovereignIndex<T> index = indexing.getIndex(message.getCellDefinition(), convertSettings(message.getIndexSettings()));
    Index.Status status;
    if (index != null) {
      status = convertStatus(index.getState());
    } else {
      status = Index.Status.DEAD;
    }
    return new IndexStatusResponse(status);
  }

  private long getMSN(Record<K> record) {
    return ((SovereignPersistentRecord) record).getMSN();
  }

  private DatasetEntityResponse addRecord(InvokeContext context, AddRecordMessage<K> message) {
    return performMutation(context, message, () -> {
      SovereignRecord<K> existingRecord =
          this.dataset.tryAdd(SovereignDataset.Durability.IMMEDIATE, message.getKey(), message.getCells());

      boolean added = existingRecord == null;
      fireEvent(added, message);
      return createAddResponse(message, added, existingRecord);
    });
  }

  private DatasetEntityResponse createAddResponse(AddRecordMessage<K> message, boolean added, SovereignRecord<K> existingRecord) {
    if (message.isRespondInFull()) {
      if (existingRecord == null) {
        return new AddRecordFullResponse<>();
      } else {
        return new AddRecordFullResponse<>(existingRecord.getKey(), getMSN(existingRecord), existingRecord);
      }
    } else {
      return new AddRecordSimplifiedResponse(added);
    }
  }

  private DatasetEntityResponse getRecord(GetRecordMessage<K> message) {
    K key = message.getKey();
    Predicate<? super Record<K>> predicate = message.getPredicate();

    try {
      Record<K> record = ofNullable(this.dataset.tryGet(key)).filter(predicate).orElse(null);
      if (record == null) {
        return new GetRecordResponse<>();
      } else {
        Long msn = getMSN(record);

        if (!key.equals(record.getKey())) {
          throw new AssertionError("Sovereign record does not match requested key: key:" + key + " sovereign key: " + record.getKey());
        }

        return new GetRecordResponse<>(key, msn, record);
      }
    } catch (RecordLockedException e) {
      // Unlikely, but the get could generate a busy
      return new BusyResponse(KEY);
    }
  }

  private DatasetEntityResponse performMutation(InvokeContext context, MutationMessage<K> message, LockableRecordFunction mutation) {
    try {
      datasetReplicator.registerReplicationInfo(context, message);
      try {
        return mutation.apply();
      } finally {
        datasetReplicator.clearSourceMessage();
      }

    } catch (RecordLockedException e) {
      // TODO: Implement local retry
      return new BusyResponse(KEY);
    }
  }

  private void fireEvent(boolean fireEvent, MutationMessage<K> message) {
    if (fireEvent) {
      sendEvent(message);
    }
  }

  private void sendEvent(MutationMessage<K> message) {
    K key = message.getKey();
    ChangeType changeType = message.getChangeType();
    eventSender.sendToAll(new ChangeEventResponse<>(key, changeType));
  }

  @Override
  public ReconnectHandler startReconnect() {
    return (clientDescriptor, extendedReconnectData) -> {
      ClientState clientState = validateClientConnected(clientDescriptor);
      ReconnectCodec codec = new ReconnectCodec();
      ReconnectState reconnectState = codec.decode(extendedReconnectData);

      boolean sendChangeEvents = reconnectState.sendChangeEvents();

      eventSender.sendChangeEvents(clientDescriptor, sendChangeEvents);

      UUID stableClientId = reconnectState.getStableClientId();
      clientState.setStableClientId(stableClientId);
    };
  }

  @Override
  public void prepareKeyForSynchronizeOnPassive(PassiveSynchronizationChannel<DatasetEntityMessage> syncChannel, int concurrencyKey) {
    int shardIndex = ConcurrencyShardMapper.concurrencyKeyToShardIndex(concurrencyKey);

    if (shardIndex == 0) {
      this.datasetSynchronizer.startSync(syncChannel);
    }

    if (shardIndex != concurrencyShardMapper.getShardCount()) {
      LOGGER.info("Prepare key : {} for sychronization to passive for dataset : {}", concurrencyKey, this.dataset.getAlias());
      this.datasetSynchronizer.synchronizeShard(syncChannel, shardIndex);
    }
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<DatasetEntityMessage> syncChannel, int concurrencyKey) {
    LOGGER.info("Syncing dataset : {} to passive on concurrency key {}", this.dataset.getAlias(), concurrencyKey);

    int shardIndex = ConcurrencyShardMapper.concurrencyKeyToShardIndex(concurrencyKey);
    if (shardIndex == concurrencyShardMapper.getShardCount()) {
      syncMessageTrackerDataToPassive(syncChannel, shardIndex); //Sync all the message tracker data for non-CRUD ops
    } else {
      this.datasetSynchronizer.synchronizeRecordedMutations(syncChannel, shardIndex);
      syncMessageTrackerDataToPassive(syncChannel, shardIndex);
    }

    LOGGER.debug("Send the Sync boundary for shard {} for dataset {}", shardIndex, getDataset().getAlias());
    try {
      entityMessenger.messageSelf(new SyncBoundaryMessage(shardIndex));
    } catch (MessageCodecException e) {
      LOGGER.error("Message codec failure ", e);
    }
  }

  private void syncMessageTrackerDataToPassive(PassiveSynchronizationChannel<DatasetEntityMessage> syncChannel, int shardIndex) {
    messageHandler.getTrackedClients().forEach(clientSourceId -> {
      Map<Long, DatasetEntityResponse> trackedResponses = this.messageHandler.getTrackedResponsesForSegment(shardIndex, clientSourceId);
      syncChannel.synchronizeToPassive(new MessageTrackerSyncMessage(clientSourceId.toLong(), trackedResponses, shardIndex));
    });
  }

  @Override
  public void createNew() throws ConfigurationException {
    this.datasetEntityStateService.createDataset(true);
    this.dataset = this.datasetEntityStateService.getDataset();
    this.datasetReplicator = new DatasetReplicator<>(this.dataset, entityMessenger);
    this.datasetReplicator.setChangeConsumer();
    datasetManagement.entityCreated(dataset);
  }

  @Override
  public void loadExisting() {
    this.datasetEntityStateService.loadExistingDataset();
    this.dataset = this.datasetEntityStateService.getDataset();
    this.datasetReplicator = new DatasetReplicator<>(this.dataset, entityMessenger);
    this.datasetReplicator.setChangeConsumer();
    datasetManagement.entityPromotionCompleted(dataset);
  }

  @Override
  public void destroy() {
    this.datasetEntityStateService.destroy();
    this.pipelineProcessorExecutor.shutdown();
    this.datasetManagement.close();
  }

  private ClientState validateClientConnected(ClientDescriptor clientDescriptor) {
    ClientState clientState = clientMap.get(clientDescriptor);
    if (clientState == null) {
      throw new StoreRuntimeException("Client " + clientDescriptor +" is not attached to dataset '" + this.dataset.getAlias() + "'");
    }
    return clientState;
  }

  /**
   * Gets a reference to the {@link SovereignDataset} menaged through this {@code DatasetActiveEntity}.
   * <p><b>This method is intended for unit test only.</b>
   * @return a reference to the {@code SovereignDataset} managed by this {@code DatasetActiveEntity}
   */
  SovereignDataset<K> getDataset() {
    return dataset;
  }

  /**
   * Gets the collection of {@link ClientDescriptor} instances representing connected clients.
   * <p><b>This method is intended for unit test only.</b>
   * @return an unmodifiable view of the connected client {@code ClientDescriptor}s
   */
  Collection<ClientDescriptor> getConnectedClients() {
    return Collections.unmodifiableCollection(clientMap.keySet());
  }

  /**
   * Gets a snapshot, as a {@code Map}, of the index dynamic creation requests pending for this dataset.  This
   * method takes a lock that interferes with the {@link #indexCreate(ClientDescriptor, IndexCreateMessage)}
   * method.
   * <p><b>This method is intended for unit test only.</b>
   * @return a snapshot of the pending index creation requests for this dataset
   */
  @SuppressWarnings("unchecked")
  Map<Map.Entry<CellDefinition<?>, IndexSettings>, Collection<UUID>> getPendingIndexRequests() {
    return Collections.unmodifiableMap(datasetEntityStateService.getPendingCreationRequests().values().stream()
        .collect(toMap(r -> (Map.Entry<CellDefinition<?>, IndexSettings>)(Object)r.getRequestKey(),
            r -> r.getRetirementHandles().keySet())));
  }

  /**
   * Gets a snapshot of the index dynamic creation requests pending for a client.
   * @param client the {@code ClientDescriptor} for the client to query
   * @return a snapshot of the pending index creation requests for {@code client}; if the specified client is
   *        not active, an empty collection is returned
   */
  @SuppressWarnings("unchecked")
  Collection<Map.Entry<CellDefinition<?>, IndexSettings>> getPendingIndexRequests(ClientDescriptor client) {
    ClientState clientState = clientMap.get(client);
    if (clientState == null) {
      return Collections.emptyList();
    }
    UUID idxReqId = clientState.getStableClientId();
    return datasetEntityStateService.getPendingCreationRequests().values().stream()
            .filter(indexCreationRequest -> indexCreationRequest.isPendingFor(idxReqId))
            .map(indexCreationRequest -> (Map.Entry<CellDefinition<?>, IndexSettings>)(Object)indexCreationRequest.getRequestKey())
            .collect(toList());
  }

  /**
   * Gets a collection of the open streams (each represented by a {@link PipelineProcessor}) for a given client.
   * <p><b>This method is intended for unit test only.</b>
   * @param clientDescriptor the {@code ClientDescriptor} for the client
   * @return a unmodifiable view of the open {@code PipelineProcessor} instances
   */
  Collection<PipelineProcessor> getOpenStreams(ClientDescriptor clientDescriptor) {
    ClientState clientState = clientMap.get(clientDescriptor);
    return ( clientState == null ? null : Collections.unmodifiableCollection(clientState.streamMap.values()) );
  }

  DatasetReplicator<K> getDatasetReplicator() {
    return datasetReplicator;
  }

  /**
   * Tracks the operational state of a single client's interactions with the dataset
   * managed by a {@link DatasetActiveEntity}.
   */
  private final class ClientState {
    private final ClientDescriptor client;
    private final Map<UUID, PipelineProcessor> streamMap = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicReference<UUID> stableClientId = new AtomicReference<>();

    private ClientState(ClientDescriptor client) {
      this.client = client;
    }

    @SuppressWarnings("unchecked")
    private <T extends PipelineProcessor> T registerPipelineProcessor(UUID streamId, Supplier<T> execution) {
      checkIfClosed(String.format("Stream source registration failed for %s: client operations are closed", client));
      return (T) streamMap.compute(streamId, (id, currentStream) -> {
        if (currentStream == null) {
          return execution.get();
        } else {
          throw new StoreStreamRegisteredException(String.format("Client %s attempting to open stream %s more than once", client, streamId));
        }
      });
    }

    /**
     * Gets the {@link PipelineProcessor} for the identified stream.
     * @param streamId the id of the stream for the {@code PipelineProcessor} to fetch
     * @param <P> the type of the pipeline processor
     * @return the {@code StreamExecution} defined for the stream
     * @throws StoreStreamNotFoundException if the client is closed or the stream is not registered or is closed
     * @throws ClassCastException if the stream execution is not of type {@code P}
     */
    @SuppressWarnings("unchecked")
    private <P extends PipelineProcessor> P getPipelineProcessor(UUID streamId) {
      checkIfClosed(String.format("Stream operation failed for %s: client operations are closed", client));
      PipelineProcessor stream = streamMap.get(streamId);
      if (stream == null) {
        throw new StoreStreamNotFoundException(
            String.format("Stream operation failed for %s: Stream[%s] not found", client, streamId));
      }
      return (P) stream;
    }

    private void checkIfClosed(String message) throws StoreRuntimeException {
      if (closed.get()) {
        throw new StoreRuntimeException(message);
      }
    }

    /**
     * Sets the stable client ID if, and only if, it has not already been set.
     * @param stableClientId the desired stable client ID
     */
    private void setStableClientId(UUID stableClientId) {
      this.stableClientId.compareAndSet(null, stableClientId);
    }

    private UUID getStableClientId() {
      return stableClientId.get();
    }

    public DatasetEntityResponse identify(IdentifyClientMessage message) {
      UUID requestedClientId = message.getRequestedClientId();

      /*
       * This code block is single-threaded through the Voltron MANAGEMENT concurrency key.
       */
      UUID assignedClientId = stableClientId.get();
      if (assignedClientId != null) {
        throw new IllegalStateException("Client " + client + " already has a stable client ID of "
            + assignedClientId + " assigned");
      }

      clientMap.values().stream().filter(cs -> requestedClientId.equals(cs.getStableClientId())).findAny().ifPresent(cs -> {
        throw new ClientIdCollisionException("Requested stable client ID " + requestedClientId
            + " is already assigned to " + cs.client);
      });

      if (stableClientId.compareAndSet(null, requestedClientId)) {
        return new SuccessResponse();
      } else {
        throw new IllegalStateException("Client " + client + " already has a stable client ID of "
            + stableClientId.get() + " assigned (a race?)");
      }
    }

    public void disconnect() {
      ofNullable(stableClientId.get()).ifPresent(id -> {
        datasetEntityStateService.disconnected(id);
      });

      if (closed.compareAndSet(false, true)) {
        Iterator<Map.Entry<UUID, PipelineProcessor>> entries = streamMap.entrySet().iterator();
        while (entries.hasNext()) {
          Map.Entry<UUID, PipelineProcessor> entry = entries.next();
          streamMap.computeIfPresent(entry.getKey(), (id, elementSource) -> {
            elementSource.close();
            return null;
          });
          entries.remove();
        }
      }
      datasetManagement.clientDisconnected(client);
      LOGGER.info("Client " + client + " disconnected from dataset '" + dataset.getAlias() + "'");

    }

    public DatasetEntityResponse openPipelineProcessor(PipelineProcessorOpenMessage message) {
      if (message.getStreamType() == null) {
        try {
          return openBatchedElementSource(message);
        } catch (IllegalArgumentException t) {
          return openInlineElementSource(message);
        }
      } else {
        switch (message.getStreamType()) {
          case INLINE:
            return openInlineElementSource(message);
          case BATCHED:
            return openBatchedElementSource(message);
          default:
            throw new RemoteStreamException("Unsupported stream execution type: " + message.getStreamType());
        }
      }
    }

    private DatasetEntityResponse closePipelineProcessor(PipelineProcessorCloseMessage message) {
      if (closed.get()) {
        return null;
      }
      PipelineProcessor elementSource = streamMap.remove(message.getStreamId());
      if (elementSource != null) {
        elementSource.close();
        datasetManagement.streamCompleted(elementSource);
      }
      return new PipelineProcessorCloseResponse(message.getStreamId(), ofNullable(elementSource)
          .flatMap(es -> es.getExplanation().map(e -> PlanRenderer.render(serverInfo, e))).orElse("Unexplained"));
    }

    private DatasetEntityResponse openInlineElementSource(PipelineProcessorOpenMessage message) {
      /*
       * Open a "remote" stream backed by the dataset with a portable pipeline sequence from the client.
       */
      UUID streamId = message.getStreamId();
      Supplier<InlineElementSource<K, Object>> elementSourceSupplier;
      if (message.getTerminalOperation() == null) {
        elementSourceSupplier = () -> new InlineElementSource<>(pipelineProcessorExecutor, entityMessenger, client,
            streamId, message.getElementType(), dataset, message.getPortableOperations(), message.getRequiresExplanation());
      } else {
        elementSourceSupplier = () -> new InlineElementSource<>(pipelineProcessorExecutor, entityMessenger, client,
            streamId, message.getElementType(), dataset, message.getPortableOperations(), message.getTerminalOperation(), message.getRequiresExplanation());
      }

      pipelineProcessorExecutor.execute(registerPipelineProcessor(streamId, elementSourceSupplier));
      return new PipelineProcessorOpenResponse(streamId, RemoteStreamType.INLINE);
    }

    private DatasetEntityResponse openBatchedElementSource(PipelineProcessorOpenMessage message) {
      /*
       * Open a "remote" stream backed by the dataset with a portable pipeline sequence from the client.
       */
      UUID streamId = message.getStreamId();
      if (message.getTerminalOperation() == null) {
        registerPipelineProcessor(streamId, () -> new BatchedElementSource<>(pipelineProcessorExecutor, entityMessenger, client,
            streamId, message.getElementType(), dataset, message.getPortableOperations(), message.getRequiresExplanation()));
        return new PipelineProcessorOpenResponse(streamId, RemoteStreamType.BATCHED);
      } else {
        throw new IllegalArgumentException("Batched element sources do not support terminated streams.");
      }
    }

    public DatasetEntityResponse onPipelineProcessor(PipelineProcessorMessage msg) {
      PipelineProcessor pipelineProcessor = getPipelineProcessor(msg.getStreamId());
      try {
        return pipelineProcessor.invoke(msg);
      } finally {
        if (pipelineProcessor.isComplete()) {
          datasetManagement.streamCompleted(streamMap.remove(msg.getStreamId()));
        }
      }
    }

    public DatasetEntityResponse executeTerminatedPipeline(ExecuteTerminatedPipelineMessage message) {
      UUID streamId = message.getStreamId();
      TerminatedPipelineProcessor<K> pipelineProcessor = registerPipelineProcessor(message.getStreamId(),
          () -> new TerminatedPipelineProcessor<>(
                  pipelineProcessorExecutor, entityMessenger, serverInfo,
                  client, streamId, dataset,
                  message.getIntermediateOperations(), message.getTerminalOperation(), message.getRequiresExplanation()));
      try {
        DatasetEntityResponse response = onPipelineProcessor(message);
        if (response instanceof PipelineRequestProcessingResponse) {
          if (pipelineProcessor.isComplete()) {
            //asynchronous execution beat us so we fetch the result now
            return pipelineProcessor.invoke(new PipelineProcessorRequestResultMessage(streamId));
          } else {
            //asynchronous execution, come back later please
            return response;
          }
        } else {
          //synchronous execution - close the stream
          pipelineProcessor.close();
          return response;
        }
      } catch (Throwable t) {
        pipelineProcessor.close();
        streamMap.remove(streamId);
        throw t;
      }
    }
  }

  private interface LockableRecordFunction {
    DatasetEntityResponse apply() throws RecordLockedException;
  }
}
