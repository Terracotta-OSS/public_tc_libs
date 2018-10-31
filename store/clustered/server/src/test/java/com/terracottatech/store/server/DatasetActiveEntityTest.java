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
import com.terracottatech.sovereign.exceptions.RecordLockedException;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.StoreStreamNotFoundException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.exceptions.ClientIdCollisionException;
import com.terracottatech.store.common.messages.BusyResponse;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetOperationMessageCodec;
import com.terracottatech.store.common.messages.ErrorResponse;
import com.terracottatech.store.common.messages.IdentifyClientMessage;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.common.messages.SuccessResponse;
import com.terracottatech.store.common.messages.crud.AddRecordFullResponse;
import com.terracottatech.store.common.messages.crud.AddRecordMessage;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.GetRecordMessage;
import com.terracottatech.store.common.messages.crud.GetRecordResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordFullResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateAcceptedResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusResponse;
import com.terracottatech.store.common.messages.indexing.IndexDestroyMessage;
import com.terracottatech.store.common.messages.indexing.IndexStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexStatusResponse;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.intrinsics.impl.InstallOperation;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorRequestResultMessage;
import com.terracottatech.store.common.messages.stream.PipelineRequestProcessingResponse;
import com.terracottatech.store.common.messages.stream.RemoteStreamType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseResponse;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchApplyResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchConsumedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchExhaustedResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchWaypointResponse;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceMutateMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseResponse;
import com.terracottatech.store.common.messages.stream.inline.WaypointMarker;
import com.terracottatech.store.common.reconnect.ReconnectCodec;
import com.terracottatech.store.common.reconnect.ReconnectState;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.ComparableCellDefinition;
import com.terracottatech.store.function.BuildableToLongFunction;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.server.execution.ExecutionService;
import com.terracottatech.store.server.execution.ExecutionServiceImpl;
import com.terracottatech.store.server.execution.ExecutionServiceProvider;
import com.terracottatech.store.server.messages.replication.MessageTrackerSyncMessage;
import com.terracottatech.store.server.messages.replication.SyncBoundaryMessage;
import com.terracottatech.store.server.state.DatasetEntityStateService;
import com.terracottatech.store.server.storage.configuration.PersistentStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.MemoryStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageType;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import com.terracottatech.store.server.stream.InlineElementSource;
import com.terracottatech.store.server.stream.InlineElementSourceAccessor;
import com.terracottatech.store.server.stream.PipelineProcessor;
import com.terracottatech.test.data.Animals;
import com.terracottatech.test.data.Animals.AnimalRecord;
import com.terracottatech.tool.Diagnostics;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityUserException;
import org.terracotta.entity.ExplicitRetirementHandle;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ReconnectRejectedException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.platform.ServerInfo;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DELETE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MUTATE_THEN_INTERNAL;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.MUTATE;
import static com.terracottatech.store.common.messages.DatasetEntityResponseType.BUSY_RESPONSE;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.intrinsics.impl.AlwaysTrue.alwaysTrue;
import static com.terracottatech.store.server.messages.ServerServerMessageType.MESSAGE_TRACKER_SYNC_MESSAGE;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class DatasetActiveEntityTest {

  private MyStorageFactory storageFactory = new MyStorageFactory();
  private MyServiceRegistry serviceRegistry;
  private RecordingClientCommunicator clientCommunicator;
  private AtomicLong txnIdGenerator = new AtomicLong(1);
  private ClientDescriptor clientDescriptor = new NamedClientDescriptor("TestClient{}", 1);
  private ClientDescriptor altClientDescriptor = new NamedClientDescriptor("AltTestClient{}", 2);
  private ClientDescriptor thirdClientDescriptor = new NamedClientDescriptor("ThirdTestClient{}", 3);
  private final UUID stableClientId = UUID.randomUUID();

  @Mock
  private ServiceProviderConfiguration serviceProviderConfiguration;

  @Mock
  private PlatformConfiguration platformConfiguration;

  @SuppressWarnings("rawtypes")
  @Mock
  private IEntityMessenger iEntityMessenger;

  private ExecutionService executionService;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);

    serviceRegistry = new MyServiceRegistry();
    clientCommunicator = new RecordingClientCommunicator();
    ExecutionServiceProvider executionServiceProvider = new ExecutionServiceProvider();
    executionServiceProvider.initialize(serviceProviderConfiguration, platformConfiguration);
    executionService = executionServiceProvider.getService(0L, () -> ExecutionService.class);
    serviceRegistry.addService(StorageFactory.class, storageFactory);
    serviceRegistry.addService(ClientCommunicator.class, clientCommunicator);
    serviceRegistry.addService(ExecutionService.class, executionService);
    serviceRegistry.addService(IEntityMessenger.class, iEntityMessenger);
    serviceRegistry.addService(OOOMessageHandler.class, new RecordingMessageHandler());
    serviceRegistry.addService(ServerInfo.class, new ServerInfo("serverName"));

    doAnswer(invocationOnMock -> {
      ((Consumer<?>)invocationOnMock.getArgument(1)).accept(null);
      return null;
    }).when(iEntityMessenger).messageSelf(any(), any());
  }

  @After
  public void tearDown() throws Exception {
    ((ExecutionServiceImpl)executionService).close();
  }

  @Test
  public void testIdentify() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();

    /*
     * Test identify before connect -- should fail & not prevent subsequent proper identification.
     */
    DatasetEntityResponse idResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new IdentifyClientMessage(stableClientId));
    assertThat(idResponse, is(instanceOf(ErrorResponse.class)));
    assertThat(activeEntity.getConnectedClients(), not(hasItem(clientDescriptor)));

    /*
     * Now identify after proper connect ...
     */
    activeEntity.connected(clientDescriptor);
    assertThat(activeEntity.getConnectedClients(), hasItem(clientDescriptor));
    idResponse = activeEntity.invokeActive(createContext(clientDescriptor), new IdentifyClientMessage(stableClientId));
    if (idResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) idResponse).getCause());
    }
    assertThat(idResponse, is(instanceOf(SuccessResponse.class)));

    /*
     * Try the identify sequence again for the same client -- should return an IllegalStateException
     */
    UUID ignoredClientId = UUID.randomUUID();
    idResponse = activeEntity.invokeActive(createContext(clientDescriptor), new IdentifyClientMessage(ignoredClientId));
    assertThat(idResponse, is(instanceOf(ErrorResponse.class)));
    Throwable againException = ((ErrorResponse)idResponse).getCause();
    assertThat(againException, is(instanceOf(IllegalStateException.class)));
    assertThat(againException.getMessage(), containsString(" already has a stable client ID "));

    /*
     * Attempt to identify an alternate client using the same ID -- should return ClientIdCollisionException
     */
    activeEntity.connected(altClientDescriptor);
    assertThat(activeEntity.getConnectedClients(), hasItem(altClientDescriptor));
    idResponse = activeEntity.invokeActive(createContext(altClientDescriptor), new IdentifyClientMessage(stableClientId));
    assertThat(idResponse, is(instanceOf(ErrorResponse.class)));
    Throwable altException = ((ErrorResponse)idResponse).getCause();
    assertThat(altException, is(instanceOf(ClientIdCollisionException.class)));
    assertThat(altException.getMessage(), containsString(" is already assigned "));

    /*
     * Disconnect the alternate client ...
     */
    activeEntity.disconnected(altClientDescriptor);
    assertThat(activeEntity.getConnectedClients(), not(hasItem(altClientDescriptor)));

    /*
     * Identify the alternate client using a different ID.
     */
    UUID altAssignedClientId = UUID.randomUUID();
    activeEntity.connected(altClientDescriptor);
    idResponse = activeEntity.invokeActive(createContext(altClientDescriptor), new IdentifyClientMessage(altAssignedClientId));
    if (idResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) idResponse).getCause());
    }
    assertThat(idResponse, is(instanceOf(SuccessResponse.class)));
  }

  @Test
  public void createNewCreatesASovereignDataset() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    checkDatasetExists();
    assertTrue(addRecord(activeEntity, 123L, Collections.emptyList()));
  }

  @Test
  public void loadExistingLoadsASovereignDataset() throws Exception {
    DatasetActiveEntity<Long> originalActiveEntity = createNewDataset();
    assertTrue(addRecord(originalActiveEntity, 123L, Collections.emptyList()));

    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);
    DatasetActiveEntity<Long> restoredActiveEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    restoredActiveEntity.loadExisting();

    checkDatasetExists();

    assertFalse(addRecord(restoredActiveEntity, 123L, Collections.emptyList()));
    assertTrue(addRecord(restoredActiveEntity, 456L, Collections.emptyList()));
  }

  @Test
  public void loadExistingTransientCreatesSovereignDataset() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);
    DatasetActiveEntity<Long> restoredActiveEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    restoredActiveEntity.loadExisting();

    checkDatasetExists();
  }

  @Test
  public void destroyDestroysTheSovereignDataset() throws Exception {
    DatasetActiveEntity<Long> originalActiveEntity = createNewDataset();
    checkDatasetExists();
    originalActiveEntity.destroy();
    assertEquals(0, storageFactory.getStorage().getManagedDatasets().size());
  }

  @Test
  public void handlesGetRecordMessages() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    assertFalse(recordExists(activeEntity, 123L));
    assertTrue(addRecord(activeEntity, 123L, Collections.emptyList()));
    assertTrue(recordExists(activeEntity, 123L));
  }

  @Test
  public void handlesGetRecordMessagesWithPredicate() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    assertFalse(recordExists(activeEntity, 123L,  alwaysTrue()));
    assertTrue(addRecord(activeEntity, 123L, Collections.emptyList()));
    assertTrue(recordExists(activeEntity, 123L,  alwaysTrue()));
    assertFalse(recordExists(activeEntity, 123L,  alwaysTrue().negate()));
  }

  @Test
  public void handlesPredicatedUpdateSimplifiedMessages() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    assertFalse(recordExists(activeEntity, 123L));
    assertFalse(predicatedUpdateRecord(activeEntity, 123L,  alwaysTrue()));
    assertFalse(predicatedUpdateRecord(activeEntity, 123L,  alwaysTrue().negate()));
    assertTrue(addRecord(activeEntity, 123L, Collections.emptyList()));
    long msn = recordMsn(activeEntity, 123L);
    assertFalse(predicatedUpdateRecord(activeEntity, 123L,  alwaysTrue().negate()));
    assertEquals(msn, recordMsn(activeEntity, 123L));
    assertTrue(predicatedUpdateRecord(activeEntity, 123L,  alwaysTrue()));
    assertNotEquals(msn, recordMsn(activeEntity, 123L));
  }

  @Test
  public void handlesPredicatedUpdateFullMessages() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    assertFalse(recordExists(activeEntity, 123L));
    assertFalse(predicatedUpdateRecordReturnTuple(activeEntity, 123L,  alwaysTrue()).isPresent());
    assertFalse(predicatedUpdateRecordReturnTuple(activeEntity, 123L,  alwaysTrue().negate()).isPresent());
    assertTrue(addRecord(activeEntity, 123L, Collections.emptyList()));
    long msn = recordMsn(activeEntity, 123L);
    assertFalse(predicatedUpdateRecordReturnTuple(activeEntity, 123L,  alwaysTrue().negate()).isPresent());
    assertEquals(msn, recordMsn(activeEntity, 123L));
    Optional<Tuple<Long, Long>> tuple = predicatedUpdateRecordReturnTuple(activeEntity, 123L,  alwaysTrue());
    assertTrue(tuple.isPresent());
    assertEquals(msn, tuple.get().getFirst().longValue());
    long newMsn = tuple.get().getSecond();
    assertNotEquals(msn, newMsn);
    assertNotEquals(msn, recordMsn(activeEntity, 123L));
    assertEquals(newMsn, recordMsn(activeEntity, 123L));
  }

  @Test
  public void predicatedUpdateExactlyOnce() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    Long key = 123L;
    assertTrue(addRecord(activeEntity, key, Collections.emptyList()));

    long clientId = 5L;
    long currentTransactionId = 3L;
    long oldestTransactionId = 3L;
    ActiveInvokeContext<DatasetEntityResponse> context = createContext(clientId, currentTransactionId, oldestTransactionId);

    boolean respondInFull = true;
    IntrinsicUpdateOperation<Long> updateOperation = (IntrinsicUpdateOperation<Long>) Functions.installUpdateOperation(emptySet());
    PredicatedUpdateRecordMessage<Long> message = new PredicatedUpdateRecordMessage<>(stableClientId, key, alwaysTrue(), updateOperation, respondInFull);
    DatasetEntityResponse response = activeEntity.invokeActive(context, message);
    assertThat(response, notNullValue());

    ArgumentCaptor<PredicatedUpdateRecordMessage<?>> originalCaptured = ArgumentCaptor.forClass(PredicatedUpdateRecordMessage.class);
    ArgumentCaptor<CRUDDataReplicationMessage> replicatedCaptured = ArgumentCaptor.forClass(CRUDDataReplicationMessage.class);
    verify(iEntityMessenger, times(2)).messageSelfAndDeferRetirement(originalCaptured.capture(), replicatedCaptured.capture());
    assertThat(originalCaptured.getValue(), sameInstance(message));
    CRUDDataReplicationMessage capturedMessage = replicatedCaptured.getValue();
    assertThat(capturedMessage.getClientId(), is(clientId));
    assertThat(capturedMessage.getCurrentTransactionId(), is(currentTransactionId));
    assertThat(capturedMessage.getOldestTransactionId(), is(oldestTransactionId));
    assertThat(capturedMessage.isRespondInFull(), is(respondInFull));

    assertThat(activeEntity.invokeActive(context, message), sameInstance(response));
  }

  @Test
  public void handlesPredicatedDeleteSimplifiedMessages() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    assertFalse(recordExists(activeEntity, 123L));
    assertFalse(predicatedDeleteRecord(activeEntity, 123L,  alwaysTrue()));
    assertFalse(predicatedDeleteRecord(activeEntity, 123L,  alwaysTrue().negate()));
    assertTrue(addRecord(activeEntity, 123L, Collections.emptyList()));
    assertFalse(predicatedDeleteRecord(activeEntity, 123L,  alwaysTrue().negate()));
    assertTrue(predicatedDeleteRecord(activeEntity, 123L,  alwaysTrue()));
    assertFalse(recordExists(activeEntity, 123L));
  }

  @Test
  public void handlesPredicatedDeleteFullMessages() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    assertFalse(recordExists(activeEntity, 123L));
    assertNull(predicatedDeleteRecordReturnData(activeEntity, 123L,  alwaysTrue()));
    assertNull(predicatedDeleteRecordReturnData(activeEntity, 123L,  alwaysTrue().negate()));
    assertTrue(addRecord(activeEntity, 123L, Collections.emptyList()));
    long msn = recordMsn(activeEntity, 123L);
    assertNull(predicatedDeleteRecordReturnData(activeEntity, 123L,  alwaysTrue().negate()));
    assertEquals(msn, recordMsn(activeEntity, 123L));
    assertEquals(msn, predicatedDeleteRecordReturnData(activeEntity, 123L,  alwaysTrue()).getMsn());
    assertFalse(recordExists(activeEntity, 123L));
  }

  @Test
  public void predicatedDeleteExactlyOnce() throws Exception {
    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    Long key = 123L;
    assertTrue(addRecord(activeEntity, key, Collections.emptyList()));

    long clientId = 5L;
    long currentTransactionId = 3L;
    long oldestTransactionId = 3L;
    ActiveInvokeContext<DatasetEntityResponse> context = createContext(clientId, currentTransactionId, oldestTransactionId);

    boolean respondInFull = true;
    PredicatedDeleteRecordMessage<Long> message = new PredicatedDeleteRecordMessage<>(stableClientId, key, alwaysTrue(), respondInFull);
    DatasetEntityResponse response = activeEntity.invokeActive(context, message);
    assertThat(response, notNullValue());

    ArgumentCaptor<PredicatedDeleteRecordMessage<?>> originalCaptured = ArgumentCaptor.forClass(PredicatedDeleteRecordMessage.class);
    ArgumentCaptor<CRUDDataReplicationMessage> replicatedCaptured = ArgumentCaptor.forClass(CRUDDataReplicationMessage.class);
    verify(iEntityMessenger, times(2)).messageSelfAndDeferRetirement(originalCaptured.capture(), replicatedCaptured.capture());
    assertThat(originalCaptured.getValue(), sameInstance(message));
    CRUDDataReplicationMessage capturedMessage = replicatedCaptured.getValue();
    assertThat(capturedMessage.getClientId(), is(clientId));
    assertThat(capturedMessage.getCurrentTransactionId(), is(currentTransactionId));
    assertThat(capturedMessage.getOldestTransactionId(), is(oldestTransactionId));
    assertThat(capturedMessage.isRespondInFull(), is(respondInFull));

    assertThat(activeEntity.invokeActive(context, message), sameInstance(response));
  }

  @Test
  public void addRecordWithCellsAndRetrieve() throws Exception {
    Cell<?> cell1 = defineString("cell1").newCell("123");
    Cell<?> cell2 = defineString("cell2").newCell("456");

    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    assertFalse(recordExists(activeEntity, 123L));
    assertTrue(addRecord(activeEntity, 123L, Arrays.asList(cell1, cell2)));
    GetRecordResponse<Long> record = getRecord(activeEntity, 123L);
    assertThat(record.getData(), notNullValue());
    ArrayList<Cell<?>> recordCells = new ArrayList<>();
    record.getData().getCells().forEach(recordCells::add);
    assertEquals(Arrays.asList(cell1, cell2), recordCells);
    assertEquals(record.getData().getMsn(), addRecordReturnPreviousMsn(activeEntity, 123L, Collections.emptyList()));
  }

  @Test
  public void addRecordExactlyOnce() throws Exception {
    Cell<?> cell1 = defineString("cell1").newCell("123");
    Cell<?> cell2 = defineString("cell2").newCell("456");

    DatasetActiveEntity<Long> activeEntity = createNewDataset();

    long clientId = 5L;
    long currentTransactionId = 2L;
    long oldestTransactionId = 1L;
    ActiveInvokeContext<DatasetEntityResponse> context = createContext(clientId, currentTransactionId, oldestTransactionId);

    boolean respondInFull = true;
    AddRecordMessage<Long> message = new AddRecordMessage<>(stableClientId, 5L, Arrays.asList(cell1, cell2), respondInFull
    );
    DatasetEntityResponse response = activeEntity.invokeActive(context, message);
    assertThat(response, notNullValue());

    ArgumentCaptor<AddRecordMessage<?>> originalCaptured = ArgumentCaptor.forClass(AddRecordMessage.class);
    ArgumentCaptor<CRUDDataReplicationMessage> replicatedCaptured = ArgumentCaptor.forClass(CRUDDataReplicationMessage.class);
    verify(iEntityMessenger).messageSelfAndDeferRetirement(originalCaptured.capture(), replicatedCaptured.capture());
    assertThat(originalCaptured.getValue(), sameInstance(message));
    CRUDDataReplicationMessage capturedMessage = replicatedCaptured.getValue();
    assertThat(capturedMessage.getClientId(), is(clientId));
    assertThat(capturedMessage.getCurrentTransactionId(), is(currentTransactionId));
    assertThat(capturedMessage.getOldestTransactionId(), is(oldestTransactionId));
    assertThat(capturedMessage.isRespondInFull(), is(respondInFull));

    assertThat(activeEntity.invokeActive(context, message), sameInstance(response));
  }

  @Test
  public void createNewUsingFRS() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", "disk", emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);
    DatasetActiveEntity<Long> activeEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    activeEntity.createNew();

    PersistentStorageConfiguration storageConfiguration = (PersistentStorageConfiguration) storageFactory.getLastConfiguration();
    assertEquals(StorageType.FRS, storageConfiguration.getStorageType());
    String diskResource = storageConfiguration.getDiskResource();
    assertEquals("disk", diskResource);
    assertEquals(storageConfiguration.getPersistentStorageType(), PersistentStorageType.defaultEngine());
  }

  @Test
  public void addSendsEvents() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key", new ArrayList<>(), false)
    ), "TestClient{}:key:ADDITION:");
  }

  @Test
  public void addSendsEventsForTwoAdds() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key1", new ArrayList<>(), false),
            new AddRecordMessage<>(stableClientId, "key2", new ArrayList<>(), false)
    ), "TestClient{}:key1:ADDITION:", "TestClient{}:key2:ADDITION:");
  }

  @Test
  public void predicatedUpdateSendsEventsWhenUpdateSucceeds() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key1", new ArrayList<>(), false),
            new PredicatedUpdateRecordMessage<>(stableClientId, "key1",
                     alwaysTrue(),
                    (IntrinsicUpdateOperation<String>) Functions.installUpdateOperation(emptySet()),
                    false)
    ), "TestClient{}:key1:ADDITION:", "TestClient{}:key1:MUTATION:");
  }

  @Test
  public void predicatedUpdateSendsNoEventWhenUpdateFailsMissingRecord() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new PredicatedUpdateRecordMessage<>(stableClientId, "key1",
                     alwaysTrue(),
                    (IntrinsicUpdateOperation<String>) Functions.installUpdateOperation(emptySet()),
                    false)
    ));
  }

  @Test
  public void predicatedUpdateSendsNoEventWhenUpdateFailsPredicateIsFalse() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key1", new ArrayList<>(), false),
            new PredicatedUpdateRecordMessage<>(stableClientId, "key1",
                     alwaysTrue().negate(),
                    (IntrinsicUpdateOperation<String>) Functions.installUpdateOperation(emptySet()),
                    false)
    ), "TestClient{}:key1:ADDITION:");
  }

  @Test
  public void predicatedDeleteSendsEventsWhenDeleteSucceeds() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key1", new ArrayList<>(), false),
            new PredicatedDeleteRecordMessage<>(stableClientId, "key1",
                     alwaysTrue(), false)
    ), "TestClient{}:key1:ADDITION:", "TestClient{}:key1:DELETION:");
  }

  @Test
  public void predicatedDeleteNoEventWhenDeleteFailsMissingRecord() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new PredicatedDeleteRecordMessage<>(stableClientId, "key1",
                     alwaysTrue(), false)
    ));
  }

  @Test
  public void predicatedDeleteNoEventWhenDeleteFailsPredicateIsFalse() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key1", new ArrayList<>(), false),
            new PredicatedDeleteRecordMessage<>(stableClientId, "key1",
                     alwaysTrue().negate(), false)
    ), "TestClient{}:key1:ADDITION:");
  }

  @Test
  public void addDoesNotSendEventWhenRecordExists() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key", new ArrayList<>(), false),
            new AddRecordMessage<>(stableClientId, "key", new ArrayList<>(), false)
    ), "TestClient{}:key:ADDITION:");
  }

  @Test
  public void noEventsIfNotSubscribed() throws Exception {
    runSendEventTest(Arrays.asList(
            new AddRecordMessage<>(stableClientId, "key", new ArrayList<>(), false)
    ));
  }

  @Test
  public void noEventsIfDeregistered() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key1", new ArrayList<>(), false),
            new SendChangeEventsMessage(false),
            new AddRecordMessage<>(stableClientId, "key2", new ArrayList<>(), false)
    ), "TestClient{}:key1:ADDITION:");
  }

  @Test
  public void noEventsUnlessSubscribe() throws Exception {
    runSendEventTest(Arrays.asList(
            new AddRecordMessage<>(stableClientId, "key", new ArrayList<>(), false)
    ));
  }

  @Test
  public void turningChangeEventsOffWhenTheyWereAlreadyOff() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(false),
            new AddRecordMessage<>(stableClientId, "key1", new ArrayList<>(), false)
    ));
  }

  @Test
  public void askForChangeEventsGetChangeEvents() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new AddRecordMessage<>(stableClientId, "key", new ArrayList<>(), false)
    ), "TestClient{}:key:ADDITION:");
  }

  @Test
  public void changeToNoEventsGetNoEvents() throws Exception {
    runSendEventTest(Arrays.asList(
            new SendChangeEventsMessage(true),
            new SendChangeEventsMessage(false),
            new AddRecordMessage<>(stableClientId, "key", new ArrayList<>(), false)
    ));
  }

  @Test
  public void handleReconnectRestoresEventStatusNoEvents() throws Exception {
    byte[] reconnectData = new ReconnectCodec().encode(new ReconnectState(true, UUID.randomUUID()));
    runSendEventTest(
            (entity, client) -> {
              try {
                entity.startReconnect().handleReconnect(client, reconnectData);
              } catch (ReconnectRejectedException e) {
                e.printStackTrace();
              }
              return null;
            },
            Arrays.asList(
                    new AddRecordMessage<>(stableClientId, "checkLatch", new ArrayList<>(), false)
            ),
            "TestClient{}:checkLatch:ADDITION:"
    );
  }

  @Test
  public void eventsResentForRedeliveredMessage() throws Exception {
    runSendEventTest(2, Arrays.asList(
        new SendChangeEventsMessage(true),
        new AddRecordMessage<>(stableClientId, "checkLatch", new ArrayList<>(), false)
    ), "TestClient{}:checkLatch:ADDITION:", "TestClient{}:checkLatch:ADDITION:");
  }

  private void runSendEventTest(List<DatasetEntityMessage> messages, String... expectedResponses) throws Exception {
    runSendEventTest(1, messages, expectedResponses);
  }

  private void runSendEventTest(int sendCount, List<DatasetEntityMessage> messages, String... expectedResponses) throws Exception {
    runSendEventTest(sendCount, (entity, client) -> null, messages, expectedResponses);
  }

  private ActiveInvokeContext<DatasetEntityResponse> createContext(ClientDescriptor clientDescriptor) {
    ActiveInvokeContext<DatasetEntityResponse> invokeContext = mock(ActiveInvokeContext.class);
    when(invokeContext.getClientDescriptor()).thenReturn(clientDescriptor);
    when(invokeContext.isValidClientInformation()).thenReturn(true);
    ClientSourceId sourceId = clientDescriptor.getSourceId();
    when(invokeContext.getClientSource()).thenReturn(sourceId);
    when(invokeContext.getOldestTransactionId()).thenReturn(txnIdGenerator.get());
    when(invokeContext.getCurrentTransactionId()).thenReturn(txnIdGenerator.incrementAndGet());
    return invokeContext;
  }

  private ActiveInvokeContext<DatasetEntityResponse> createContext(long clientId, long currentTransactionId, long oldestTransactionId) {
    ClientSourceId clientSourceId = mock(ClientSourceId.class);
    when(clientSourceId.toLong()).thenReturn(clientId);
    ClientDescriptor clientDescriptor = mock(ClientDescriptor.class);
    when(clientDescriptor.getSourceId()).thenReturn(clientSourceId);
    ActiveInvokeContext<DatasetEntityResponse> invokeContext = mock(ActiveInvokeContext.class);
    when(invokeContext.getClientDescriptor()).thenReturn(clientDescriptor);
    when(invokeContext.getClientSource()).thenReturn(clientSourceId);
    when(invokeContext.getCurrentTransactionId()).thenReturn(currentTransactionId);
    when(invokeContext.getOldestTransactionId()).thenReturn(oldestTransactionId);
    return invokeContext;
  }

  private void runSendEventTest(BiFunction<DatasetActiveEntity<String>, ClientDescriptor, Void> entityConfig, List<DatasetEntityMessage> messages, String... expectedResponses) throws Exception {
    runSendEventTest(1, entityConfig, messages, expectedResponses);
  }

  private void runSendEventTest(int sendCount, BiFunction<DatasetActiveEntity<String>, ClientDescriptor, Void> entityConfig, List<DatasetEntityMessage> messages, String... expectedResponses) throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<String> configuration = new DatasetEntityConfiguration<>(Type.STRING, "address", datasetConfiguration);
    DatasetActiveEntity<String> activeEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    activeEntity.createNew();

    identifyClient(activeEntity, clientDescriptor);

    entityConfig.apply(activeEntity, clientDescriptor);

    for (DatasetEntityMessage message : messages) {
      ActiveInvokeContext<DatasetEntityResponse> context = createContext(clientDescriptor);
      for (int i = 0; i < sendCount; i++) {
        activeEntity.invokeActive(context, message);
      }
    }

    List<String> responses = clientCommunicator.getResponses();
    assertArrayEquals(expectedResponses, responses.toArray());
  }

  private DatasetActiveEntity<Long> createNewDataset() throws ConfigurationException, ServiceException {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);
    DatasetActiveEntity<Long> activeEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    activeEntity.createNew();

    MemoryStorageConfiguration storageConfiguration = (MemoryStorageConfiguration) storageFactory.getLastConfiguration();
    assertEquals(StorageType.MEMORY, storageConfiguration.getStorageType());

    return activeEntity;
  }

  @Ignore("get/tryGet locks only when under load")
  @Test
  public void testGetWhileLocked() throws Exception {
    lockedRecordTest((entity, key) -> {
      DatasetEntityResponse response = entity.invokeActive(createContext(clientDescriptor),
          new GetRecordMessage<>(key, alwaysTrue()));
      assertThat(response, is(instanceOf(BusyResponse.class)));
      assertThat(((BusyResponse)response).getReason(), is(BusyResponse.Reason.KEY));
    });
  }

  @Test
  public void testAddWhileLocked() throws Exception {
    lockedRecordTest((entity, key) -> {
      DatasetEntityResponse response = entity.invokeActive(createContext(clientDescriptor),
          new AddRecordMessage<>(stableClientId, key, Collections.emptySet(), false));
      assertThat(response, is(instanceOf(BusyResponse.class)));
      assertThat(((BusyResponse)response).getReason(), is(BusyResponse.Reason.KEY));
    });
  }

  @Test
  public void testUpdateWhileLocked() throws Exception {
    lockedRecordTest((entity, key) -> {
      DatasetEntityResponse response = entity.invokeActive(createContext(clientDescriptor),
          new PredicatedUpdateRecordMessage<>(stableClientId, key, alwaysTrue(),
              new InstallOperation<>(singleton(Cell.cell("foo", "value"))), false));
      assertThat(response, is(instanceOf(BusyResponse.class)));
      assertThat(((BusyResponse)response).getReason(), is(BusyResponse.Reason.KEY));
    });
  }

  @Test
  public void testDeleteWhileLocked() throws Exception {
    lockedRecordTest((entity, key) -> {
      DatasetEntityResponse response = entity.invokeActive(createContext(clientDescriptor),
          new PredicatedDeleteRecordMessage<>(stableClientId, key, alwaysTrue(), false));
      assertThat(response, is(instanceOf(BusyResponse.class)));
      assertThat(((BusyResponse)response).getReason(), is(BusyResponse.Reason.KEY));
    });
  }

  private void lockedRecordTest(LockableRecordConsumer<Long> proc) throws Exception {
    DatasetActiveEntity<Long> entity = createNewDataset();
    /*
     * Cheat and get direct access to the SovereignDataset.
     */
    @SuppressWarnings("unchecked") SovereignDataset<Long> dataset =
        (SovereignDataset<Long>)storageFactory.getStorage().getManagedDatasets().iterator().next();

    Long key = 1L;
    assertThat(entity.invokeActive(createContext(clientDescriptor),
                                  new AddRecordMessage<>(stableClientId, key, Collections.emptySet(), true)),
                                  is(notNullValue()));

    Phaser barrier = new Phaser(2);
    Runnable locker = () -> {
      dataset.records().filter(Record.<Long>keyFunction().is(key))
          .forEach(dataset.applyMutation(IMMEDIATE, r -> {
            barrier.arriveAndAwaitAdvance();
            try {
              barrier.awaitAdvanceInterruptibly(barrier.arrive(), 1L, TimeUnit.SECONDS);    // Await proc completion
            } catch (InterruptedException | TimeoutException e) {
              // ignored
            }
            return r;
          }));
      barrier.arriveAndDeregister();    // Permit main to continue
    };
    Thread lockerThread = new Thread(locker);
    lockerThread.setDaemon(true);
    lockerThread.start();

    barrier.arriveAndAwaitAdvance();    // Wait for Record lock to be held
    try {
      proc.accept(entity, key);
    } finally {
      barrier.arriveAndAwaitAdvance();      // Permit completion of the "mutation"
      try {
        barrier.awaitAdvanceInterruptibly(barrier.arrive(), 1L, TimeUnit.SECONDS);    // Await background completion
      } catch (InterruptedException | TimeoutException e) {
        // ignored
      }
    }
  }

  @Test
  public void testConsumeInlineElementSource() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();

    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");
    List<PipelineOperation> portableOperations = singletonList(
            FILTER.newInstance(mammalFilter));
    DatasetEntityResponse response;

    /*
     * Not yet connected ...
     */
    UUID streamId = UUID.randomUUID();
    response = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorOpenMessage(streamId,
            RemoteStreamType.INLINE, ElementType.RECORD, portableOperations, null, false));
    assertThat(response, is(instanceOf(ErrorResponse.class)));
    assertThat(((ErrorResponse) response).getCause(), is(instanceOf(StoreRuntimeException.class)));

    /*
     * Connect and try again ...
     */
    identifyClient(activeEntity, clientDescriptor);
    response = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorOpenMessage(streamId,
            RemoteStreamType.INLINE, ElementType.RECORD, portableOperations, null, false));
    if (response instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) response).getCause());
    }
    assertThat(response, is(instanceOf(PipelineProcessorOpenResponse.class)));
    UUID responseStreamId = ((PipelineProcessorOpenResponse) response).getStreamId();
    assertThat(responseStreamId, is(streamId));

    /*
     * Consume the element source ...
     */
    List<Record<String>> observedAnimalRecords = new ArrayList<>();
    boolean exhausted = false;
    do {
      response = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
      if (response instanceof ErrorResponse) {
        throw new AssertionError(((ErrorResponse) response).getCause());

      } else if (response instanceof TryAdvanceFetchApplyResponse) {
        @SuppressWarnings("unchecked")
        TryAdvanceFetchApplyResponse<String> fetchApplyResponse = (TryAdvanceFetchApplyResponse<String>) response;
        assertThat(fetchApplyResponse.getStreamId(), is(streamId));
        RecordData<String> recordData = fetchApplyResponse.getElement().getRecordData();
        observedAnimalRecords.add(new AnimalRecord(recordData.getKey(), recordData.getCells()));

        DatasetEntityResponse releaseResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceReleaseMessage(streamId));
        if (releaseResponse instanceof ErrorResponse) {
          throw new AssertionError(((ErrorResponse) releaseResponse).getCause());
        }

      } else if (response instanceof TryAdvanceFetchExhaustedResponse) {
        exhausted = true;
        assertThat(((TryAdvanceFetchExhaustedResponse) response).getStreamId(), is(streamId));

      }
    } while (!exhausted);

    assertThat(observedAnimalRecords, containsInAnyOrder(Animals.recordStream().filter(mammalFilter).toArray()));

    activeEntity.disconnected(clientDescriptor);
  }

  @Test
  public void testConsumeBatchedElementSourceInASingleBatch() throws Exception {
    testConsumeBatchedElementSource(TAXONOMIC_CLASS.value().is("mammal"), Integer.MAX_VALUE);
  }

  @Test
  public void testConsumeBatchedElementSourceInMultipleBatches() throws Exception {
    testConsumeBatchedElementSource(TAXONOMIC_CLASS.value().is("mammal"), 2);
  }

  @Test
  public void testConsumeEmptyBatchedElementSource() throws Exception {
    testConsumeBatchedElementSource(TAXONOMIC_CLASS.value().is("geography"), 10);
  }

  private void testConsumeBatchedElementSource(Predicate<Record<?>> filter, int batchSize) throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();

    List<PipelineOperation> portableOperations = singletonList(FILTER.newInstance(filter));
    DatasetEntityResponse response;

    /*
     * Not yet connected ...
     */
    UUID streamId = UUID.randomUUID();
    response = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorOpenMessage(streamId,
            RemoteStreamType.BATCHED, ElementType.RECORD, portableOperations, null, false));
    assertThat(response, is(instanceOf(ErrorResponse.class)));
    assertThat(((ErrorResponse) response).getCause(), is(instanceOf(StoreRuntimeException.class)));

    /*
     * Connect and try again ...
     */
    identifyClient(activeEntity, clientDescriptor);
    response = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorOpenMessage(streamId,
            RemoteStreamType.BATCHED, ElementType.RECORD, portableOperations, null, false));
    if (response instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) response).getCause());
    }
    assertThat(response, is(instanceOf(PipelineProcessorOpenResponse.class)));
    UUID responseStreamId = ((PipelineProcessorOpenResponse) response).getStreamId();
    assertThat(responseStreamId, is(streamId));

    /*
     * Consume the element source ...
     */
    List<Record<String>> observedAnimalRecords = new ArrayList<>();
    boolean exhausted = false;
    do {
      response = activeEntity.invokeActive(createContext(clientDescriptor), new BatchFetchMessage(streamId, batchSize));
      if (response instanceof ErrorResponse) {
        throw new AssertionError(((ErrorResponse) response).getCause());

      } else {
        @SuppressWarnings("unchecked")
        BatchFetchResponse fetchApplyResponse = (BatchFetchResponse) response;
        assertThat(fetchApplyResponse.getStreamId(), is(streamId));
        if (fetchApplyResponse.getElements().isEmpty()) {
          exhausted = true;
        } else {
          observedAnimalRecords.addAll(fetchApplyResponse.getElements().stream()
                  .map(Element::<String>getRecordData)
                  .map(rd -> new AnimalRecord(rd.getKey(), rd.getCells())).collect(Collectors.toList()));
        }
      }
    } while (!exhausted);

    assertThat(observedAnimalRecords, containsInAnyOrder(Animals.recordStream().filter(filter).toArray()));

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * This tests assures the processing for a pipeline containing a portable mutative terminal operation where
   * a portion of the pipeline is non-portable.  This test performs the processing expected of the following
   * client-side pipeline:
   * <pre>{@code
   *   stream
   *     .filter(TAXONOMIC_CLASS.value().is("mammal"))      // portable Predicate
   *     .filter(r -> !r.getKey().equals("echidna"))        // non-portable Predicate
   *     .mutate(write(OBSERVATIONS, OBSERVATIONS.longValueOr(0).increment())     // portable transform
   * }</pre>
   */
  @Test
  public void testConsumeMutativeElementSource() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();

    BuildableToLongFunction<Record<?>> observe = OBSERVATIONS.longValueOr(0).increment();
    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");
    int waypointId = 0;
    List<PipelineOperation> portableOperations = Arrays.asList(
        FILTER.newInstance(mammalFilter),
        FILTER.newInstance(new WaypointMarker<>(waypointId)));
    PipelineOperation terminalOperation = MUTATE.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(observe));
    DatasetEntityResponse response;

    UUID streamId = UUID.randomUUID();

    identifyClient(activeEntity, clientDescriptor);
    response = activeEntity.invokeActive(createContext(clientDescriptor),
        new PipelineProcessorOpenMessage(streamId, RemoteStreamType.INLINE, ElementType.TERMINAL, portableOperations, terminalOperation, false));
    if (response instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) response).getCause());
    }
    assertThat(response, is(instanceOf(PipelineProcessorOpenResponse.class)));
    UUID responseStreamId = ((PipelineProcessorOpenResponse) response).getStreamId();
    assertThat(responseStreamId, is(streamId));

    /*
     * Since this is a mutative stream, deferred retirement is used to respond to the TryAdvanceFetchMessage.
     * Set up a "reusable" retirement handle providing a means of awaiting fetch preparedness.
     */
    ExplicitRetirementHandle<?> retirementHandle = mock(ExplicitRetirementHandle.class);
    Phaser releaseLatch = new Phaser(2);
    doAnswer(invocation -> {
      releaseLatch.arrive();
      return null;
    }).when(retirementHandle).release();
    when(iEntityMessenger.deferRetirement(any(), any(),  any())).thenReturn(retirementHandle);

    /*
     * Consume/update the element source ...
     */
    List<Record<String>> observedAnimalRecords = new ArrayList<>();
    boolean exhausted = false;
    do {
      response = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
      if (response instanceof ErrorResponse) {
        throw new AssertionError(((ErrorResponse) response).getCause());

      } else if (!(response instanceof PipelineRequestProcessingResponse)) {
        throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + response);
      }
      assertThat(((PipelineRequestProcessingResponse)response).getStreamId(), is(streamId));

      /*
       * Await fetch prepare ...
       */
      long deadline = 30;
      try {
        releaseLatch.awaitAdvanceInterruptibly(releaseLatch.arrive(), deadline, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        throw new AssertionError("Fetch operation did not complete within " + deadline + " seconds");
      }

      /*
       * Now get the actual fetch response ...
       */
      response = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorRequestResultMessage(streamId));
      if (response instanceof ErrorResponse) {
        throw new AssertionError(((ErrorResponse) response).getCause());

      } else if (response instanceof TryAdvanceFetchWaypointResponse) {
        @SuppressWarnings("unchecked")
        TryAdvanceFetchWaypointResponse<String> fetchResponse = (TryAdvanceFetchWaypointResponse<String>) response;
        assertThat(fetchResponse.getStreamId(), is(streamId));
        assertThat(fetchResponse.getWaypointId(), is(waypointId));
        RecordData<String> recordData = fetchResponse.getElement().getRecordData();
        AnimalRecord animalRecord = new AnimalRecord(recordData.getKey(), recordData.getCells());
        observedAnimalRecords.add(animalRecord);

        /*
         * For an echidna, simulate a filter that drops the record ...
         */
        if (animalRecord.getKey().equals("echidna")) {
          DatasetEntityResponse releaseResponse =
              activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceReleaseMessage(streamId));
          if (releaseResponse instanceof ErrorResponse) {
            throw new AssertionError(((ErrorResponse)releaseResponse).getCause());
          } else if (!(releaseResponse instanceof TryAdvanceReleaseResponse)) {
            throw new AssertionError("Unexpected response to TryAdvanceReleaseMessage - " + releaseResponse);
          }
        } else {
          DatasetEntityResponse mutateResponse =
              activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceMutateMessage(streamId, waypointId, null));
          if (mutateResponse instanceof ErrorResponse) {
            throw new AssertionError(((ErrorResponse)mutateResponse).getCause());
          } else if (mutateResponse instanceof PipelineRequestProcessingResponse) {
            PipelineRequestProcessingResponse consumedResponse = (PipelineRequestProcessingResponse)mutateResponse;
            assertThat(consumedResponse.getStreamId(), is(streamId));

            /*
             * Await fetch prepare ...
             */
            try {
              releaseLatch.awaitAdvanceInterruptibly(releaseLatch.arrive(), deadline, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
              throw new AssertionError("Fetch operation did not complete within " + deadline + " seconds");
            }

            TryAdvanceFetchConsumedResponse fetchConsumedResponse = (TryAdvanceFetchConsumedResponse) activeEntity
                    .invokeActive(createContext(clientDescriptor), new PipelineProcessorRequestResultMessage(streamId));

            assertThat(fetchConsumedResponse.getStreamId(), is(streamId));

          } else {
            throw new AssertionError("Unexpected response to TryAdvanceMutateMessage - " + mutateResponse);
          }
        }

      } else if (response instanceof TryAdvanceFetchExhaustedResponse) {
        exhausted = true;
        assertThat(((TryAdvanceFetchExhaustedResponse) response).getStreamId(), is(streamId));

      } else {
        throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + response);
      }
    } while (!exhausted);

    assertThat(observedAnimalRecords, containsInAnyOrder(Animals.recordStream().filter(mammalFilter).toArray()));

    Map<String, Record<String>> targetMammals = Animals.recordStream()
        .filter(mammalFilter)
        .filter(r -> !r.getKey().equals("echidna"))
        .collect(toMap(Record::getKey, r -> r));

    for (String targetMammal : targetMammals.keySet()) {
      Record<String> actualRecord = getAnimalRecord(activeEntity, targetMammal);
      assertThat(actualRecord, is(not(nullValue())));
      assertThat(actualRecord.get(OBSERVATIONS).orElse(0L), is(1 + targetMammals.get(targetMammal).get(OBSERVATIONS).orElse(0L)));
    }

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * This tests assures expected handling of stream closure of a partially-portable, mutative pipeline immediately
   * following stream creation.  This test performs the processing expected of the following client-side pipeline:
   * <pre>{@code
   *   stream
   *     .filter(TAXONOMIC_CLASS.value().is("mammal"))      // portable Predicate
   *     .<non-portable-stuff>
   * }</pre>
   */
  @Test
  public void testBatchedElementSourceCloseAfterOpenBeforeFetch() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    identifyClient(activeEntity, clientDescriptor);

    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");
    List<PipelineOperation> portableOperations = singletonList(FILTER.newInstance(mammalFilter));
    DatasetEntityResponse response;

    UUID streamId = UUID.randomUUID();
    response = activeEntity.invokeActive(createContext(clientDescriptor),
            new PipelineProcessorOpenMessage(streamId, RemoteStreamType.BATCHED, ElementType.RECORD, portableOperations, null, false));
    if (response instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) response).getCause());
    }
    assertThat(response, is(instanceOf(PipelineProcessorOpenResponse.class)));
    assertThat(((PipelineProcessorOpenResponse) response).getStreamId(), is(streamId));
    assertThat(((PipelineProcessorOpenResponse) response).getSourceType(), is(RemoteStreamType.BATCHED));

    Collection<PipelineProcessor> openStreams = activeEntity.getOpenStreams(clientDescriptor);
    assertThat(openStreams, hasSize(1));

    /*
     * Close the stream _before_ obtaining any elements.
     */
    closePipelineProcessor(activeEntity, streamId);

    /*
     * Now try a fetch
     */
    DatasetEntityResponse fetchResponse = activeEntity.invokeActive(createContext(clientDescriptor), new BatchFetchMessage(streamId, 1));
    if (!(fetchResponse instanceof ErrorResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + fetchResponse);
    }
    Throwable cause = ((ErrorResponse)fetchResponse).getCause();
    if (!(cause instanceof StoreStreamNotFoundException)) {
      throw new AssertionError("Expecting StoreStreamNotFoundException; found " + cause.getClass().getName(), cause);
    }
    assertThat(cause.getMessage(), containsString("Stream operation failed"));

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * This tests assures expected handling of stream closure of a partially-portable, mutative pipeline immediately
   * following stream creation.  This test performs the processing expected of the following client-side pipeline:
   * <pre>{@code
   *   stream
   *     .filter(TAXONOMIC_CLASS.value().is("mammal"))      // portable Predicate
   *     .filter( <<nonPortablePredicate>> )
   *     .mutate(write(OBSERVATIONS, OBSERVATIONS.longValueOr(0).increment())     // portable transform
   * }</pre>
   */
  @Test
  public void testLockedElementSourceCloseAfterOpenBeforeFetch() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    identifyClient(activeEntity, clientDescriptor);

    BuildableToLongFunction<Record<?>> observe = OBSERVATIONS.longValueOr(0).increment();
    UpdateOperation.CellUpdateOperation<?, Long> transform = UpdateOperation.write(OBSERVATIONS).longResultOf(observe);
    Predicate<Record<?>> mammalFilter = TAXONOMIC_CLASS.value().is("mammal");
    int waypointId = 0;
    List<PipelineOperation> portableOperations = Arrays.asList(
        FILTER.newInstance(mammalFilter),
        FILTER.newInstance(new WaypointMarker<>(waypointId)));
    PipelineOperation terminalOperation = MUTATE.newInstance(transform);

    UUID streamId = UUID.randomUUID();
    openMutativeElementSource(activeEntity, streamId, portableOperations, terminalOperation);

    Collection<PipelineProcessor> openStreams = activeEntity.getOpenStreams(clientDescriptor);
    assertThat(openStreams, hasSize(1));
    InlineElementSourceAccessor<String, ?> elementSource = new InlineElementSourceAccessor<>((InlineElementSource<String, ?>) openStreams.iterator().next());
    assertThat(elementSource.getStreamId(), is(streamId));
    assertThat(elementSource.getCurrentState(), is("READY"));

    /*
     * Close the stream _before_ obtaining any elements.
     */
    closePipelineProcessor(activeEntity, streamId);
    assertInlineElementSourceClosed(elementSource);

    /*
     * Now try a fetch
     */
    DatasetEntityResponse fetchResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
    if (!(fetchResponse instanceof ErrorResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + fetchResponse);
    }
    Throwable cause = ((ErrorResponse)fetchResponse).getCause();
    if (!(cause instanceof StoreStreamNotFoundException)) {
      throw new AssertionError("Expecting StoreStreamNotFoundException; found " + cause.getClass().getName(), cause);
    }
    assertThat(cause.getMessage(), containsString("Stream operation failed"));
    assertThat(elementSource.getCurrentState(), is("CLOSED"));

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * This tests assures expected handling of stream closure of a partially-portable, mutative pipeline following
   * the fetch prepared response to the first fetch.  This test performs the processing expected of the following
   * client-side pipeline:
   * <pre>{@code
   *   stream
   *     .filter(TAXONOMIC_CLASS.value().is("mammal"))      // portable Predicate
   *     .filter( <<nonPortablePredicate>> )
   *     .mutate(write(OBSERVATIONS, OBSERVATIONS.longValueOr(0).increment())     // portable transform
   * }</pre>
   */
  @Test
  public void testElementSourceCloseAfterFetchBeforeResume() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    identifyClient(activeEntity, clientDescriptor);

    int waypointId = 0;
    List<PipelineOperation> portableOperations = Arrays.asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(waypointId)));
    BuildableToLongFunction<Record<?>> observe = OBSERVATIONS.longValueOr(0).increment();
    PipelineOperation terminalOperation =
        MUTATE.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(observe));

    UUID streamId = UUID.randomUUID();
    openMutativeElementSource(activeEntity, streamId, portableOperations, terminalOperation);

    Collection<PipelineProcessor> openStreams = activeEntity.getOpenStreams(clientDescriptor);
    assertThat(openStreams, hasSize(1));
    InlineElementSourceAccessor<String, ?> elementSource = new InlineElementSourceAccessor<>((InlineElementSource) openStreams.iterator().next());
    assertThat(elementSource.getStreamId(), is(streamId));
    assertThat(elementSource.getCurrentState(), is("READY"));

    /*
     * This test influences behavior of DatasetActiveEntity.tryAdvanceFetch by manipulating a lock internal
     * to InlineElementSource ... access to which is obtained here.
     */
    Field stateLockField = InlineElementSource.class.getDeclaredField("stateLock");
    stateLockField.setAccessible(true);
    ReadWriteLock stateLock = (ReadWriteLock)stateLockField.get(elementSource.getElementSource());

    /*
     * Provide a retirement handle through which the test can be notified the fetch prepare is complete.
     */
    ExplicitRetirementHandle<?> retirementHandle = mock(ExplicitRetirementHandle.class);
    CountDownLatch releaseLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      releaseLatch.countDown();
      return null;
    }).when(retirementHandle).release();
    when(iEntityMessenger.deferRetirement(any(), any(),  any())).thenReturn(retirementHandle);

    /*
     * Interlock InlineElementSource to delay the fetch operation ... we want to invoke close before the fetch is
     * completed.
     */
    Lock elementSourceLock = stateLock.writeLock();
    elementSourceLock.lock();
    try {
      /*
       * Now try a fetch; since the stream holds a mutation, we're expecting a fetch prepared response (as a
       * deferred response)
       */
      DatasetEntityResponse fetchResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
      if (fetchResponse instanceof ErrorResponse) {
        throw new AssertionError(((ErrorResponse)fetchResponse).getCause());
      } else {
        if (!(fetchResponse instanceof PipelineRequestProcessingResponse)) {
          throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + fetchResponse);
        }
      }
      assertThat(((PipelineRequestProcessingResponse)fetchResponse).getStreamId(), is(streamId));

      /*
       * At this point, we have a pending fetch request that is not complete.  Close the element source
       */
      closePipelineProcessor(activeEntity, streamId);

    } finally {
      elementSourceLock.unlock();
    }

    /*
     * Await fetch prepare completion -- should happen even though the client is now disconnected.
     */
    long deadline = 30L;
    if (!releaseLatch.await(deadline, TimeUnit.SECONDS)) {
      throw new AssertionError("Fetch operation did not complete within " + deadline + " seconds");
    }

    /*
     * Now resume the fetch; we should see it closed.
     */
    DatasetEntityResponse resumeResponse = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorRequestResultMessage(streamId));
    if (!(resumeResponse instanceof ErrorResponse)) {
      throw new AssertionError("Unexpected response to PipelineProcessorRequestResultMessage - " + resumeResponse);
    }
    /*
     * The specific failure exception is racy ... we're happy with any StoreRuntimeException
     */
    assertThat(((ErrorResponse)resumeResponse).getCause(), is(instanceOf(StoreRuntimeException.class)));

    assertInlineElementSourceClosed(elementSource);

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * This tests assures expected handling of stream closure of a partially-portable, mutative pipeline following
   * the waypoint response to the first fetch.  This test performs the processing expected of the following
   * client-side pipeline:
   * <pre>{@code
   *   stream
   *     .filter(TAXONOMIC_CLASS.value().is("mammal"))      // portable Predicate
   *     .filter( <<nonPortablePredicate>> )
   *     .mutate(write(OBSERVATIONS, OBSERVATIONS.longValueOr(0).increment())     // portable transform
   * }</pre>
   */
  @Test
  public void testInlineElementSourceCloseAfterWaypointBeforeMutate() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    identifyClient(activeEntity, clientDescriptor);

    int waypointId = 0;
    List<PipelineOperation> portableOperations = Arrays.asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(waypointId)));
    BuildableToLongFunction<Record<?>> observe = OBSERVATIONS.longValueOr(0).increment();
    PipelineOperation terminalOperation =
        MUTATE.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(observe));

    UUID streamId = UUID.randomUUID();
    openMutativeElementSource(activeEntity, streamId, portableOperations, terminalOperation);

    Collection<PipelineProcessor> openStreams = activeEntity.getOpenStreams(clientDescriptor);
    assertThat(openStreams, hasSize(1));
    InlineElementSourceAccessor<String, ?> elementSource = new InlineElementSourceAccessor<>((InlineElementSource<String, ?>) openStreams.iterator().next());
    assertThat(elementSource.getStreamId(), is(streamId));
    assertThat(elementSource.getCurrentState(), is("READY"));

    /*
     * Provide a retirement handle through which the test can be notified the fetch prepare is complete.
     */
    ExplicitRetirementHandle<?> retirementHandle = mock(ExplicitRetirementHandle.class);
    CountDownLatch releaseLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      releaseLatch.countDown();
      return null;
    }).when(retirementHandle).release();
    when(iEntityMessenger.deferRetirement(any(), any(),  any())).thenReturn(retirementHandle);

    /*
     * Now try a fetch; since the stream holds a mutation, we're expecting a fetch prepared response (as a
     * deferred response)
     */
    DatasetEntityResponse fetchResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
    if (fetchResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)fetchResponse).getCause());
    } else {
      if (!(fetchResponse instanceof PipelineRequestProcessingResponse)) {
        throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + fetchResponse);
      }
    }
    assertThat(((PipelineRequestProcessingResponse)fetchResponse).getStreamId(), is(streamId));

    /*
     * Await fetch prepare completion.
     */
    long deadline = 30L;
    if (!releaseLatch.await(deadline, TimeUnit.SECONDS)) {
      throw new AssertionError("Fetch operation did not complete within " + deadline + " seconds");
    }

     /*
     * Now resume the fetch; we're expecting a waypoint response
     */
    DatasetEntityResponse resumeResponse = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorRequestResultMessage(streamId));
    if (resumeResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)resumeResponse).getCause());
    } else if (!(resumeResponse instanceof TryAdvanceFetchWaypointResponse)) {
      throw new AssertionError("Unexpected response to PipelineProcessorRequestResultMessage - " + resumeResponse);
    }
    TryAdvanceFetchWaypointResponse<?> waypointResponse = (TryAdvanceFetchWaypointResponse)resumeResponse;
    assertThat(waypointResponse.getWaypointId(), is(waypointId));
    Element element = waypointResponse.getElement();
    assertThat(element.getType(), is(ElementType.RECORD));
    assertThat(element.getRecordData(), is(not(nullValue())));
    assertThat(elementSource.getCurrentState(), is("WAYPOINT_PENDING"));

    /*
     * Close the stream _after_ obtaining waypoint response.
     */
    closePipelineProcessor(activeEntity, streamId);
    assertInlineElementSourceClosed(elementSource);

    /*
     * Attempt to resume at the waypoint.  Since the transform is portable, no payload is necessary.
     */
    DatasetEntityResponse mutateResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceMutateMessage(streamId, waypointId, null));
    if (!(mutateResponse instanceof ErrorResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceMutateMessage - " + mutateResponse);
    }
    Throwable cause = ((ErrorResponse)mutateResponse).getCause();
    if (!(cause instanceof StoreStreamNotFoundException)) {
      throw new AssertionError("Expecting StoreStreamNotFoundException; found " + cause.getClass().getName(), cause);
    }
    assertThat(cause.getMessage(), containsString("Stream operation failed"));
    assertThat(elementSource.getCurrentState(), is("CLOSED"));

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * This tests assures expected handling of stream closure of a partially-portable, mutative pipeline following
   * the mutation response for the first element and releasing that element from the pipeline.  This test performs
   * the processing expected of the following client-side pipeline:
   * <pre>{@code
   *   stream
   *     .filter(TAXONOMIC_CLASS.value().is("mammal"))      // portable Predicate
   *     .filter( <<nonPortablePredicate>> )
   *     .mutateThen(write(OBSERVATIONS, OBSERVATIONS.longValueOr(0).increment())     // portable transform
   *     .<<some undesignated conclusion>>
   * }</pre>
   */
  @Test
  public void testInlineElementSourceCloseAfterMutateBeforeRelease() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    identifyClient(activeEntity, clientDescriptor);

    BuildableToLongFunction<Record<?>> observe = OBSERVATIONS.longValueOr(0).increment();
    int waypointId = 0;
    List<PipelineOperation> portableOperations = Arrays.asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(waypointId)),
        MUTATE_THEN_INTERNAL.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(observe)));

    UUID streamId = UUID.randomUUID();
    openMutativeElementSource(activeEntity, streamId, portableOperations, null);

    Collection<PipelineProcessor> openStreams = activeEntity.getOpenStreams(clientDescriptor);
    assertThat(openStreams, hasSize(1));
    InlineElementSourceAccessor<String, ?> elementSource = new InlineElementSourceAccessor<>((InlineElementSource<String, ?>) openStreams.iterator().next());
    assertThat(elementSource.getStreamId(), is(streamId));
    assertThat(elementSource.getCurrentState(), is("READY"));

    /*
     * Provide a retirement handle through which the test can be notified the fetch prepare is complete.
     */
    ExplicitRetirementHandle<?> retirementHandle = mock(ExplicitRetirementHandle.class);
    CountDownLatch releaseLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      releaseLatch.countDown();
      return null;
    }).when(retirementHandle).release();
    when(iEntityMessenger.deferRetirement(any(), any(),  any())).thenReturn(retirementHandle);

    /*
     * Now try a fetch; since the stream holds a mutation, we're expecting a fetch prepared response (as a
     * deferred response)
     */
    DatasetEntityResponse fetchResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
    if (fetchResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)fetchResponse).getCause());
    } else {
      if (!(fetchResponse instanceof PipelineRequestProcessingResponse)) {
        throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + fetchResponse);
      }
    }
    assertThat(((PipelineRequestProcessingResponse)fetchResponse).getStreamId(), is(streamId));

    /*
     * Await fetch prepare completion.
     */
    long deadline = 30L;
    if (!releaseLatch.await(deadline, TimeUnit.SECONDS)) {
      throw new AssertionError("Fetch operation did not complete within " + deadline + " seconds");
    }

     /*
     * Now resume the fetch; we're expecting a waypoint response
     */
    DatasetEntityResponse resumeResponse = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorRequestResultMessage(streamId));
    if (resumeResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)resumeResponse).getCause());
    } else if (!(resumeResponse instanceof TryAdvanceFetchWaypointResponse)) {
      throw new AssertionError("Unexpected response to PipelineProcessorRequestResultMessage - " + resumeResponse);
    }
    TryAdvanceFetchWaypointResponse<?> waypointResponse = (TryAdvanceFetchWaypointResponse)resumeResponse;
    assertThat(waypointResponse.getWaypointId(), is(waypointId));
    Element element = waypointResponse.getElement();
    assertThat(element.getType(), is(ElementType.RECORD));
    RecordData<String> preMutationRecordData = element.getRecordData();
    assertThat(preMutationRecordData, is(not(nullValue())));
    assertThat(elementSource.getCurrentState(), is("WAYPOINT_PENDING"));

    /*
     * Attempt to resume at the waypoint.  Since the transform is portable, no payload is necessary.
     */
    DatasetEntityResponse mutateResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceMutateMessage(streamId, waypointId, null));
    if (mutateResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)mutateResponse).getCause());
    } else if (!(mutateResponse instanceof PipelineRequestProcessingResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceMutateMessage - " + mutateResponse);
    }
    reset(iEntityMessenger);

    PipelineRequestProcessingResponse processingResponse = (PipelineRequestProcessingResponse)mutateResponse;

    /*
     * Await fetch prepare completion.
     */
    if (!releaseLatch.await(deadline, TimeUnit.SECONDS)) {
      throw new AssertionError("Fetch operation did not complete within " + deadline + " milliseconds");
    }
    mutateResponse = awaitPipelineResult(activeEntity, processingResponse.getStreamId());
    if (!(mutateResponse instanceof TryAdvanceFetchApplyResponse)) {
      throw new AssertionError("Unexpected response to PipelineRequestProcessingResponse - " + mutateResponse);
    }
    TryAdvanceFetchApplyResponse<?> applyResponse = (TryAdvanceFetchApplyResponse)mutateResponse;
    element = applyResponse.getElement();
    assertThat(element.getType(), is(ElementType.RECORD));
    assertThat(element.getRecordData(), is(not(nullValue())));
    assertThat(elementSource.getCurrentState(), is("RELEASE_REQUIRED"));

    RecordData<String> postMutationRecordData = element.getRecordData();

    AnimalRecord preMutationRecord = new AnimalRecord(preMutationRecordData.getKey(), preMutationRecordData.getCells());
    assertThat(new AnimalRecord(postMutationRecordData.getKey(), postMutationRecordData.getCells()).get(OBSERVATIONS).orElse(0L),
        is(1 + preMutationRecord.get(OBSERVATIONS).orElse(0L)));

    /*
     * Close the stream _after_ getting response from waypoint/mutation.
     */
    closePipelineProcessor(activeEntity, streamId);
    assertInlineElementSourceClosed(elementSource);

    /*
     * Attempt to send RELEASE
     */
    DatasetEntityResponse releaseResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceReleaseMessage(streamId));
    if (!(releaseResponse instanceof ErrorResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceReleaseMessage - " + releaseResponse);
    }
    Throwable cause = ((ErrorResponse)releaseResponse).getCause();
    if (!(cause instanceof StoreStreamNotFoundException)) {
      throw new AssertionError("Expecting StoreStreamNotFoundException; found " + cause.getClass().getName(), cause);
    }
    assertThat(cause.getMessage(), containsString("Stream operation failed"));
    assertThat(elementSource.getCurrentState(), is("CLOSED"));

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * This tests assures expected handling of stream closure of a partially-portable, mutative pipeline following
   * release of the first element and fetching the second element from the pipeline.  This test performs
   * the processing expected of the following client-side pipeline:
   * <pre>{@code
   *   stream
   *     .filter(TAXONOMIC_CLASS.value().is("mammal"))      // portable Predicate
   *     .filter( <<nonPortablePredicate>> )
   *     .mutateThen(write(OBSERVATIONS, OBSERVATIONS.longValueOr(0).increment())     // portable transform
   *     .<<some undesignated conclusion>>
   * }</pre>
   */
  @Test
  public void testInlineElementSourceCloseAfterReleaseBeforeFetch() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    identifyClient(activeEntity, clientDescriptor);

    BuildableToLongFunction<Record<?>> observe = OBSERVATIONS.longValueOr(0).increment();
    int waypointId = 0;

    List<PipelineOperation> portableOperations = Arrays.asList(
        FILTER.newInstance(TAXONOMIC_CLASS.value().is("mammal")),
        FILTER.newInstance(new WaypointMarker<>(waypointId)),
        MUTATE_THEN_INTERNAL.newInstance(UpdateOperation.write(OBSERVATIONS).longResultOf(observe)));

    UUID streamId = UUID.randomUUID();
    openMutativeElementSource(activeEntity, streamId, portableOperations, null);

    Collection<PipelineProcessor> openStreams = activeEntity.getOpenStreams(clientDescriptor);
    assertThat(openStreams, hasSize(1));
    InlineElementSourceAccessor<String, ?> elementSource = new InlineElementSourceAccessor<>((InlineElementSource<String, ?>) openStreams.iterator().next());
    assertThat(elementSource.getStreamId(), is(streamId));
    assertThat(elementSource.getCurrentState(), is("READY"));

    /*
     * Provide a retirement handle through which the test can be notified the fetch prepare is complete.
     */
    ExplicitRetirementHandle<?> retirementHandle = mock(ExplicitRetirementHandle.class);
    CountDownLatch releaseLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      releaseLatch.countDown();
      return null;
    }).when(retirementHandle).release();
    when(iEntityMessenger.deferRetirement(any(), any(),  any())).thenReturn(retirementHandle);

    /*
     * Now try a fetch; since the stream holds a mutation, we're expecting a fetch prepared response (as a
     * deferred response)
     */
    DatasetEntityResponse fetchResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
    if (fetchResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)fetchResponse).getCause());
    } else {
      if (!(fetchResponse instanceof PipelineRequestProcessingResponse)) {
        throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + fetchResponse);
      }
    }
    assertThat(((PipelineRequestProcessingResponse)fetchResponse).getStreamId(), is(streamId));

    /*
     * Await fetch prepare completion.
     */
    long deadline = 30L;
    if (!releaseLatch.await(deadline, TimeUnit.SECONDS)) {
      throw new AssertionError("Fetch operation did not complete within " + deadline + " seconds");
    }

    /*
     * Now get the real response for the fetch
     */
    DatasetEntityResponse resumeResponse = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorRequestResultMessage(streamId));
    if (resumeResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)resumeResponse).getCause());
    } else if (!(resumeResponse instanceof TryAdvanceFetchWaypointResponse)) {
      throw new AssertionError("Unexpected response to PipelineProcessorRequestResultMessage - " + resumeResponse);
    }
    TryAdvanceFetchWaypointResponse<?> waypointResponse = (TryAdvanceFetchWaypointResponse)resumeResponse;
    assertThat(waypointResponse.getWaypointId(), is(waypointId));
    Element element = waypointResponse.getElement();
    assertThat(element.getType(), is(ElementType.RECORD));
    RecordData<String> preMutationRecordData = element.getRecordData();
    assertThat(preMutationRecordData, is(not(nullValue())));
    assertThat(elementSource.getCurrentState(), is("WAYPOINT_PENDING"));

    /*
     * Attempt to resume at the waypoint.  Since the transform is portable, no payload is necessary.
     */
    DatasetEntityResponse mutateResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceMutateMessage(streamId, waypointId, null));
    if (mutateResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)mutateResponse).getCause());
    } else if (!(mutateResponse instanceof PipelineRequestProcessingResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceMutateMessage - " + mutateResponse);
    }

    reset(iEntityMessenger);

    PipelineRequestProcessingResponse processingResponse = (PipelineRequestProcessingResponse)mutateResponse;
    /*
     * Await fetch prepare completion.
     */
    if (!releaseLatch.await(deadline, TimeUnit.SECONDS)) {
      throw new AssertionError("Fetch operation did not complete within " + deadline + " seconds");
    }
    mutateResponse = awaitPipelineResult(activeEntity, processingResponse.getStreamId());
    if (!(mutateResponse instanceof TryAdvanceFetchApplyResponse)) {
      throw new AssertionError("Unexpected response to PipelineRequestProcessingResponse - " + mutateResponse);
    }

    TryAdvanceFetchApplyResponse<?> applyResponse = (TryAdvanceFetchApplyResponse)mutateResponse;

    element = applyResponse.getElement();
    assertThat(element.getType(), is(ElementType.RECORD));
    assertThat(element.getRecordData(), is(not(nullValue())));
    assertThat(elementSource.getCurrentState(), is("RELEASE_REQUIRED"));

    RecordData<String> postMutationRecordData = element.getRecordData();

    AnimalRecord preMutationRecord = new AnimalRecord(preMutationRecordData.getKey(), preMutationRecordData.getCells());
    assertThat(new AnimalRecord(postMutationRecordData.getKey(), postMutationRecordData.getCells()).get(OBSERVATIONS).orElse(0L),
        is(1 + preMutationRecord.get(OBSERVATIONS).orElse(0L)));

    /*
     * Release the element from the pipeline.
     */
    DatasetEntityResponse releaseResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceReleaseMessage(streamId));
    if (releaseResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)releaseResponse).getCause());
    } else if (!(releaseResponse instanceof TryAdvanceReleaseResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceReleaseMessage - " + releaseResponse);
    }
    TryAdvanceReleaseResponse okReleaseResponse = (TryAdvanceReleaseResponse)releaseResponse;
    assertThat(okReleaseResponse.getStreamId(), is(streamId));

    /*
     * Close the stream _after_ releasing the element and _before_ the next fetch.
     */
    closePipelineProcessor(activeEntity, streamId);
    assertInlineElementSourceClosed(elementSource);

    /*
     * Attempt a fetch for the next element.
     */
    fetchResponse = activeEntity.invokeActive(createContext(clientDescriptor), new TryAdvanceFetchMessage(streamId));
    if (!(fetchResponse instanceof ErrorResponse)) {
      throw new AssertionError("Unexpected response to TryAdvanceFetchMessage - " + fetchResponse);
    }
    Throwable cause = ((ErrorResponse)fetchResponse).getCause();
    if (!(cause instanceof StoreStreamNotFoundException)) {
      throw new AssertionError("Expecting StoreStreamNotFoundException; found " + cause.getClass().getName(), cause);
    }
    assertThat(cause.getMessage(), containsString("Stream operation failed"));
    assertThat(elementSource.getCurrentState(), is("CLOSED"));

    activeEntity.disconnected(clientDescriptor);
  }

  /**
   * Test dynamic manipulation of indexes.  As a unit test, this method is awful -- it tests too much
   * stuff.  But the pre-conditions for setting up a test are fairly substantial and include index
   * operations that need to be tested ...
   */
  @Test
  public void testDynamicIndex() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    UUID stableClientId = identifyClient(activeEntity, clientDescriptor);
    UUID altStableClientId = identifyClient(activeEntity, altClientDescriptor);

    SovereignIndexing sovereignIndexing = activeEntity.getDataset().getIndexing();
    assertThat(sovereignIndexing.getIndex(TAXONOMIC_CLASS, SovereignIndexSettings.BTREE), is(nullValue()));

    /*
     * This test influences behavior or DatasetActiveEntity.createIndex by manipulating an
     * internal lock ... access to which is obtained here.
     */
    ReentrantLock createRequestLock = getReentrantLock(activeEntity);

    /*
     * First, attempt to delete the index.
     */
    DatasetEntityResponse preDeleteResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new IndexDestroyMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE));
    assertThat(preDeleteResponse, is(instanceOf(ErrorResponse.class)));
    assertThat(sovereignIndexing.getIndex(TAXONOMIC_CLASS, SovereignIndexSettings.BTREE), is(nullValue()));

    /*
     * Now initiate index creation.
     */
    ExplicitRetirementHandle<?> retirementHandle = mock(ExplicitRetirementHandle.class);
    CountDownLatch releaseLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      releaseLatch.countDown();
      return null;
    }).when(retirementHandle).release();
    when(iEntityMessenger.deferRetirement(any(), any(),  any())).thenReturn(retirementHandle);

    /*
     * Interfere with createIndex processing by taking the lock used by createIndex internally.
     * Being a ReentrantLock, this will not prevent createIndex from operating but will prevent
     * "administrative" completion of the indexing operation.
     */
    String createRequestId;
    String altCreateRequestId;
    createRequestLock.lock();
    try {
      /*
       * When used by a client, delivery of the response to IndexCreateMessage is delayed (intentionally)
       * by Voltron infrastructure.  This delay happens within Voltron's handling of the response -- return
       * from the invoke method is not affected.
       */
      DatasetEntityResponse createResponse =
          activeEntity.invokeActive(createContext(clientDescriptor), new IndexCreateMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE, stableClientId));
      assertThat(createResponse, is(instanceOf(IndexCreateAcceptedResponse.class)));
      createRequestId = ((IndexCreateAcceptedResponse)createResponse).getCreationRequestId();

      /*
       * While preventing the indexing operation from completing, initiate another request from the same client.
       */
      DatasetEntityResponse altCreateResponse =
          activeEntity.invokeActive(createContext(clientDescriptor), new IndexCreateMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE, stableClientId));
      assertThat(altCreateResponse, is(instanceOf(ErrorResponse.class)));
      assertThat(((ErrorResponse)altCreateResponse).getCause(), is(instanceOf(IllegalStateException.class)));

      /*
       * Again, while preventing the indexing operation from completing, initiate another request from
       * a different client.
       */
      altCreateResponse =
          activeEntity.invokeActive(createContext(altClientDescriptor), new IndexCreateMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE, altStableClientId));
      assertThat(altCreateResponse, is(instanceOf(IndexCreateAcceptedResponse.class)));
      altCreateRequestId = ((IndexCreateAcceptedResponse)altCreateResponse).getCreationRequestId();

    } finally {
      createRequestLock.unlock();
    }

    long deadline = 60L;
    if (!releaseLatch.await(deadline, TimeUnit.SECONDS)) {
      throw new AssertionError("Indexing operation did not complete within " + deadline + " seconds");
    }

    /*
     * The Sovereign dataset index had better exist.
     */
    SovereignIndex<String> index = sovereignIndexing.getIndex(TAXONOMIC_CLASS, SovereignIndexSettings.BTREE);
    assertThat(index, is(notNullValue()));
    assertTrue(index.isLive());

    DatasetEntityResponse statusResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new IndexCreateStatusMessage(createRequestId, stableClientId));
    assertThat(statusResponse, is(instanceOf(IndexCreateStatusResponse.class)));
    assertThat(((IndexCreateStatusResponse)statusResponse).getStatus(), is(Index.Status.LIVE));

    DatasetEntityResponse altStatusResponse =
        activeEntity.invokeActive(createContext(altClientDescriptor), new IndexCreateStatusMessage(altCreateRequestId, altStableClientId));
    assertThat(altStatusResponse, is(instanceOf(IndexCreateStatusResponse.class)));
    assertThat(((IndexCreateStatusResponse)altStatusResponse).getStatus(), is(Index.Status.LIVE));

    /*
     * Do it again expecting failure because it already exists...
     */
    DatasetEntityResponse recreateResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new IndexCreateMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE, stableClientId));
    assertThat(recreateResponse, is(instanceOf(ErrorResponse.class)));
    assertThat(((ErrorResponse)recreateResponse).getCause(), is(instanceOf(IllegalArgumentException.class)));

    /*
     * Now that we've successfully created the index, delete it.
     */
    DatasetEntityResponse deleteResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new IndexDestroyMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE));
    assertThat(deleteResponse, is(instanceOf(SuccessResponse.class)));
    assertThat(sovereignIndexing.getIndex(TAXONOMIC_CLASS, SovereignIndexSettings.BTREE), is(nullValue()));

    activeEntity.disconnected(altClientDescriptor);
    activeEntity.disconnected(clientDescriptor);
  }

  private <K extends Comparable<K>> UUID identifyClient(DatasetActiveEntity<K> activeEntity, ClientDescriptor clientDescriptor)
      throws EntityUserException {
    activeEntity.connected(clientDescriptor);

    UUID stableClientId = UUID.randomUUID();
    DatasetEntityResponse idResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new IdentifyClientMessage(stableClientId));
    if (idResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) idResponse).getCause());
    }
    assertThat(idResponse, is(instanceOf(SuccessResponse.class)));
    return stableClientId;
  }

  /**
   * This test ensures a create index request is properly tracked/completed when the originating client
   * disconnects.
   */
  @Test
  public void testDynamicIndexWithClientDisconnect() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    UUID stableClientId = identifyClient(activeEntity, clientDescriptor);
    UUID altStableClientId = identifyClient(activeEntity, altClientDescriptor);

    /*
     * This test influences behavior or DatasetActiveEntity.createIndex by manipulating an
     * internal lock ... access to which is obtained here.
     */
    ReentrantLock createRequestLock = getReentrantLock(activeEntity);

    createRequestLock.lock();
    try {
      /*
       * Use a couple of clients to initiate an index creation.
       */
      DatasetEntityResponse createResponse =
          activeEntity.invokeActive(createContext(clientDescriptor), new IndexCreateMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE, stableClientId));
      assertThat(createResponse, is(instanceOf(IndexCreateAcceptedResponse.class)));

      DatasetEntityResponse altCreateResponse =
          activeEntity.invokeActive(createContext(altClientDescriptor), new IndexCreateMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE, altStableClientId));
      assertThat(altCreateResponse, is(instanceOf(IndexCreateAcceptedResponse.class)));

      Map.Entry<CellDefinition<?>, IndexSettings> requestKey =
          new AbstractMap.SimpleImmutableEntry<>(TAXONOMIC_CLASS, IndexSettings.BTREE);
      assertThat(activeEntity.getPendingIndexRequests().get(requestKey),
          containsInAnyOrder(stableClientId, altStableClientId));
      assertThat(activeEntity.getPendingIndexRequests(clientDescriptor), contains(requestKey));
      assertThat(activeEntity.getPendingIndexRequests(altClientDescriptor), contains(requestKey));

      /*
       * Disconnect the second client -- this should discard its interest in the request but leave the
       * other intact.
       */
      activeEntity.disconnected(altClientDescriptor);

      assertThat(activeEntity.getPendingIndexRequests().get(requestKey), contains(stableClientId));
      assertThat(activeEntity.getPendingIndexRequests(clientDescriptor), contains(requestKey));
      assertThat(activeEntity.getPendingIndexRequests(altClientDescriptor), is(empty()));

      /*
       * Disconnect the first client -- the request should remain in place without "interested parties".
       */
      activeEntity.disconnected(clientDescriptor);

      assertThat(activeEntity.getPendingIndexRequests().get(requestKey), is(empty()));
      assertThat(activeEntity.getPendingIndexRequests(clientDescriptor), is(empty()));
      assertThat(activeEntity.getPendingIndexRequests(altClientDescriptor), is(empty()));

      /*
       * Try the request from yet a third client ...
       */
      UUID requesterId = identifyClient(activeEntity, thirdClientDescriptor);
      DatasetEntityResponse thirdCreateResponse =
          activeEntity.invokeActive(createContext(thirdClientDescriptor), new IndexCreateMessage<>(TAXONOMIC_CLASS, IndexSettings.BTREE, requesterId));
      assertThat(thirdCreateResponse, is(instanceOf(IndexCreateAcceptedResponse.class)));

      assertThat(activeEntity.getPendingIndexRequests().get(requestKey), contains(requesterId));
      assertThat(activeEntity.getPendingIndexRequests(thirdClientDescriptor), contains(requestKey));

      activeEntity.disconnected(thirdClientDescriptor);

    } finally {
      createRequestLock.unlock();
    }

    /*
     * Now that all of the clients are gone, ensure the indexing request completes.
     * Since the clients are all gone, the retirementHandle hook used in testDynamicIndex
     * can't be used ... waiting for the indexing operation to be completed has to be done
     * through polling.
     */
    SovereignIndexing sovereignIndexing = activeEntity.getDataset().getIndexing();
    long deadline = TimeUnit.SECONDS.toMillis(60L);
    long startTime = System.currentTimeMillis();
    SovereignIndex<String> index;
    do {
      index = sovereignIndexing.getIndex(TAXONOMIC_CLASS, SovereignIndexSettings.BTREE);
      if (index == null) {
        if (System.currentTimeMillis() - startTime >= deadline) {
          throw new AssertionError("Indexing operation did not complete within "
              + TimeUnit.MILLISECONDS.toSeconds(deadline) + " seconds");
        }
        TimeUnit.MILLISECONDS.sleep(5L);
      }
    } while ( index == null );

    long waitTime = 50L;
    while (!index.isLive()) {
      if (System.currentTimeMillis() - startTime >= deadline) {
        throw new AssertionError("Indexing operation did not complete within "
            + TimeUnit.MILLISECONDS.toSeconds(deadline) + " seconds");
      }
      TimeUnit.MILLISECONDS.sleep(waitTime);
      waitTime <<= 1;
    }

    assertThat(index, is(notNullValue()));
    assertTrue(index.isLive());
  }

  /**
   * This test ensures that asynchronous index creation tasks don't
   * get involved in a race condition with disconnect calls.
   */
  @Test
  public void testDynamicIndexWithConcurrentDisconnect() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    IntStream.range(0, 50)
            .mapToObj(id -> new NamedClientDescriptor(String.format("TestClient{%d}", id), id))
            .peek(descriptor -> connectAndCreateNewIndex(activeEntity, descriptor))
            .forEach(activeEntity::disconnected);
  }

  private void connectAndCreateNewIndex(DatasetActiveEntity<String> activeEntity, ClientDescriptor descriptor) {
    try {
      UUID stableClientId = identifyClient(activeEntity, descriptor);
      CellDefinition<String> definition = CellDefinition.defineString("Field for " + descriptor);
      activeEntity.invokeActive(createContext(descriptor),
              new IndexCreateMessage<>(definition, IndexSettings.BTREE, stableClientId));
    } catch (EntityUserException e) {
      throw new RuntimeException(e);
    }
  }

  private static ReentrantLock getReentrantLock(DatasetActiveEntity<String> activeEntity) throws NoSuchFieldException, IllegalAccessException {
    Field datasetEntityStateServiceField = DatasetActiveEntity.class.getDeclaredField("datasetEntityStateService");
    datasetEntityStateServiceField.setAccessible(true);
    DatasetEntityStateService<?> entityStateService = (DatasetEntityStateService)datasetEntityStateServiceField.get(activeEntity);

    Field creationRequestLockField = DatasetEntityStateService.class.getDeclaredField("creationRequestLock");
    creationRequestLockField.setAccessible(true);
    return (ReentrantLock)creationRequestLockField.get(entityStateService);
  }

  @Test
  public void testIndexStatus() throws Exception {
    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    identifyClient(activeEntity, clientDescriptor);

    SovereignIndexing sovereignIndexing = activeEntity.getDataset().getIndexing();

    sovereignIndexing.createIndex(TAXONOMIC_CLASS, SovereignIndexSettings.BTREE).call();
    sovereignIndexing.createIndex(OBSERVATIONS, SovereignIndexSettings.BTREE);    // Define but don't populate

    assertIndexStatus(activeEntity, TAXONOMIC_CLASS, Index.Status.LIVE);
    assertIndexStatus(activeEntity, OBSERVATIONS, Index.Status.INITALIZING);
    assertIndexStatus(activeEntity, STATUS, Index.Status.DEAD);

    activeEntity.disconnected(clientDescriptor);
  }

  @Test
  public void testDisconnectUntracksClient() throws Exception {
    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = mock(OOOMessageHandler.class);
    serviceRegistry.addService(OOOMessageHandler.class, messageHandler);

    DatasetActiveEntity<Long> activeEntity = createNewDataset();
    ClientSourceId clientSourceId = mock(ClientSourceId.class);

    activeEntity.notifyDestroyed(clientSourceId);
    verify(messageHandler).untrackClient(clientSourceId);
  }

  @Test
  public void testMessageTrackerSync() throws Exception {
    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = mock(OOOMessageHandler.class);
    serviceRegistry.addService(OOOMessageHandler.class, messageHandler);

    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap(), 2);
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);
    DatasetActiveEntity<Long> activeEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    activeEntity.createNew();

    long clientId = 5L;
    ClientSourceId clientSourceId = mock(ClientSourceId.class);
    when(clientSourceId.toLong()).thenReturn(clientId);
    when(messageHandler.getTrackedClients()).thenReturn(Stream.of(clientSourceId));

    int index1 = 0;
    long txnId1 = 123L;
    when(messageHandler.getTrackedResponsesForSegment(index1, clientSourceId)).thenReturn(Collections.singletonMap(txnId1, mock(DatasetEntityResponse.class)));
    int index2 = 1;
    long txnId2 = 234L;
    when(messageHandler.getTrackedResponsesForSegment(index2, clientSourceId)).thenReturn(Collections.singletonMap(txnId2, mock(DatasetEntityResponse.class)));
    int index3 = 2;
    long txnId3 = 345L;
    when(messageHandler.getTrackedResponsesForSegment(index3, clientSourceId)).thenReturn(Collections.singletonMap(txnId3, mock(DatasetEntityResponse.class)));

    PassiveSynchronizationChannel<DatasetEntityMessage> syncChannel = mock(PassiveSynchronizationChannel.class);
    activeEntity.prepareKeyForSynchronizeOnPassive(syncChannel, 1); //Non-blocking concurrency key for shard 1
    verify(syncChannel, never()).synchronizeToPassive(any(MessageTrackerSyncMessage.class));

    reset(syncChannel);
    ArgumentCaptor<DatasetEntityMessage> messageCaptor = ArgumentCaptor.forClass(DatasetEntityMessage.class);
    activeEntity.synchronizeKeyToPassive(syncChannel, 1);   //Blocking concurrency key for shard 1
    verify(syncChannel, atLeastOnce()).synchronizeToPassive(messageCaptor.capture());
    assertThat(messageCaptor.getAllValues().stream()
        .filter(msg -> msg.getType() == MESSAGE_TRACKER_SYNC_MESSAGE)
        .map(msg -> (MessageTrackerSyncMessage) msg)
        .filter(msg -> msg.getClientId() == clientId)
        .filter(msg -> msg.getSegmentIndex() == index1)
        .flatMap(msg -> msg.getTrackedResponses().entrySet().stream())
        .filter(entry -> entry.getKey() == txnId1)
        .count(), is(1L));

    reset(syncChannel);
    activeEntity.prepareKeyForSynchronizeOnPassive(syncChannel, 2);   //Non-blocking concurrency key for shard 2
    verify(syncChannel, never()).synchronizeToPassive(any(MessageTrackerSyncMessage.class));

    when(messageHandler.getTrackedClients()).thenReturn(Stream.of(clientSourceId)); //Redefining the stream here as the previous operation exhausts the stream opened in the original mock
    reset(syncChannel);
    messageCaptor = ArgumentCaptor.forClass(DatasetEntityMessage.class);
    activeEntity.synchronizeKeyToPassive(syncChannel, 2);   //Blocking concurrency key for shard 2
    verify(syncChannel, atLeastOnce()).synchronizeToPassive(messageCaptor.capture());
    assertThat(messageCaptor.getAllValues().stream()
        .filter(msg -> msg.getType() == MESSAGE_TRACKER_SYNC_MESSAGE)
        .map(msg -> (MessageTrackerSyncMessage) msg)
        .filter(msg -> msg.getClientId() == clientId)
        .filter(msg -> msg.getSegmentIndex() == index2)
        .flatMap(msg -> msg.getTrackedResponses().entrySet().stream())
        .filter(entry -> entry.getKey() == txnId2)
        .count(), is(1L));

    when(messageHandler.getTrackedClients()).thenReturn(Stream.of(clientSourceId));
    reset(syncChannel);
    messageCaptor = ArgumentCaptor.forClass(DatasetEntityMessage.class);
    activeEntity.synchronizeKeyToPassive(syncChannel, 3);   //concurrency key for message tracker data of non-CRUD ops
    verify(syncChannel).synchronizeToPassive(messageCaptor.capture());
    assertThat(messageCaptor.getAllValues().size(), is(1)); //Only the message tracker data sync message
    assertThat(messageCaptor.getAllValues().stream()
        .filter(msg -> msg.getType() == MESSAGE_TRACKER_SYNC_MESSAGE)
        .map(msg -> (MessageTrackerSyncMessage) msg)
        .filter(msg -> msg.getClientId() == clientId)
        .filter(msg -> msg.getSegmentIndex() == index3)
        .flatMap(msg -> msg.getTrackedResponses().entrySet().stream())
        .filter(entry -> entry.getKey() == txnId3)
        .count(), is(1L));
  }

  @Test
  public void pipelineExecutorOverloaded() throws Exception {
    ExecutionService overloadedExecutionService = mock(ExecutionService.class);
    ExecutorService overloadedExecutorService = mock(ExecutorService.class);
    when(overloadedExecutionService.getPipelineProcessorExecutor(any(String.class))).thenReturn(overloadedExecutorService);
    when(overloadedExecutionService.getUnorderedExecutor(any(String.class))).thenReturn(overloadedExecutorService);
    doThrow(new RejectedExecutionException()).when(overloadedExecutorService).execute(any(Runnable.class));
    serviceRegistry.addService(ExecutionService.class, overloadedExecutionService);

    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    activeEntity.connected(clientDescriptor);

    UUID streamId = UUID.randomUUID();
    PipelineOperation terminalOperation = PipelineOperation.TerminalOperation.DELETE.newInstance();
    DatasetEntityResponse response = activeEntity.invokeActive(createContext(clientDescriptor),
            new PipelineProcessorOpenMessage(
                    streamId,
                    RemoteStreamType.INLINE,
                    ElementType.ELEMENT_VALUE,
                    new ArrayList<>(),
                    terminalOperation,
                    false
            )
    );

    assertEquals(BUSY_RESPONSE, response.getType());
  }

  @Test
  public void pipelineExecutorOverloadedWithTerminalStream() throws Exception {
    ExecutionService overloadedExecutionService = mock(ExecutionService.class);
    ExecutorService overloadedExecutorService = mock(ExecutorService.class);
    when(overloadedExecutionService.getPipelineProcessorExecutor(any(String.class))).thenReturn(overloadedExecutorService);
    when(overloadedExecutionService.getUnorderedExecutor(any(String.class))).thenReturn(overloadedExecutorService);
    doThrow(new RejectedExecutionException()).when(overloadedExecutorService).execute(any(Runnable.class));
    serviceRegistry.addService(ExecutionService.class, overloadedExecutionService);

    DatasetActiveEntity<String> activeEntity = createNewAnimalDataset();
    activeEntity.connected(clientDescriptor);

    UUID streamId = UUID.randomUUID();
    PipelineOperation terminalOperation = PipelineOperation.TerminalOperation.DELETE.newInstance();
    DatasetEntityResponse response = activeEntity.invokeActive(createContext(clientDescriptor),
        new ExecuteTerminatedPipelineMessage(
            streamId,
            emptyList(),
            terminalOperation,
            false, true));

    assertEquals(BUSY_RESPONSE, response.getType());
    assertThat(activeEntity.getOpenStreams(clientDescriptor), empty());
  }

  @Test
  public void testSyncBoundaryMessageAfterSyncIsComplete() throws Exception {
    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = mock(OOOMessageHandler.class);
    serviceRegistry.addService(OOOMessageHandler.class, messageHandler);

    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap(), 2);
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);
    DatasetActiveEntity<Long> activeEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    activeEntity.createNew();

    reset(iEntityMessenger);
    ArgumentCaptor<DatasetEntityMessage> argumentCaptor = ArgumentCaptor.forClass(DatasetEntityMessage.class);

    doNothing().when(iEntityMessenger).messageSelf(argumentCaptor.capture());
    PassiveSynchronizationChannel<DatasetEntityMessage> syncChannel = mock(PassiveSynchronizationChannel.class, RETURNS_DEEP_STUBS);
    activeEntity.prepareKeyForSynchronizeOnPassive(syncChannel, 1);
    activeEntity.synchronizeKeyToPassive(syncChannel, 1);

    assertThat(argumentCaptor.getAllValues().size(), is(1));
    assertThat(argumentCaptor.getValue(), instanceOf(SyncBoundaryMessage.class));
  }

  private <T extends Comparable<T>>
  void assertIndexStatus(DatasetActiveEntity<String> activeEntity,
                         ComparableCellDefinition<T> cellDefinition,
                         Index.Status expectedStatus) throws EntityUserException {
    DatasetEntityResponse statusResponse =
        activeEntity.invokeActive(createContext(clientDescriptor), new IndexStatusMessage<>(cellDefinition, IndexSettings.BTREE));
    assertThat(statusResponse, is(instanceOf(IndexStatusResponse.class)));
    assertThat(((IndexStatusResponse)statusResponse).getStatus(), is(expectedStatus));
  }

  /**
   * Asserts an {@link InlineElementSource} is closed as observed through a wrapping {@link InlineElementSourceAccessor}.
   * @param elementSource the {@code InlineElementSourceAccessor}
   */
  private void assertInlineElementSourceClosed(InlineElementSourceAccessor<String, ?> elementSource) throws InterruptedException {
    assertThat(elementSource.getCurrentState(), is("CLOSED"));
    threadExitLoop:
    {
      Thread elementProducerThread = null;
      long delay = 5L;
      for (int i = 8; i > 0; i--) {
        elementProducerThread = elementSource.getElementProducerThread();
        if (elementProducerThread == null) {
          break threadExitLoop;
        }
        TimeUnit.MILLISECONDS.sleep(delay);
        delay <<= 1;
      }
      Diagnostics.threadDump();
      throw new AssertionError("elementProducerThread did not terminate - " + elementProducerThread);
    }
  }

  /**
   * Awaits completion of a pipeline operation ({@code TryAdvanceFetchMessage}) for which a
   * {@code PipelineRequestProcessingResponse} was received.  These responses are prepared asynchronously
   * and not available until all background processing is done.  This is signaled with a call to
   * {@link IEntityMessenger#messageSelf(EntityMessage)}.
   *
   * @param activeEntity the {@code DatasetActiveEntity} for the operations
   * @param streamId the value of {@link PipelineRequestProcessingResponse#getStreamId()} from the initial response
   * @return the real response for the operation
   * @throws Exception if the fetch for the response failed
   */
  @SuppressWarnings("rawtypes")
  private DatasetEntityResponse awaitPipelineResult(DatasetActiveEntity<String> activeEntity, UUID streamId)
      throws Exception {
    ArgumentCaptor<Consumer<IEntityMessenger.MessageResponse>> captor = ArgumentCaptor.forClass(Consumer.class);    // unchecked
    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      while (true) {
        try {
          assertThat(captor.getAllValues().size(), is(0));
          verify(iEntityMessenger).messageSelf(any(), captor.capture());
          System.out.format("Observed iEntityMessenger.messageSelf(*,%s)%n", captor.getValue());
        } catch (Throwable e) {
          //no op
        }
        if (captor.getAllValues().size() == 1) {
          break;
        }
      }
      captor.getValue().accept(null);
    }, Executors.newSingleThreadExecutor());
    DatasetEntityResponse mutateResponse = activeEntity.invokeActive(createContext(clientDescriptor),
        new PipelineProcessorRequestResultMessage(streamId));

    try {
      future.get(30, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      List<Consumer<IEntityMessenger.MessageResponse>> capturedValues = captor.getAllValues();
      Diagnostics.threadDump();
      throw new AssertionError("Failed timely observation of non-null messageSelf() call - captor.getAllValues()=" + capturedValues, e);
    }

    System.out.format("Observed iEntityMessenger.messageSelf() calls: %s%n", captor.getAllValues());

    return mutateResponse;
  }

  /**
   * Closes an {@link PipelineProcessor} through a {@link DatasetActiveEntity}.
   * @param activeEntity the {@code DatasetActiveEntity} owing the {@code InlineElementSource}
   * @param streamId the stream identifier of the {@code InlineElementSource} to close
   */
  private void closePipelineProcessor(DatasetActiveEntity<String> activeEntity, UUID streamId) throws EntityUserException {
    DatasetEntityResponse closeResponse = activeEntity.invokeActive(createContext(clientDescriptor), new PipelineProcessorCloseMessage(streamId));
    if (closeResponse instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse)closeResponse).getCause());
    } else if (!(closeResponse instanceof PipelineProcessorCloseResponse)) {
      throw new AssertionError("Unexpected response to PipelineProcessorCloseMessage - " + closeResponse);
    }
    assertThat(((PipelineProcessorCloseResponse)closeResponse).getStreamId(), is(streamId));
  }

  /**
   * Opens a new {@link InlineElementSource} with mutative pipeline provided.
   * @param activeEntity the {@code DatasetActiveEntity} through which the stream pipeline is opened
   * @param streamId the stream identifier to assign to the {@code InlineElementSource}
   * @param portableOperations the portable intermediate operations sequence
   * @param terminalOperation the portable, mutative terminal operation; {@code null} if the pipeline is not terminated
   */
  private void openMutativeElementSource(DatasetActiveEntity<String> activeEntity,
                                         UUID streamId,
                                         List<PipelineOperation> portableOperations,
                                         PipelineOperation terminalOperation) throws EntityUserException {
    PipelineProcessorOpenMessage openMessage;
    if (terminalOperation == null) {
      ElementType elementType;
      PipelineOperation.Operation lastOperation = portableOperations.get(portableOperations.size() - 1).getOperation();
      if (EnumSet.of(MUTATE_THEN_INTERNAL, DELETE_THEN).contains(lastOperation)) {
        elementType = ElementType.RECORD;
      } else if (MUTATE_THEN.equals(lastOperation)) {
        /* Returns a Tuple<RecordData<K>, RecordData<K>> */
        elementType = ElementType.ELEMENT_VALUE;
      } else {
        throw new AssertionError("Unsupported final intermediate operation - " + lastOperation);
      }
      openMessage = new PipelineProcessorOpenMessage(streamId, RemoteStreamType.INLINE, elementType, portableOperations, null, false);
    }
    else openMessage = new PipelineProcessorOpenMessage(streamId, RemoteStreamType.INLINE, ElementType.TERMINAL, portableOperations,
        terminalOperation, false);
    DatasetEntityResponse response = activeEntity.invokeActive(createContext(clientDescriptor), openMessage);
    if (response instanceof ErrorResponse) {
      throw new AssertionError(((ErrorResponse) response).getCause());
    }
    assertThat(response, is(instanceOf(PipelineProcessorOpenResponse.class)));
    UUID responseStreamId = ((PipelineProcessorOpenResponse) response).getStreamId();
    assertThat(responseStreamId, is(streamId));
  }

  /**
   * Creates a new, populated {@link Animals} dataset.
   *
   * @return the new {@code DatasetActiveEntity} instance
   */
  private DatasetActiveEntity<String> createNewAnimalDataset() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<String> configuration = new DatasetEntityConfiguration<>(Type.STRING, "animals", datasetConfiguration);
    DatasetActiveEntity<String> activeEntity = DatasetActiveEntity.create(serviceRegistry, configuration);
    activeEntity.createNew();

    MemoryStorageConfiguration storageConfiguration = (MemoryStorageConfiguration) storageFactory.getLastConfiguration();
    assertEquals(StorageType.MEMORY, storageConfiguration.getStorageType());

    Animals.recordStream().forEach(
            r -> {
              AddRecordSimplifiedResponse response = null;
              try {
                response = (AddRecordSimplifiedResponse) activeEntity.invokeActive(
                    createContext(clientDescriptor), new AddRecordMessage<>(stableClientId, r.getKey(), r, false));
              } catch (EntityUserException e) {
                throw new AssertionError("Invoke failed", e);
              }
              if (!response.isAdded()) {
                throw new AssertionError("Failed to add record for '" + r.getKey() + "'");
              }
            }
    );

    return activeEntity;
  }

  private Record<String> getAnimalRecord(DatasetActiveEntity<String> activeEntity, String key) throws EntityUserException {
    IntrinsicPredicate<Record<?>> alwaysTruePredicate =  alwaysTrue();
    DatasetEntityResponse response =
        activeEntity.invokeActive(createContext(clientDescriptor), new GetRecordMessage<>(key, alwaysTruePredicate));
    if (response instanceof GetRecordResponse) {
      GetRecordResponse<String> recordResponse = (GetRecordResponse)response;
      RecordData<String> recordData = recordResponse.getData();
      if (recordData != null) {
        return new AnimalRecord(recordData.getKey(), recordData.getCells());
      } else {
        return null;
      }
    } else {
      throw new AssertionError("Unexpected response for get '" + key + "' - " + response);
    }
  }

  private void checkDatasetExists() {
    assertEquals(1, storageFactory.getStorage().getManagedDatasets().size());
    SovereignDataset<?> dataset = storageFactory.getStorage().getManagedDatasets().iterator().next();
    assertEquals(Type.LONG, dataset.getType());
    assertEquals("address", dataset.getAlias());
  }

  private boolean addRecord(DatasetActiveEntity<Long> activeEntity, long key, Iterable<Cell<?>> cells) throws EntityUserException {
    AddRecordSimplifiedResponse response = (AddRecordSimplifiedResponse) activeEntity.invokeActive(
        createContext(clientDescriptor), new AddRecordMessage<>(stableClientId, key, cells, false));
    return response.isAdded();
  }

  private long addRecordReturnPreviousMsn(DatasetActiveEntity<Long> activeEntity, long key, Iterable<Cell<?>> cells) throws EntityUserException {
    AddRecordFullResponse<?> response = (AddRecordFullResponse) activeEntity.invokeActive(createContext(clientDescriptor),
        new AddRecordMessage<>(stableClientId, key, cells, true));
    return response.getExisting().getMsn();
  }

  private boolean predicatedUpdateRecord(DatasetActiveEntity<Long> activeEntity, long key, IntrinsicPredicate<? super Record<Long>> predicate) throws EntityUserException {
    IntrinsicUpdateOperation<Long> updateOperation = (IntrinsicUpdateOperation<Long>) Functions.installUpdateOperation(emptySet());
    PredicatedUpdateRecordSimplifiedResponse response = (PredicatedUpdateRecordSimplifiedResponse) activeEntity.invokeActive(
        createContext(clientDescriptor), new PredicatedUpdateRecordMessage<>(stableClientId, key, predicate, updateOperation, false));
    return response.isUpdated();
  }

  private Optional<Tuple<Long, Long>> predicatedUpdateRecordReturnTuple(DatasetActiveEntity<Long> activeEntity, long key, IntrinsicPredicate<? super Record<Long>> predicate) throws EntityUserException {
    IntrinsicUpdateOperation<Long> updateOperation = (IntrinsicUpdateOperation<Long>) Functions.installUpdateOperation(emptySet());
    PredicatedUpdateRecordFullResponse<?> response = (PredicatedUpdateRecordFullResponse) activeEntity.invokeActive(
        createContext(clientDescriptor), new PredicatedUpdateRecordMessage<>(stableClientId, key, predicate, updateOperation, true));
    if (response.getBefore() == null) {
      return Optional.empty();
    } else {
      return Optional.of(Tuple.of(response.getBefore().getMsn(), response.getAfter().getMsn()));
    }
  }

  private boolean predicatedDeleteRecord(DatasetActiveEntity<Long> activeEntity, long key, IntrinsicPredicate<? super Record<Long>> predicate) throws EntityUserException {
    PredicatedDeleteRecordSimplifiedResponse response = (PredicatedDeleteRecordSimplifiedResponse) activeEntity.invokeActive(
        createContext(clientDescriptor), new PredicatedDeleteRecordMessage<>(stableClientId, key, predicate, false));
    return response.isDeleted();
  }

  private RecordData<?> predicatedDeleteRecordReturnData(DatasetActiveEntity<Long> activeEntity, long key, IntrinsicPredicate<? super Record<Long>> predicate) throws EntityUserException {
    PredicatedDeleteRecordFullResponse<?> response = (PredicatedDeleteRecordFullResponse) activeEntity.invokeActive(
        createContext(clientDescriptor), new PredicatedDeleteRecordMessage<>(stableClientId, key, predicate, true));
    return response.getDeleted();
  }

  private boolean recordExists(DatasetActiveEntity<Long> activeEntity, long key, IntrinsicPredicate<? super Record<Long>> predicate) throws Exception {
    GetRecordResponse<Long> response = getRecord(activeEntity, key, predicate);
    return response.getData() != null;
  }

  private boolean recordExists(DatasetActiveEntity<Long> activeEntity, long key) throws Exception {
    GetRecordResponse<?> response = getRecord(activeEntity, key,  alwaysTrue());
    return response.getData() != null;
  }

  private long recordMsn(DatasetActiveEntity<Long> activeEntity, long key) throws Exception {
    GetRecordResponse<Long> response = getRecord(activeEntity, key);
    return response.getData().getMsn();
  }

  private GetRecordResponse<Long> getRecord(DatasetActiveEntity<Long> activeEntity, long key) throws Exception {
    return getRecord(activeEntity, key,  alwaysTrue());
  }

  private GetRecordResponse<Long> getRecord(DatasetActiveEntity<Long> activeEntity, long key, IntrinsicPredicate<? super Record<Long>> predicate) throws Exception {
    GetRecordResponse<Long> response = (GetRecordResponse<Long>) activeEntity.invokeActive(createContext(
            clientDescriptor), new GetRecordMessage<>(key, predicate));
    DatasetOperationMessageCodec codec = new DatasetOperationMessageCodec(new IntrinsicCodec(ServerIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS));
    byte[] encoding = codec.encodeResponse(response);
    return (GetRecordResponse<Long>) codec.decodeResponse(encoding);
  }

  @FunctionalInterface
  private interface LockableRecordConsumer<K extends Comparable<K>> {
    void accept(DatasetActiveEntity<K> entity, K key) throws RecordLockedException, EntityUserException;
  }
}
