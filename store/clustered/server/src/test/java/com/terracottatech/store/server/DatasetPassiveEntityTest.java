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

import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.ErrorResponse;
import com.terracottatech.store.common.messages.indexing.IndexCreateMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexCreateStatusResponse;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.common.reconnect.ReconnectCodec;
import com.terracottatech.store.common.reconnect.ReconnectState;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.server.management.DatasetManagement;
import com.terracottatech.store.server.messages.replication.MetadataSyncMessage;
import com.terracottatech.store.server.state.DatasetEntityStateService;
import com.terracottatech.store.server.sync.DatasetLoader;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.ExplicitRetirementHandle;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.MessageCodecException;

import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.storage.factory.StorageFactory;
import org.terracotta.platform.ServerInfo;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class DatasetPassiveEntityTest {

  @Test
  public void testInvokeContextRecreation() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);

    StorageFactory storageFactory = new MyStorageFactory();
    DatasetEntityStateService<Long> stateService = new DatasetEntityStateService<>(configuration, null, storageFactory, null);

    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = mock(OOOMessageHandler.class);
    DatasetPassiveEntity<Long> dataset = new DatasetPassiveEntity<>(stateService, mock(DatasetManagement.class), messageHandler);
    dataset.createNew();

    long clientId = 5L;
    long currentTransactionId = 9L;
    long oldestTransactionId = 8L;
    CRUDDataReplicationMessage message = new CRUDDataReplicationMessage(1L, mock(ByteBuffer.class), true, clientId, currentTransactionId, oldestTransactionId);

    InvokeContext context = mock(InvokeContext.class);
    ClientSourceId clientSourceId = mock(ClientSourceId.class);
    when(clientSourceId.toLong()).thenReturn(clientId);
    when(context.makeClientSourceId(clientId)).thenReturn(clientSourceId);

    dataset.invokePassive(context, message);

    ArgumentCaptor<InvokeContext> contextCaptor = ArgumentCaptor.forClass(InvokeContext.class);
    ArgumentCaptor<CRUDDataReplicationMessage> messageCaptor = ArgumentCaptor.forClass(CRUDDataReplicationMessage.class);
    verify(messageHandler).invoke(contextCaptor.capture(), messageCaptor.capture(), any(BiFunction.class));

    assertThat(messageCaptor.getValue(), sameInstance(message));
    InvokeContext capturedContext = contextCaptor.getValue();
    assertThat(capturedContext.getClientSource().toLong(), is(clientId));
    assertThat(capturedContext.getCurrentTransactionId(), is(currentTransactionId));
    assertThat(capturedContext.getOldestTransactionId(), is(oldestTransactionId));
  }

  @Test
  public void testExactlyOnceInvocationOnFailover() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);

    StorageFactory storageFactory = new MyStorageFactory();
    DatasetEntityStateService<Long> stateService = new DatasetEntityStateService<>(configuration, null, storageFactory, null);

    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = mock(OOOMessageHandler.class);
    DatasetPassiveEntity<Long> dataset = new DatasetPassiveEntity<>(stateService, mock(DatasetManagement.class), messageHandler);
    dataset.createNew();

    long clientId = 5L;
    long currentTransactionId = 9L;
    long oldestTransactionId = 8L;
    CRUDDataReplicationMessage message = new CRUDDataReplicationMessage(1L, mock(ByteBuffer.class), true, clientId, currentTransactionId, oldestTransactionId);

    InvokeContext context = mock(InvokeContext.class);
    ClientSourceId clientSourceId = mock(ClientSourceId.class);
    when(clientSourceId.toLong()).thenReturn(clientId);
    when(context.makeClientSourceId(clientId)).thenReturn(clientSourceId);

    dataset.invokePassive(context, message);

    ArgumentCaptor<InvokeContext> contextCaptor = ArgumentCaptor.forClass(InvokeContext.class);
    ArgumentCaptor<CRUDDataReplicationMessage> messageCaptor = ArgumentCaptor.forClass(CRUDDataReplicationMessage.class);
    verify(messageHandler).invoke(contextCaptor.capture(), messageCaptor.capture(), any(BiFunction.class));

    assertThat(messageCaptor.getValue(), sameInstance(message));
    InvokeContext capturedContext = contextCaptor.getValue();
    assertThat(capturedContext.getClientSource().toLong(), sameInstance(clientId));
    assertThat(capturedContext.getCurrentTransactionId(), is(currentTransactionId));
    assertThat(capturedContext.getOldestTransactionId(), is(oldestTransactionId));
  }

  @Test
  public void testDisconnectUntracksClient() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);

    StorageFactory storageFactory = new MyStorageFactory();
    DatasetEntityStateService<Long> stateService = new DatasetEntityStateService<>(configuration, null, storageFactory, null);

    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = mock(OOOMessageHandler.class);
    DatasetPassiveEntity<Long> passiveEntity = new DatasetPassiveEntity<>(stateService, mock(DatasetManagement.class), messageHandler);
    passiveEntity.createNew();

    ClientSourceId clientSourceId = mock(ClientSourceId.class);

    passiveEntity.notifyDestroyed(clientSourceId);
    verify(messageHandler).untrackClient(clientSourceId);
  }

  @Test
  public void testFullyPortableMutativeStreamResponseAfterFailover() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);

    StorageFactory storageFactory = new MyStorageFactory();
    DatasetEntityStateService<Long> stateService = new DatasetEntityStateService<>(configuration, null, storageFactory, null);

    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = new RecordingMessageHandler();
    DatasetPassiveEntity<Long> passiveEntity = new DatasetPassiveEntity<>(stateService, mock(DatasetManagement.class), messageHandler);
    passiveEntity.createNew();

    long clientId = 5L;

    InvokeContext context = mock(InvokeContext.class);
    ClientSourceId clientSourceId = mock(ClientSourceId.class);
    when(clientSourceId.toLong()).thenReturn(clientId);
    when(context.makeClientSourceId(clientId)).thenReturn(clientSourceId);
    when(context.getCurrentTransactionId()).thenReturn(8L);
    when(context.getOldestTransactionId()).thenReturn(8L);
    when(context.isValidClientInformation()).thenReturn(true);

    ExecuteTerminatedPipelineMessage message = mock(ExecuteTerminatedPipelineMessage.class);
    when(message.getType()).thenReturn(DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE);
    when(message.isMutative()).thenReturn(true);

    passiveEntity.invokePassive(context, message);

    ServerInfo serverInfo = new ServerInfo("serverName");
    DatasetActiveEntity<Long> datasetActiveEntity = new DatasetActiveEntity<>(null, null, serverInfo,
            messageHandler, stateService, null, null, null);

    ActiveInvokeContext<DatasetEntityResponse> activeInvokeContext = mock(ActiveInvokeContext.class);
    when(clientSourceId.toLong()).thenReturn(clientId);
    when(activeInvokeContext.makeClientSourceId(clientId)).thenReturn(clientSourceId);
    when(activeInvokeContext.getCurrentTransactionId()).thenReturn(8L);
    when(activeInvokeContext.getOldestTransactionId()).thenReturn(8L);
    when(activeInvokeContext.isValidClientInformation()).thenReturn(true);

    DatasetEntityResponse entityResponse = datasetActiveEntity.invokeActive(activeInvokeContext, message);

    assertThat(entityResponse, instanceOf(ErrorResponse.class));
    assertThat(((ErrorResponse)entityResponse).getCause().getMessage(),
            containsString("Stream execution was prematurely terminated due to an active server failover"));

  }

  @Test
  public void testIndexCreationFailoverBehavior() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);

    CountDownLatch indexingRelease = new CountDownLatch(1);

    StorageFactory storageFactory = new MyStorageFactory();
    ExecutorService indexingExecutor = Executors.newSingleThreadExecutor(r -> new Thread(() -> {
      try {
        indexingRelease.await();
      } catch (InterruptedException e) {
        //ignore
      } finally {
        r.run();
      }
    }));
    try {
      DatasetEntityStateService<Long> stateService = new DatasetEntityStateService<>(configuration, null, storageFactory, indexingExecutor);

      OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = new RecordingMessageHandler();
      DatasetPassiveEntity<Long> passiveEntity = new DatasetPassiveEntity<>(stateService, mock(DatasetManagement.class), messageHandler);

      passiveEntity.createNew();

      UUID stableClientId = UUID.randomUUID();

      CellDefinition<Integer> indexedCell = CellDefinition.defineInt("indexedCell");
      IndexSettings indexSettings = IndexSettings.btree();
      passiveEntity.invokePassive(passiveContext(42L), new IndexCreateMessage<>(indexedCell, indexSettings, stableClientId));

      CountDownLatch retirementRelease = new CountDownLatch(1);
      IEntityMessenger<EntityMessage, EntityResponse> entityMessenger = mock(IEntityMessenger.class);
      when(entityMessenger.deferRetirement(anyString(), any(EntityMessage.class), any(EntityMessage.class))).thenAnswer(invocation -> new ExplicitRetirementHandle<EntityResponse>() {
        @Override
        public String getTag() {
          return "fake";
        }

        @Override
        public void release() throws MessageCodecException {
          retirementRelease.countDown();
        }

        @Override
        public void release(Consumer<IEntityMessenger.MessageResponse<EntityResponse>> consumer) throws MessageCodecException {

        }
      });
      ServerInfo serverInfo = new ServerInfo("serverName");
      DatasetActiveEntity<Long> datasetActiveEntity = new DatasetActiveEntity<>(null, entityMessenger, serverInfo, messageHandler, stateService, null, mock(DatasetManagement.class), null);
      datasetActiveEntity.loadExisting();
      ClientDescriptor clientDescriptor = descriptor();
      datasetActiveEntity.connected(clientDescriptor);
      ActiveServerEntity.ReconnectHandler reconnectHandler = datasetActiveEntity.startReconnect();
      reconnectHandler.handleReconnect(clientDescriptor, new ReconnectCodec().encode(new ReconnectState(false, stableClientId)));

      String creationRequestId = configuration.getDatasetName() + '[' + indexSettings + ':' + indexedCell + ']';
      IndexCreateStatusResponse firstResponse = (IndexCreateStatusResponse) datasetActiveEntity.invokeActive(activeContext(clientDescriptor), new IndexCreateStatusMessage(creationRequestId, stableClientId));

      Assert.assertThat(firstResponse.getFault(), Matchers.nullValue());
      Assert.assertThat(firstResponse.getStatus(), Matchers.nullValue());
      Assert.assertThat(firstResponse.isRetry(), Matchers.is(true));

      indexingRelease.countDown();
      retirementRelease.await();

      IndexCreateStatusResponse secondResponse = (IndexCreateStatusResponse) datasetActiveEntity.invokeActive(activeContext(clientDescriptor), new IndexCreateStatusMessage(creationRequestId, stableClientId));

      Assert.assertThat(secondResponse.getFault(), Matchers.nullValue());
      Assert.assertThat(secondResponse.getStatus(), Matchers.is(Index.Status.LIVE));
    } finally {
      indexingExecutor.shutdown();
    }
  }

  @Test
  public void testIndexCreationDuringSync() throws Exception {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", null, emptyMap());
    DatasetEntityConfiguration<Long> configuration = new DatasetEntityConfiguration<>(Type.LONG, "address", datasetConfiguration);

    StorageFactory storageFactory = new MyStorageFactory();
    ExecutorService indexingExecutor = mock(ExecutorService.class);

    DatasetEntityStateService<Long> stateService = new DatasetEntityStateService<>(configuration, null, storageFactory, indexingExecutor);

    OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> messageHandler = new RecordingMessageHandler();
    DatasetPassiveEntity<?> datasetPassiveEntity = new DatasetPassiveEntity<>(stateService, mock(DatasetManagement.class), messageHandler);

    datasetPassiveEntity.createNew();
    datasetPassiveEntity.startSyncEntity();

    DatasetLoader<?> datasetLoader = datasetPassiveEntity.getDatasetLoader();

    assertThat(datasetLoader, notNullValue());

    SovereignDatasetDescriptionImpl<?, ?> description = mock(SovereignDatasetDescriptionImpl.class);

    SimpleIndexDescription<Integer> indexDescription = mock(SimpleIndexDescription.class);
    when(indexDescription.getCellDefinition()).thenReturn(CellDefinition.defineInt("test"));
    when(indexDescription.getIndexSettings()).thenReturn(SovereignIndexSettings.BTREE);
    when(description.getIndexDescriptions()).thenReturn(Collections.singletonList(indexDescription));

    assertThat(stateService.getDataset().getIndexing().getIndexes().size(), is(0));

    datasetPassiveEntity.invokePassive(passiveContext(42L), new MetadataSyncMessage(description, MetadataKey.Tag.DATASET_DESCR.ordinal()));

    assertThat(stateService.getDataset().getIndexing().getIndexes().size(), is(1));

    UUID stableClientId = UUID.randomUUID();

    datasetPassiveEntity.invokePassive(passiveContext(42L), new IndexCreateMessage<>(CellDefinition.defineString("testIdx"),
            IndexSettings.BTREE, stableClientId));

    assertThat(datasetLoader.getPendingIndexDescriptions().size(), is(1));

    datasetPassiveEntity.endSyncEntity();
    assertThat(stateService.getDataset().getIndexing().getIndexes().size(), is(2));

  }

  private static ClientDescriptor descriptor() {
    ClientDescriptor descriptor = mock(ClientDescriptor.class);
    return descriptor;
  }

  private static ActiveInvokeContext<DatasetEntityResponse> activeContext(ClientDescriptor descriptor) {
    ActiveInvokeContext<DatasetEntityResponse> context = mock(ActiveInvokeContext.class);
    when(context.isValidClientInformation()).thenReturn(true);
    when(context.getClientDescriptor()).thenReturn(descriptor);
    return context;
  }

  private static InvokeContext passiveContext(long clientSourceId) {
    ClientSourceId clientSource = mock(ClientSourceId.class);
    when(clientSource.toLong()).thenReturn(clientSourceId);
    InvokeContext context = mock(InvokeContext.class);
    when(context.getClientSource()).thenReturn(clientSource);
    return context;
  }
}