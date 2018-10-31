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

import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.messages.ConfigurationEncoder;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.IdentifyClientMessage;
import com.terracottatech.store.common.messages.SuccessResponse;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.exception.EntityException;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.indexing.IndexSettings.btree;
import static java.util.Collections.singletonMap;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatasetEntityClientServiceTest {
  @Mock
  private EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint;

  @Mock
  private InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> invocationBuilder;

  @Captor
  private ArgumentCaptor<DatasetEntityMessage> messageCaptor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(invocationBuilder.message(messageCaptor.capture())).thenReturn(invocationBuilder);
    when(invocationBuilder.replicate(anyBoolean())).thenReturn(invocationBuilder);
    when(invocationBuilder.ackCompleted()).thenReturn(invocationBuilder);
    when(invocationBuilder.invoke()).thenAnswer(invocation -> {
      DatasetEntityMessage message = messageCaptor.getValue();
      if (message instanceof IdentifyClientMessage) {
        SuccessResponse response = new SuccessResponse();
        return new InvokeFuture<DatasetEntityResponse>() {
          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public DatasetEntityResponse get() throws InterruptedException, EntityException {
            return response;
          }

          @Override
          public DatasetEntityResponse getWithTimeout(long l, TimeUnit timeUnit)
              throws InterruptedException, EntityException, TimeoutException {
            return response;
          }

          @Override
          public void interrupt() {

          }
        };
      }
      return mock(InvokeFuture.class);
    });
    when(endpoint.beginInvoke()).thenReturn(invocationBuilder);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void handlesDatasetEntity() {
    DatasetEntityClientService service = new DatasetEntityClientService();
    assertTrue(service.handlesEntityType((Class) DatasetEntity.class));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void doesNotHandleGeneralEntityTypes() {
    EntityClientService service = new DatasetEntityClientService();
    assertFalse(service.handlesEntityType(Entity.class));
  }

  @Test
  public void serializesConfiguration() {
    checkConfigurationSerialization(Type.BOOL, "name", "A", null);
    checkConfigurationSerialization(Type.CHAR, "name", "A", null);
    checkConfigurationSerialization(Type.INT, "name", "A", null);
    checkConfigurationSerialization(Type.LONG, "name", "A", null);
    checkConfigurationSerialization(Type.DOUBLE, "name", "A", null);
    checkConfigurationSerialization(Type.STRING, "name", "A", null);
    checkConfigurationSerialization(Type.STRING, "bob", "A", null);
    checkConfigurationSerialization(Type.STRING, "bob", "A", "B");
  }

  private <K extends Comparable<K>> void checkConfigurationSerialization(Type<K> keyType, String datasetName, String offheapResource, String diskResource) {
    DatasetEntityClientService service = new DatasetEntityClientService();

    byte[] encoding = service.serializeConfiguration(new DatasetEntityConfiguration<>(keyType, datasetName, new ClusteredDatasetConfiguration(offheapResource, diskResource,
            singletonMap(defineString("foo"), btree()))));
    DatasetEntityConfiguration<?> configuration = service.deserializeConfiguration(encoding);

    assertEquals(keyType, configuration.getKeyType());
    assertEquals(datasetName, configuration.getDatasetName());
    assertEquals(offheapResource, configuration.getDatasetConfiguration().getOffheapResource());
    assertEquals(Optional.ofNullable(diskResource), configuration.getDatasetConfiguration().getDiskResource());
    assertThat(configuration.getDatasetConfiguration().getIndexes(), hasEntry(defineString("foo"), btree()));
  }

  @Test
  public void creates() {
    DatasetEntityClientService service = new DatasetEntityClientService();
    when(endpoint.getEntityConfiguration())
        .thenReturn(ConfigurationEncoder.encode(new DatasetEntityConfiguration<>(Type.STRING, "bob", new ClusteredDatasetConfiguration("A", "B", singletonMap(defineString("foo"), btree())))));
    DatasetEntity<?> entity = service.create(endpoint, null);
    assertEquals(Type.STRING, entity.getKeyType());
  }
}
