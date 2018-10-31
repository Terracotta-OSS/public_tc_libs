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
package com.terracottatech.store;

import com.terracottatech.connection.BasicMultiConnection;
import com.terracottatech.store.client.ConnectionProperties;
import com.terracottatech.store.client.management.ClusteredManageableDatasetManager;
import com.terracottatech.store.client.reconnectable.ReconnectableLeasedConnection;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.ClusteredDatasetManagerProvider;
import com.terracottatech.store.manager.ConfigurationMode;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfigurationBuilder;

import org.junit.Test;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.management.entity.nms.agent.client.NmsAgentEntity;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.voltron.proxy.MessageListener;
import org.terracotta.voltron.proxy.client.EndpointListener;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusteredDatasetManagerProviderTest {
  private ClusteredDatasetManagerProvider provider = new ClusteredDatasetManagerProvider();

  @Test
  public void clusteredGivesABuilder() {
    ClusteredDatasetManagerBuilder clustered = provider.clustered(URI.create("http://localhost:8080"));
    assertNotNull(clustered);
  }

  @Test
  public void testConnection() throws Exception {
    BasicMultiConnection connection = mock(BasicMultiConnection.class);
    mockNmsAgent(connection);
    when(MockConnectionService.MOCK.connect(any(URI.class), any())).thenReturn(connection);

    Properties connectionProperties = new Properties();
    connectionProperties.setProperty(ConnectionProperties.DISABLE_RECONNECT, "true");

    ClusteredDatasetManagerConfiguration configuration =
        new ClusteredDatasetManagerConfigurationBuilder(URI.create("mock://example.com")).build();
    assertNotNull(provider.using(configuration, ConfigurationMode.VALIDATE, connectionProperties));
  }

  @Test
  public void testReconnection() throws Exception {
    BasicMultiConnection connection = mock(BasicMultiConnection.class);
    mockNmsAgent(connection);
    when(MockConnectionService.MOCK.connect(any(URI.class), any())).thenReturn(connection);

    ClusteredDatasetManagerConfiguration configuration =
        new ClusteredDatasetManagerConfigurationBuilder(URI.create("mock://example.com")).build();

    ClusteredManageableDatasetManager datasetManager = (ClusteredManageableDatasetManager)provider.using(configuration, ConfigurationMode.VALIDATE);
    assertThat(datasetManager.getConnection(), is(instanceOf(ReconnectableLeasedConnection.class)));
  }

  @SuppressWarnings("unchecked")
  private void mockNmsAgent(BasicMultiConnection connection) throws Exception {
    EntityRef<NmsAgentEntity, Object, Object> ref = mock(EntityRef.class);
    NmsAgentEntity nmsAgentEntity = new LocalNmsAgentEntity();
    when(connection.getEntityRef(NmsAgentEntity.class, 1L, "NmsAgent")).thenReturn(ref);
    when(ref.fetchEntity(null)).thenReturn(nmsAgentEntity);
  }

  /**
   * Local implementation of {@code NmsAgentEntity} to avoid issues with {@code Proxy} and {@code Mock}
   * implementing {@code equals} and {@code hashCode}.
   */
  private static class LocalNmsAgentEntity implements NmsAgentEntity {

    @Override
    public void close() {
      throw new UnsupportedOperationException("LocalNmsAgentEntity.close not implemented");
    }

    @Override
    public Future<Void> exposeManagementMetadata(Object clientDescriptor, ContextContainer contextContainer, Capability... capabilities) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> exposeTags(Object clientDescriptor, String... tags) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> answerManagementCall(Object clientDescriptor, String managementCallId, ContextualReturn<?> contextualReturn) {
      throw new UnsupportedOperationException("LocalNmsAgentEntity.answerManagementCall not implemented");
    }

    @Override
    public Future<Void> pushNotification(Object clientDescriptor, ContextualNotification notification) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> pushStatistics(Object clientDescriptor, ContextualStatistics... statistics) {
      throw new UnsupportedOperationException("LocalNmsAgentEntity.pushStatistics not implemented");
    }

    @Override
    public void setEndpointListener(EndpointListener endpointListener) {
    }

    @Override
    public <T> void registerMessageListener(Class<T> type, MessageListener<T> listener) {
    }
  }
}
