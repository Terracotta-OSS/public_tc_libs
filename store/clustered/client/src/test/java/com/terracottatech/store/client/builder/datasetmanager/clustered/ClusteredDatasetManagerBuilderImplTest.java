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

package com.terracottatech.store.client.builder.datasetmanager.clustered;

import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.ClusteredDatasetManagerProvider;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.config.ClusteredDatasetManagerConfiguration;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ClusteredDatasetManagerBuilderImplTest {

  private ClusteredDatasetManagerProvider provider = mock(ClusteredDatasetManagerProvider.class);
  private ClusteredDatasetManagerBuilder baseBuilder =
      new ClusteredDatasetManagerBuilderImpl(provider, URI.create("mock://example.com"));


  @Test
  public void testDefaultConnectionTimeout() throws Exception {
    ClusteredDatasetManagerConfiguration datasetManagerConfiguration = captureConfiguration(baseBuilder);
    assertThat(datasetManagerConfiguration.getClientSideConfiguration().getConnectTimeout(), is(ClusteredDatasetManagerBuilder.DEFAULT_CONNECTION_TIMEOUT_MS));
  }

  @Test
  public void testNonDefaultConnectionTimeout() throws Exception {
    ClusteredDatasetManagerConfiguration datasetManagerConfiguration =
        captureConfiguration(baseBuilder.withConnectionTimeout(5, TimeUnit.SECONDS));
    assertThat(datasetManagerConfiguration.getClientSideConfiguration().getConnectTimeout(), is(TimeUnit.SECONDS.toMillis(5)));
  }

  @Test
  public void testDefaultReconnectTimeout() throws Exception {
    ClusteredDatasetManagerConfiguration datasetManagerConfiguration = captureConfiguration(baseBuilder);
    assertThat(datasetManagerConfiguration.getClientSideConfiguration().getReconnectTimeout(), is(ClusteredDatasetManagerBuilder.DEFAULT_RECONNECT_TIMEOUT_MS));
  }

  @Test
  public void testNonDefaultReconnectTimeout() throws Exception {
    ClusteredDatasetManagerConfiguration datasetManagerConfiguration =
        captureConfiguration(baseBuilder.withReconnectTimeout(5, TimeUnit.SECONDS));
    assertThat(datasetManagerConfiguration.getClientSideConfiguration().getReconnectTimeout(), is(TimeUnit.SECONDS.toMillis(5)));
  }

  private ClusteredDatasetManagerConfiguration captureConfiguration(ClusteredDatasetManagerBuilder clusteredDatasetManagerBuilder) throws Exception {
    ArgumentCaptor<DatasetManagerConfiguration> configurationArgumentCaptor
        = ArgumentCaptor.forClass(DatasetManagerConfiguration.class);
    clusteredDatasetManagerBuilder.build();
    verify(provider).using(configurationArgumentCaptor.capture(), any());
    DatasetManagerConfiguration capturedConfiguration = configurationArgumentCaptor.getValue();
    assertThat(capturedConfiguration, instanceOf(ClusteredDatasetManagerConfiguration.class));
    return (ClusteredDatasetManagerConfiguration)capturedConfiguration;
  }
}
