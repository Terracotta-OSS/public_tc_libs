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
package com.terracottatech.store.builder;

import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilderImpl;
import com.terracottatech.store.manager.EmbeddedDatasetManagerProvider;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class EmbeddedDatasetManagerBuilderImplTest {

  private EmbeddedDatasetManagerProvider provider = mock(EmbeddedDatasetManagerProvider.class);
  private EmbeddedDatasetManagerBuilder baseBuilder = new EmbeddedDatasetManagerBuilderImpl(provider);

  @Test
  public void withOffheapResource() throws Exception {
    final String offheapResourceName = "offheap";
    final Long offheapResourceSize = (long)10 * 1024 * 1024;
    EmbeddedDatasetManagerConfiguration datasetManagerConfiguration =
        captureConfiguration(baseBuilder.offheap(offheapResourceName, offheapResourceSize, MemoryUnit.B));
    Map<String, Long> offheapResources = datasetManagerConfiguration.getResourceConfiguration().getOffheapResources();
    assertThat(offheapResources.containsKey(offheapResourceName), is(true));
    assertThat(offheapResources.get(offheapResourceName), is(offheapResourceSize));
    assertThat(datasetManagerConfiguration.getResourceConfiguration().getDiskResources().size(), is(0));
  }

  @Test
  public void withDiskResource() throws Exception {
    final String diskResourceName = "disk";
    final Path dataRoot = Files.createTempDirectory("tc-store-embedded-test");
    final EmbeddedDatasetManagerBuilder.FileMode fileMode = EmbeddedDatasetManagerBuilder.FileMode.NEW;
    final EmbeddedDatasetManagerBuilder.PersistenceMode persistenceMode =
        EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY;
    EmbeddedDatasetManagerConfiguration datasetManagerConfiguration =
        captureConfiguration(baseBuilder.offheap("offheap", 10, MemoryUnit.MB)
                                        .disk(diskResourceName, dataRoot, persistenceMode, fileMode));
    Map<String, DiskResource> diskResources = datasetManagerConfiguration.getResourceConfiguration().getDiskResources();
    assertThat(diskResources.size(), is(1));
    assertThat(diskResources.containsKey(diskResourceName), is(true));
    DiskResource diskResource = diskResources.get(diskResourceName);
    assertThat(diskResource.getDataRoot(), is(dataRoot));
    assertThat(diskResource.getFileMode(), is(fileMode));
    assertThat(diskResource.getPersistenceMode(), is(PersistentStorageEngine.FRS));
  }

  @Test
  public void withDiskResourceNewStyleApi() throws Exception {
    final String diskResourceName = "disk";
    final Path dataRoot = Files.createTempDirectory("tc-store-embedded-test");
    final EmbeddedDatasetManagerBuilder.FileMode fileMode = EmbeddedDatasetManagerBuilder.FileMode.NEW;
    EmbeddedDatasetManagerConfiguration datasetManagerConfiguration =
        captureConfiguration(baseBuilder.offheap("offheap", 10, MemoryUnit.MB)
            .disk(diskResourceName, dataRoot, fileMode));
    Map<String, DiskResource> diskResources = datasetManagerConfiguration.getResourceConfiguration().getDiskResources();
    assertThat(diskResources.size(), is(1));
    assertThat(diskResources.containsKey(diskResourceName), is(true));
    DiskResource diskResource = diskResources.get(diskResourceName);
    assertThat(diskResource.getDataRoot(), is(dataRoot));
    assertThat(diskResource.getFileMode(), is(fileMode));
    assertNull(diskResource.getPersistenceMode());
  }

  private EmbeddedDatasetManagerConfiguration captureConfiguration(EmbeddedDatasetManagerBuilder builder) throws Exception {
    ArgumentCaptor<EmbeddedDatasetManagerConfiguration> configurationArgumentCaptor =
        ArgumentCaptor.forClass(EmbeddedDatasetManagerConfiguration.class);
    builder.build();
    verify(provider).using(configurationArgumentCaptor.capture(), any());
    return configurationArgumentCaptor.getValue();
  }
}
