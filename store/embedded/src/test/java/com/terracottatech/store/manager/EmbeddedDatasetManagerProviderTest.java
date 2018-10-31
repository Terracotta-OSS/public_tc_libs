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

package com.terracottatech.store.manager;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfigurationBuilder;

import java.nio.file.Path;
import java.util.Set;

import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.NEW;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EmbeddedDatasetManagerProviderTest {
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private EmbeddedDatasetManagerProvider provider = new EmbeddedDatasetManagerProvider();

  @Test
  public void isEmbedded() {
    Set<DatasetManagerProvider.Type> supportedTypes = provider.getSupportedTypes();
    assertThat(supportedTypes.contains(DatasetManagerProvider.Type.EMBEDDED), is(true));
    assertNotNull(provider.embedded());
  }

  @Test(expected = IllegalStateException.class)
  public void mustSpecifyAnOffheapResource() throws Exception {
    EmbeddedDatasetManagerConfiguration datasetManagerConfiguration =
        new EmbeddedDatasetManagerConfigurationBuilder().build();
    provider.using(datasetManagerConfiguration, ConfigurationMode.VALIDATE);
  }

  @Test
  public void withOffheapResource() throws Exception {
    try (DatasetManager datasetManager = provider.embedded()
        .offheap("offheap", 10, MemoryUnit.MB)
        .build()) {
      assertNotNull(datasetManager);
    }
  }

  @Test
  public void withDiskResource() throws Exception {
    Path dataRoot = testFolder.newFolder("tc-store-embedded-test").toPath();

      try (DatasetManager datasetManager = provider.embedded()
          .offheap("offheap", 20, MemoryUnit.MB)
          .disk("disk", dataRoot, INMEMORY, NEW)
          .build()) {
        DatasetConfiguration configuration = datasetManager.datasetConfiguration()
                                                           .offheap("offheap")
                                                           .disk("disk")
                                                           .build();
        assertThat(datasetManager.newDataset("dataset1", LONG, configuration), is(true));
        try (Dataset<Long> dataset1 = datasetManager.getDataset("dataset1", LONG)) {
          DatasetWriterReader<Long> writer = dataset1.writerReader();
          boolean added1 = writer.add(1L);
          assertTrue(added1);
        }
      }

      try (DatasetManager datasetManager = provider.embedded()
          .offheap("offheap", 20, MemoryUnit.MB)
          .disk("disk", dataRoot, INMEMORY, REOPEN)
          .build()) {
        try (Dataset<Long> dataset1 = datasetManager.getDataset("dataset1", LONG)) {
          DatasetWriterReader<Long> writer = dataset1.writerReader();
          boolean added1 = writer.add(1L);
          assertFalse(added1);
        }

        DatasetConfiguration configuration = datasetManager.datasetConfiguration()
                                                           .offheap("offheap")
                                                           .disk("disk")
                                                           .build();

        assertThat(datasetManager.newDataset("dataset2", LONG, configuration), is(true));
        try (Dataset<Long> dataset2 = datasetManager.getDataset("dataset2", LONG)) {
          DatasetWriterReader<Long> writer = dataset2.writerReader();
          boolean added1 = writer.add(1L);
          assertTrue(added1);
        }
      }
  }

}
