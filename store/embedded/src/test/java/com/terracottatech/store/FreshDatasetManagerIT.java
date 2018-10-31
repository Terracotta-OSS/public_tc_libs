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

import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

public class FreshDatasetManagerIT {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Path dataRoot;

  @Before
  public void before() throws Exception {
    dataRoot = temporaryFolder.newFolder().toPath();
  }

  @Test
  public void destroyFromFreshDatasetManager() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
            .offheap("offheap", 50, MemoryUnit.MB)
            .disk("disk", dataRoot, EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY, EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
            .build()) {
      EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration()
              .offheap("offheap")
              .disk("disk")
              .build();
      datasetManager.newDataset("dataset", Type.LONG, configuration);
    }

    try (DatasetManager datasetManager = DatasetManager.embedded()
            .offheap("offheap", 50, MemoryUnit.MB)
            .disk("disk", dataRoot, EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY, EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
            .build()) {
      datasetManager.destroyDataset("dataset");
    }
  }
}
