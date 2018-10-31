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
package com.terracottatech.store.wrapper;

import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.NEW;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FileModeIT {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void newFileModeSuccessWithEmptyDirectory() throws Exception {
    Path dataRoot = temporaryFolder.newFolder("file-mode-it-empty").toPath();
    assertTrue(Files.exists(dataRoot));

    try (DatasetManager datasetManager = DatasetManager.embedded()
            .offheap("offheap", 100, MemoryUnit.MB)
            .disk("disk", dataRoot, INMEMORY, NEW)
            .build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
              .offheap("offheap")
              .disk("disk")
              .build();

      assertThat(datasetManager.newDataset("address", Type.INT, datasetConfiguration), is(true));
    }
  }

  @Test
  public void newFileModeSuccessWithUnknownDirectory() throws Exception {
    Path dataRoot = temporaryFolder.newFolder("file-mode-it-unknown").toPath();
    Files.delete(dataRoot);

    try (DatasetManager datasetManager = DatasetManager.embedded()
            .offheap("offheap", 100, MemoryUnit.MB)
            .disk("disk", dataRoot, INMEMORY, NEW)
            .build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
              .offheap("offheap")
              .disk("disk")
              .build();

      assertThat(datasetManager.newDataset("address", Type.INT, datasetConfiguration), is(true));
    }
  }

  @SuppressWarnings("try")
  @Test
  public void newFileModeThrowsWithAFileInTheDirectory() throws Exception {
    Path dataRoot = temporaryFolder.newFolder("file-mode-it-throws").toPath();
    Files.createDirectory(dataRoot.resolve("meta"));
    Files.createFile(dataRoot.resolve("meta").resolve("blocker.txt"));

    exception.expect(StoreException.class);
    try (DatasetManager datasetManager = DatasetManager.embedded()
            .offheap("offheap", 100, MemoryUnit.MB)
            .disk("disk", dataRoot, INMEMORY, NEW)
            .build()) {
    }
  }
}
