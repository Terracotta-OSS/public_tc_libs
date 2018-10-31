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

import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.PersistenceMode;
import com.terracottatech.store.configuration.MemoryUnit;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Optional;

import static com.terracottatech.store.configuration.MemoryUnit.MB;
import static com.terracottatech.store.manager.XmlConfiguration.parseDatasetManagerConfig;

public class LifecycleTest {
  @Ignore("This test only checks that the code compiles. It can't run without an implementation.")
  @Test
  public void createClusteredMemoryDataset() throws Exception {
    DatasetManager datasetManager = DatasetManager.clustered(URI.create("terracotta://localhost:1234")).build();
    boolean created = datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("resource").build());
  }

  @Ignore("This test only checks that the code compiles. It can't run without an implementation.")
  @Test
  public void createClusteredOnDiskDataset() throws Exception {
    DatasetManager datasetManager = DatasetManager.clustered(URI.create("terracotta://localhost:1234")).build();
    boolean created = datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().disk("resource").build());
  }

  @Ignore("This test only checks that the code compiles. It can't run without an implementation.")
  @Test
  public void useExistingClusteredDataset() throws Exception {
    DatasetManager datasetManager = DatasetManager.clustered(URI.create("terracotta://localhost:1234")).build();
    Dataset<String> address = datasetManager.getDataset("address", Type.STRING);
    Optional<Record<String>> value = address.reader().get("key");
  }

  @Ignore("This test only checks that the code compiles. It can't run without an implementation.")
  @Test
  public void createEmbeddedMemoryDataset() throws Exception {
    DatasetManager datasetManager = DatasetManager.embedded().offheap("main-offheap", 128, MB).build();
    boolean created = datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().offheap("main-offheap").build());
  }

  @Ignore("This test only checks that the code compiles. It can't run without an implementation.")
  @Test
  public void EmbeddedPersistentDataset() throws Exception {
    DatasetManager datasetManager = DatasetManager.embedded()
            .offheap("offheap", 10L, MemoryUnit.MB)
            .disk("main-disk", Paths.get("/home/user/store"), PersistenceMode.INMEMORY, FileMode.NEW)
            .build();
    boolean created = datasetManager.newDataset("address", Type.STRING, datasetManager.datasetConfiguration().disk("main-disk").build());
  }

  @Ignore("This test only checks that the code compiles. It can't run without an implementation.")
  @Test
  public void useExistingEmbeddedDataset() throws Exception {
    DatasetManager datasetManager = DatasetManager.embedded().offheap("offheap", 10, MemoryUnit.MB).build();
    Dataset<String> address = datasetManager.getDataset("address", Type.STRING);
    Optional<Record<String>> value = address.reader().get("key");
  }

  @Ignore("This test only checks that the code compiles. It can't run without an implementation.")
  @Test
  public void createDatasetManagerWithXmlConfig() throws Exception {
    DatasetManager datasetManager = DatasetManager.using(parseDatasetManagerConfig(URI.create("file:///some/path").toURL()));
    Dataset<String> address = datasetManager.getDataset("address", Type.STRING);
    Optional<Record<String>> value = address.reader().get("key");
  }
}
