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
package com.terracottatech.store.server.storage.configuration;

import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import org.junit.Test;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;

public class StorageConfigurationFactoryTest {
  @Test
  public void createOffheapConfiguration() {
    StorageConfiguration storageConfiguration = StorageConfigurationFactory.create(new DatasetEntityConfiguration<>(Type.LONG, "name", new ClusteredDatasetConfiguration("offheap", null, emptyMap())));
    assertEquals(StorageType.MEMORY, storageConfiguration.getStorageType());
    MemoryStorageConfiguration memoryStorageConfiguration = (MemoryStorageConfiguration) storageConfiguration;
    assertEquals("offheap", memoryStorageConfiguration.getOffheapResource());
  }

  @Test
  public void createFRSConfiguration() {
    StorageConfiguration storageConfiguration = StorageConfigurationFactory.create(new DatasetEntityConfiguration<>(Type.LONG, "name", new ClusteredDatasetConfiguration("offheap", "disk", emptyMap())));
    assertEquals(StorageType.FRS, storageConfiguration.getStorageType());
    PersistentStorageConfiguration persistentStorageConfiguration = (PersistentStorageConfiguration) storageConfiguration;
    assertEquals("offheap", persistentStorageConfiguration.getOffheapResource());
    assertEquals("disk", persistentStorageConfiguration.getDiskResource());
  }
}
