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
package com.terracottatech.store.server.storage.factory;

import org.junit.Test;

import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.server.storage.configuration.MemoryStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.PersistentStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageType;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class PolymorphicStorageFactoryTest extends BufferResourceTest {
  @Test
  public void callMemory() throws Exception {
    InternalStorageFactory memory = mock(InternalStorageFactory.class);
    InternalStorageFactory frs = mock(InternalStorageFactory.class);
    InternalStorageFactory hybrid = mock(InternalStorageFactory.class);
    StorageFactory storageFactory = new PolymorphicStorageFactory(
        buildTestMap(memory, frs, hybrid));

    StorageConfiguration configuration = new MemoryStorageConfiguration("offheap");
    storageFactory.getStorage(configuration);

    verify(memory).getStorage(configuration);
    verifyNoMoreInteractions(frs);
    verifyNoMoreInteractions(hybrid);
  }

  @Test
  public void callFRS() throws Exception {
    InternalStorageFactory memory = mock(InternalStorageFactory.class);
    InternalStorageFactory frs = mock(InternalStorageFactory.class);
    InternalStorageFactory hybrid = mock(InternalStorageFactory.class);
    StorageFactory storageFactory = new PolymorphicStorageFactory(buildTestMap(memory, frs, hybrid));

    StorageConfiguration configuration = new PersistentStorageConfiguration("abc", "offheap");
    storageFactory.getStorage(configuration);

    verifyNoMoreInteractions(memory);
    verify(frs).getStorage(configuration);
    verifyNoMoreInteractions(hybrid);
  }

  @Test
  public void callHybrid() throws Exception {
    InternalStorageFactory memory = mock(InternalStorageFactory.class);
    InternalStorageFactory frs = mock(InternalStorageFactory.class);
    InternalStorageFactory hybrid = mock(InternalStorageFactory.class);
    StorageFactory storageFactory = new PolymorphicStorageFactory(buildTestMap(memory, frs, hybrid));

    StorageConfiguration configuration = new PersistentStorageConfiguration("abc", "offheap",
        PersistentStorageEngine.HYBRID);
    storageFactory.getStorage(configuration);

    verifyNoMoreInteractions(memory);
    verifyZeroInteractions(frs);
    verify(hybrid).getStorage(configuration);
  }

  private Map<StorageType, InternalStorageFactory> buildTestMap(InternalStorageFactory memory,
                                                                 InternalStorageFactory frs,
                                                                 InternalStorageFactory hybrid) {
    Map<StorageType, InternalStorageFactory> testMap = new HashMap<>();
    testMap.put(StorageType.MEMORY, memory);
    testMap.put(StorageType.FRS, frs);
    testMap.put(StorageType.HYBRID, hybrid);
    return testMap;
  }
}
