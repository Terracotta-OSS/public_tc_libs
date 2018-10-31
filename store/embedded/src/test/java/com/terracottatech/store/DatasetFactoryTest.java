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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.MemorySpace;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.ShardedRecordContainer;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.persistence.AbstractStorage;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.terracottatech.sovereign.indexing.SovereignIndexSettings.btree;
import static com.terracottatech.store.indexing.IndexSettings.BTREE;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class DatasetFactoryTest {

  @SuppressWarnings({"rawtypes"})
  @Test
  public void createDataset() throws Exception {
    StorageFactory storageFactory = mock(StorageFactory.class);
    DatasetFactory datasetFactory = new DatasetFactory(storageFactory);

    CellDefinition<?> cellDef1 = CellDefinition.defineString("cell1");
    Map<CellDefinition<?>, IndexSettings> inputIndexes = new HashMap<>();
    inputIndexes.put(cellDef1, BTREE);
    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap", "disk", inputIndexes);

    // Set up a big chain of mocks to keep Sovereign happy - big because of the index code path
    AbstractStorage storage = mock(AbstractStorage.class);
    MemorySpace space = mock(MemorySpace.class);
    ShardedRecordContainer container = mock(ShardedRecordContainer.class);
    try (ContextImpl context = new ContextImpl(null, false)) {
      PersistentMemoryLocator locator = mock(PersistentMemoryLocator.class);
      when(storageFactory.getStorage(configuration)).thenReturn((SovereignStorage) storage);
      when(storage.makeSpace(any(SovereignRuntime.class))).thenReturn(space);
      when(space.createContainer()).thenReturn(container);
      when(container.start(true)).thenReturn(context);
      when(container.first(context)).thenReturn(locator);
      when(locator.isEndpoint()).thenReturn(true);

      SovereignDataset<Long> dataset1 = datasetFactory.create("dataset1", Type.LONG, configuration);

      assertEquals("dataset1", dataset1.getAlias());
      assertEquals(Type.LONG, dataset1.getType());
      SovereignIndexing indexing = dataset1.getIndexing();
      List<SovereignIndex<?>> indexes = indexing.getIndexes();
      assertEquals(1, indexes.size());
      SovereignIndex<?> index = indexes.get(0);
      assertEquals(cellDef1, index.on());
      assertEquals(btree(), index.definition());
    }
  }

  @Test
  public void destroyDataset() throws Exception {
    StorageFactory storageFactory = mock(StorageFactory.class);
    DatasetFactory datasetFactory = new DatasetFactory(storageFactory);

    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap", "disk", emptyMap());

    // Set up a chain of mocks to keep Sovereign happy
    AbstractStorage storage = mock(AbstractStorage.class);
    MemorySpace space = mock(MemorySpace.class);
    when(storageFactory.getStorage(configuration)).thenReturn((SovereignStorage) storage);
    when(storage.makeSpace(any(SovereignRuntime.class))).thenReturn(space);

    SovereignDataset<Long> dataset1 = datasetFactory.create("dataset1", Type.LONG, configuration);
    UUID uuid = dataset1.getUUID();

    datasetFactory.destroy(dataset1);

    verify(storage).destroyDataSet(uuid);
  }

  @Test
  public void lastDatasetDestroyShutsdownStorageFactory() throws Exception {
    StorageFactory storageFactory = mock(StorageFactory.class);
    DatasetFactory datasetFactory = new DatasetFactory(storageFactory);

    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap", "disk", emptyMap());

    // Set up a chain of mocks to keep Sovereign happy
    AbstractStorage storage = mock(AbstractStorage.class);
    MemorySpace space = mock(MemorySpace.class);
    when(storageFactory.getStorage(configuration)).thenReturn((SovereignStorage) storage);
    when(storage.makeSpace(any(SovereignRuntime.class))).thenReturn(space);

    SovereignDataset<Long> dataset1 = datasetFactory.create("dataset1", Type.LONG, configuration);
    SovereignDataset<Long> dataset2 = datasetFactory.create("dataset2", Type.LONG, configuration);

    datasetFactory.destroy(dataset1);
    String storageKey = StorageFactory.buildStorageKey(configuration);
    verify(storageFactory, never()).shutdownStorage(eq(storageKey), any(SovereignStorage.class));

    datasetFactory.destroy(dataset2);
    verify(storageFactory).shutdownStorage(eq(storageKey), any(SovereignStorage.class));
  }

  @Test
  public void closeClosesStorageFactory() {
    StorageFactory storageFactory = mock(StorageFactory.class);
    DatasetFactory datasetFactory = new DatasetFactory(storageFactory);

    datasetFactory.close();
    verify(storageFactory).close();
    verifyNoMoreInteractions(storageFactory);
  }
}
