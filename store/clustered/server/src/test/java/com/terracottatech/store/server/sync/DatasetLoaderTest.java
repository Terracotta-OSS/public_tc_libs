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
package com.terracottatech.store.server.sync;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.AbstractRecordContainer;
import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.ShardedRecordContainer;
import com.terracottatech.sovereign.impl.model.SovereignPrimaryMap;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.server.RawDataset;
import com.terracottatech.store.server.messages.replication.BatchedDataSyncMessage;
import com.terracottatech.store.server.messages.replication.MetadataSyncMessage;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Answers;
import org.terracotta.entity.PassiveSynchronizationChannel;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.indexing.IndexSettings.BTREE;
import static com.terracottatech.store.server.Utils.alterRecord;
import static com.terracottatech.store.server.Utils.compute;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatasetLoaderTest {

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private SovereignFRSStorage storage;

  @Before
  public void setUp() throws Exception {
    if (storage == null) {
      File file = tempFolder.newFolder();
      storage = new SovereignFRSStorage(new PersistenceRoot(file, PersistenceRoot.Mode
              .DONTCARE), SovereignBufferResource.unlimited());
      storage.startupMetadata().get();
      storage.startupData().get();
    }
  }

  @Test
  public void testDatasetLoader() throws ExecutionException, InterruptedException {

    SovereignDatasetImpl<Integer> activeDataset = (SovereignDatasetImpl<Integer>) new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
            .alias("test-active")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();


    SovereignDatasetImpl<Integer> passiveDataset = (SovereignDatasetImpl<Integer>) new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
            .alias("test-passive")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();

    DatasetSynchronizer<?> activeSynchronizer = new DatasetSynchronizer<>(() -> activeDataset);
    DatasetLoader<Integer> passiveLoader = new DatasetLoader<>(new RawDataset<>(passiveDataset));
    TestSyncChannel testSyncChannel = new TestSyncChannel(passiveLoader);

    Random random = new Random();

    Set<Integer> keys = new HashSet<>();

    random.ints(1000).distinct().forEach(x -> {
            keys.add(x);
            activeDataset.add(IMMEDIATE, x, nameCell.newCell("Name" + x), phoneCell.newCell(x));
    });

    CountDownLatch waittillUpdaterFinishes = new CountDownLatch(1);

    CompletableFuture<?> updater = CompletableFuture.runAsync(() -> {
      keys.forEach(x -> {
        if (random.nextBoolean()) {
          activeDataset.delete(IMMEDIATE, x);
        } else {
          activeDataset.applyMutation(IMMEDIATE, x, r -> true, alterRecord(compute(nameCell, c -> c.definition().newCell("Name Updated" + x))));
        }
      });
      waittillUpdaterFinishes.countDown();
    });

    CompletableFuture<?> synchronizers = CompletableFuture.runAsync(() -> {
    for (int i = 0; i < 2; i++) {
      final int shardIdx = i;
        activeSynchronizer.synchronizeShard(testSyncChannel, shardIdx);
        try {
          waittillUpdaterFinishes.await();
        } catch (InterruptedException e) {
          fail();
        }
        activeSynchronizer.synchronizeRecordedMutations(testSyncChannel, shardIdx);
      }
    });

    CompletableFuture.allOf(updater, synchronizers).get();

    List<Record<Integer>> records = new ArrayList<>();
    activeDataset.records().forEach(records::add);

    Iterator<Record<Integer>> recordIterator = records.iterator();

    passiveDataset.records().forEach(x -> {
      Record<Integer> next = recordIterator.next();
      assertThat(x.getKey(), is(next.getKey()));
      assertThat(x.get(nameCell).get(), equalTo(next.get(nameCell).get()));
      assertThat(x.get(phoneCell).get(), equalTo(next.get(phoneCell).get()));
    });

  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testPrimaryIndexInteraction() {
    SovereignDatasetImpl<Integer> dataset = mock(SovereignDatasetImpl.class);
    SovereignPrimaryMap<Integer> primaryMap = mock(SovereignPrimaryMap.class);
    when(dataset.getPrimary()).thenReturn(primaryMap);
    AbstractRecordContainer<Integer> recordContainer = mock(AbstractRecordContainer.class);
    when(recordContainer.deleteIfPresent(0)).thenReturn(1);
    ShardedRecordContainer shardedRecordContainer = mock(ShardedRecordContainer.class);
    when(dataset.getContainer()).thenReturn(shardedRecordContainer);
    ArrayList t = new ArrayList();
    t.add(recordContainer);
    when(shardedRecordContainer.getShards()).thenReturn(t);

    DatasetLoader<Integer> loader = new DatasetLoader<>(new RawDataset<>(dataset));

    BatchedDataSyncMessage batchedDataSyncMessage = mock(BatchedDataSyncMessage.class);
    when(batchedDataSyncMessage.getShardIdx()).thenReturn(0);
    ByteBuffer buffer = mock(ByteBuffer.class);
    when(recordContainer.restore(1, buffer)).thenReturn(2);
    List list = new ArrayList();
    list.add(new BufferDataTuple() {
      @Override
      public long index() {
        return 0;
      }

      @Override
      public ByteBuffer getData() {
        return null;
      }
    });

    list.add(new BufferDataTuple() {
      @Override
      public long index() {
        return 1;
      }

      @Override
      public ByteBuffer getData() {
        return buffer;
      }
    });
    when(batchedDataSyncMessage.getDataTuples()).thenReturn(list);

    loader.load(batchedDataSyncMessage);
    verify(primaryMap, times(1)).remove(null, 1, new PersistentMemoryLocator(0, null));
    verify(primaryMap, times(1)).reinstall(2, new PersistentMemoryLocator(1, null));
  }

  @Test
  public void testPendingIndexDescriptions() {
    @SuppressWarnings("unchecked")
    RawDataset<Integer> dataset = mock(RawDataset.class, Answers.RETURNS_DEEP_STUBS);

    DatasetLoader<Integer> loader = new DatasetLoader<>(dataset);

    IntCellDefinition intCellDefinition = CellDefinition.defineInt("test-Int");
    StringCellDefinition stringCellDefinition = CellDefinition.defineString("test-String");

    loader.addIndex(intCellDefinition, BTREE);
    loader.addIndex(stringCellDefinition, BTREE);
    loader.deleteIndex(intCellDefinition, BTREE);
    loader.addIndex(intCellDefinition, BTREE);
    loader.deleteIndex(stringCellDefinition, BTREE);


    assertThat(loader.getPendingIndexDescriptions().size(), is(1));
    assertThat(loader.getPendingIndexDescriptions().get(intCellDefinition), notNullValue());

  }

  private static class TestSyncChannel implements PassiveSynchronizationChannel<DatasetEntityMessage> {

    private final DatasetLoader<?> loader;

    TestSyncChannel(DatasetLoader<?> datasetLoader) {
      this.loader = datasetLoader;
    }

    @Override
    public void synchronizeToPassive(DatasetEntityMessage message) {
      if (message instanceof BatchedDataSyncMessage) {
        loader.load((BatchedDataSyncMessage) message);
      } else {
        loader.loadMetaData((MetadataSyncMessage) message);
      }
    }
  }
}