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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaBackend;
import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SyncRestartabilityTest {

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private SovereignFRSStorage storage;
  private File file;

  @Before
  public void setUp() throws Exception {
    if (storage == null) {
      file = tempFolder.newFolder();
      storage = new SovereignFRSStorage(new PersistenceRoot(file, PersistenceRoot.Mode
              .DONTCARE), SovereignBufferResource.unlimited());
      storage.startupMetadata().get();
      storage.startupData().get();
    }
  }

  @Test
  public void testDatasetRestartabilityAfterSync() throws ExecutionException, InterruptedException, IOException {

    SovereignDatasetImpl<Integer> activeDataset = (SovereignDatasetImpl<Integer>) new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();

    SovereignDatasetImpl<Integer> passiveDataset = (SovereignDatasetImpl<Integer>) new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();

    Random random = new Random();

    random.ints(100).forEach(x ->
            activeDataset.add(IMMEDIATE, x, nameCell.newCell("Name" + x), phoneCell.newCell(x)));

    ChangeListener[] listeners = new ChangeListener[2];
    for (int i = 0; i < 2; i++) {
      listeners[i] = new ChangeListener();
    }
    activeDataset.addChangeListener(0, listeners[0]);
    activeDataset.addChangeListener(1, listeners[1]);

    DatasetSchemaBackend backend = activeDataset.getRuntime().getSchema().getBackend();
    passiveDataset.getRuntime().getSchema().initialize(backend.getPersistable());

    storage.setMetadata(MetadataKey.Tag.SCHEMA.keyFor(passiveDataset.getUUID()), backend.getPersistable());

    CompletableFuture<?>[] futures = new CompletableFuture<?>[3];

    futures[0] = CompletableFuture.runAsync(() ->
            random.ints(100).forEach(x ->
                    activeDataset.add(IMMEDIATE, x, nameCell.newCell("Name" + x), phoneCell.newCell(x))));

    for (int i = 0; i < 2; i++) {
      int shardIdx = i;
      futures[i + 1] = CompletableFuture.runAsync(() -> {
        ShardIterator shardIterator = activeDataset.iterate(shardIdx);
        while (shardIterator.hasNext()) {
          BufferDataTuple dataTuple = shardIterator.next();
          AbstractRecordContainer<Integer> passiveRecordContainer = passiveDataset.getContainer().getShards().get(shardIdx);
          Integer key = passiveRecordContainer.restore(dataTuple.index(), dataTuple.getData());
          passiveDataset.getPrimary().reinstall(key, new PersistentMemoryLocator(dataTuple.index(), null));
        }
      });
    }

    CompletableFuture.allOf(futures).get();

    for (int i = 0; i < 2; i++) {
      AbstractRecordContainer<Integer> passieRecordContainer = passiveDataset.getContainer().getShards().get(i);
      activeDataset.consumeMutationsThenRemoveListener(i, listeners[i], bufferDataTuples ->
              bufferDataTuples.forEach(x -> {
                Integer key = passieRecordContainer.restore(x.index(), x.getData());
                passiveDataset.getPrimary().reinstall(key, new PersistentMemoryLocator(x.index(), null));
              }));
      passieRecordContainer.getBufferContainer().finishRestart();
    }

    List<Record<Integer>> records = new ArrayList<>();
    passiveDataset.records().forEach(records::add);

    CachingSequence prevSeq = activeDataset.getSequence();

    storage.setMetadata(MetadataKey.Tag.CACHING_SEQUENCE.keyFor(passiveDataset.getUUID()), prevSeq);

    Iterator<Record<Integer>> firstIterator = records.iterator();
    passiveDataset.records().forEach(x -> {
      Record<Integer> expectedRecord = firstIterator.next();
      assertThat(x.getKey(), equalTo(expectedRecord.getKey()));
      assertThat(x.get(nameCell).get(), equalTo(expectedRecord.get(nameCell).get()));
      assertThat(x.get(phoneCell).get(), equalTo(expectedRecord.get(phoneCell).get()));
    });

    UUID passiveDatasetUUID = passiveDataset.getUUID();

    passiveDataset.flush();

    storage.shutdown();
    System.gc();
    this.storage = null;

    storage = new SovereignFRSStorage(new PersistenceRoot(file, PersistenceRoot.Mode
            .DONTCARE), SovereignBufferResource.unlimited());
    storage.startupMetadata().get();
    storage.startupData().get();

    Iterator<Record<Integer>> recordIterator = records.iterator();

    @SuppressWarnings("unchecked")
    SovereignDatasetImpl<Integer> anotherDs = (SovereignDatasetImpl<Integer>) storage.getDataset(passiveDatasetUUID);
    anotherDs.records().forEach(x -> {
      SovereignRecord<Integer> expectedRecord = (SovereignRecord<Integer>) recordIterator.next();
      SovereignRecord<Integer> presentRecord = (SovereignRecord<Integer>) x;
      assertThat(presentRecord.getKey(), equalTo(expectedRecord.getKey()));
      assertThat(presentRecord.getMSN(), equalTo(expectedRecord.getMSN()));
      assertThat(x.get(nameCell).get(), equalTo(expectedRecord.get(nameCell).get()));
      assertThat(x.get(phoneCell).get(), equalTo(expectedRecord.get(phoneCell).get()));
    });

    CachingSequence seqAfterRestart = anotherDs.getSequence();

    int cacheMany = 1000;
    assertThat(prevSeq.current() + (cacheMany - records.size()), equalTo(seqAfterRestart.current()));

  }

}
