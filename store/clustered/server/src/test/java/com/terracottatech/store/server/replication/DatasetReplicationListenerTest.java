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
package com.terracottatech.store.server.replication;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.NullCRUDReplicationResponse;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordSimplifiedResponse;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.server.DatasetPassiveEntity;
import com.terracottatech.store.server.RawDataset;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.messages.replication.DataReplicationMessage;
import com.terracottatech.store.server.messages.replication.MetadataReplicationMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.server.Utils.alterRecord;
import static com.terracottatech.store.server.Utils.compute;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DatasetReplicationListenerTest {

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private SovereignDataset<Integer> dataset;
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

  @After
  public void tearDown() throws Exception {
    if (dataset != null) {
      storage.destroyDataSet(dataset.getUUID());
      storage.shutdown();
      System.gc();
    }
  }

  private SovereignDatasetImpl<Integer> createDatasetWithIndex(boolean offheapOnly) throws Exception {
    SovereignBuilder<Integer, SystemTimeReference> builder = new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024);
    SovereignDataset<Integer> dataset = offheapOnly ? builder
            .build() : builder.storage(storage).build();

    dataset.getIndexing().createIndex(nameCell, SovereignIndexSettings.BTREE).call();
    return (SovereignDatasetImpl<Integer>) dataset;
  }

  private SovereignDatasetImpl<Integer> createDatasetWithoutIndex(boolean offheapOnly) {
    SovereignBuilder<Integer, SystemTimeReference> builder = new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024);
    return (SovereignDatasetImpl<Integer>) (offheapOnly ? builder.build() : builder
            .storage(storage)
            .build());
  }

  @Test
  public void testReplicationCrudWithoutIndex() {
    testReplication(createDatasetWithoutIndex(false), createDatasetWithoutIndex(false));
    testReplication(createDatasetWithoutIndex(true), createDatasetWithoutIndex(true));
  }

  @Test
  public void testReplicationCrudWithIndex() throws Exception {
    testReplication(createDatasetWithIndex(false), createDatasetWithIndex(false));
    testReplication(createDatasetWithIndex(true), createDatasetWithIndex(true));
  }

  private static void testReplication(SovereignDatasetImpl<Integer> activeDataset, SovereignDatasetImpl<Integer> passiveDataset) {

    ChangeListener[] listeners = new ChangeListener[2];
    //This is just for test
    for (int i = 0; i < 2; i++) {
      listeners[i] = new ChangeListener();
      activeDataset.addChangeListener(i, listeners[i]);
    }

    Random random = new Random();
    List<Integer> added = new LinkedList<>();

    // Adding

    random.ints(1000).forEach( x -> {
      added.add(x);
      activeDataset.add(IMMEDIATE, x, nameCell.newCell("Name" + x), phoneCell.newCell(x));
    });

    // Deleting

    for (int i=0; i<50; i++) {
      int toDel = random.nextInt(1000);
      activeDataset.delete(IMMEDIATE, toDel, r -> true);
    }

    // Updating

    for (int i=0; i<50; i++) {
      int toUpdate = random.nextInt(1000);
      activeDataset.applyMutation(IMMEDIATE, toUpdate, r -> true, alterRecord(compute(nameCell, x -> x.definition().newCell("Name Updated" + toUpdate))));
    }

    DatasetReplicationListener<Integer> replicator = new DatasetReplicationListener<>(new RawDataset<>(passiveDataset));

    //this step has to be done before the stuff is written.
    passiveDataset.getRuntime().getSchema().initialize(activeDataset.getRuntime().getSchema().getBackend().getPersistable());

    for (int i = 0; i < 2; i++) {
      activeDataset.consumeMutationsThenRemoveListener(i, listeners[i], bufferDataTuples ->
              bufferDataTuples.forEach(dataTuple ->
                      replicator.handleReplicatedData(new DataReplicationMessage(dataTuple.index(), dataTuple.getData()))));
    }

    assertMatchingData(activeDataset, passiveDataset);
  }

  @Test
  public void testMetadataReplication() {
    SovereignDatasetImpl<Integer> active = createDatasetWithoutIndex(false);
    SovereignDatasetImpl<Integer> passive = createDatasetWithoutIndex(false);

    testMetadataReplication(active, passive);

    active = createDatasetWithoutIndex(true);
    passive = createDatasetWithoutIndex(true);

    testMetadataReplication(active, passive);

  }

  private static void testMetadataReplication(SovereignDatasetImpl<Integer> active, SovereignDatasetImpl<Integer> passive) {
    DatasetReplicationListener<Integer> replicator = new DatasetReplicationListener<>(new RawDataset<>(passive));

    active.getStorage().addMetaDataConsumer(active.getUUID(), (tag, metadata) -> {
      replicator.handleReplicatedMetadata(new MetadataReplicationMessage(tag.ordinal(), metadata));
    });

    Random random = new Random();
    random.ints(10000).forEach(x -> active.add(IMMEDIATE, x, phoneCell.newCell(x)));

    assertThat(active.getCurrentMSN(), is(passive.getCurrentMSN()));
    assertThat(passive.getSchema().definitions().count(), is(1L));
  }

  @Test
  public void testCRUDDataReplication() throws Exception {
    SovereignDatasetImpl<Integer> activeDataset = createDatasetWithIndex(true);
    SovereignDatasetImpl<Integer> passiveDataset = createDatasetWithIndex(true);

    DatasetReplicationListener<Integer> replicator = new DatasetReplicationListener<>(new RawDataset<>(passiveDataset));
    AtomicReference<BufferDataTuple> opResult = new AtomicReference<>();

    setMutationConsumer(activeDataset, opResult);
    activeDataset.add(IMMEDIATE, 1, nameCell.newCell("Name" + 1), phoneCell.newCell(1));
    //this step has to be done before the stuff is written.
    passiveDataset.getRuntime().getSchema().initialize(activeDataset.getRuntime().getSchema().getBackend().getPersistable());

    CRUDDataReplicationMessage crudMessage =
        new CRUDDataReplicationMessage(opResult.get().index(), opResult.get().getData(), false, 5L, 123L, 456L);
    AddRecordSimplifiedResponse addResponse = (AddRecordSimplifiedResponse) replicator.handleReplicatedData(crudMessage);
    assertThat(addResponse.isAdded(), is(true));

    setMutationConsumer(activeDataset, opResult);
    activeDataset.applyMutation(IMMEDIATE, 1, r -> true, alterRecord(compute(nameCell, x -> x.definition().newCell("Name Updated" + 1))));
    crudMessage = new CRUDDataReplicationMessage(opResult.get().index(), opResult.get().getData(), false, 5L, 123L, 456L);
    PredicatedUpdateRecordSimplifiedResponse updateResponse = (PredicatedUpdateRecordSimplifiedResponse) replicator.handleReplicatedData(crudMessage);
    assertThat(updateResponse.isUpdated(), is(true));

    setMutationConsumer(activeDataset, opResult);
    activeDataset.delete(IMMEDIATE, 1);
    crudMessage = new CRUDDataReplicationMessage(opResult.get().index(), opResult.get().getData(), false, 5L, 123L, 456L);
    PredicatedDeleteRecordSimplifiedResponse deleteResponse = (PredicatedDeleteRecordSimplifiedResponse) replicator.handleReplicatedData(crudMessage);
    assertThat(deleteResponse.isDeleted(), is(true));

    assertMatchingData(activeDataset, passiveDataset);
  }

  @Test
  public void testCRUDReplicationMessageIsIgnoredUntilSyncBoundaryMessage() throws Exception {
    SovereignDatasetImpl<Integer> passiveDataset = createDatasetWithIndex(true);

    DatasetReplicationListener<Integer> replicator = new DatasetReplicationListener<>(new RawDataset<>(passiveDataset));

    CRUDDataReplicationMessage crudDataReplicationMessage = Mockito.mock(CRUDDataReplicationMessage.class);
    replicator.prepareForSync();
    DatasetEntityResponse datasetEntityResponse = replicator.handleReplicatedData(crudDataReplicationMessage);

    assertThat(datasetEntityResponse, instanceOf(NullCRUDReplicationResponse.class));

  }

  private static void setMutationConsumer(SovereignDatasetImpl<Integer> activeDataset, AtomicReference<BufferDataTuple> opResult) {
    for (int i = 0; i < 2; i++) {
      activeDataset.setMutationConsumer(opResult::set);
    }
  }

  private static void assertMatchingData(SovereignDatasetImpl<Integer> activeDataset, SovereignDatasetImpl<Integer> passiveDataset) {
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
}