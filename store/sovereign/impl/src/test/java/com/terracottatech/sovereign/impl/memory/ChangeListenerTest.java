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
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.model.SovereignPrimaryMap;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.alterRecord;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.compute;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ChangeListenerTest {

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);
  private static final CellDefinition<Character> statusCell = CellDefinition.define("status", Type.CHAR);

  private SovereignDatasetImpl<String> dataset;

  private ChangeListener listener0;
  private ChangeListener listener1;

  private SovereignFRSStorage storage;

  @Before
  public void setUp() throws Exception {
    if (storage == null) {
      storage = new SovereignFRSStorage(new PersistenceRoot(tempFolder.newFolder(), PersistenceRoot.Mode
              .CREATE_NEW), SovereignBufferResource.unlimited());
      storage.startupMetadata().get();
      storage.startupData().get();
    }

    this.dataset = (SovereignDatasetImpl<String>) new SovereignBuilder<>(Type.STRING, SystemTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();

  }

  @After
  public void tearDown() throws IOException {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    this.dataset = null;
  }

  @Test
  public void testAddChangeListener() {

    assertFails(() -> this.dataset.consumeMutationsThenRemoveListener(0, new ChangeListener(), bufferDataTuples -> {
    }), IllegalStateException.class);
    assertFails(() -> this.dataset.consumeMutationsThenRemoveListener(1, new ChangeListener(), bufferDataTuples -> {
    }), IllegalStateException.class);

    addListeners();

    this.dataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples -> assertThat(bufferDataTuples.iterator(), notNullValue()));

    this.dataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples -> assertThat(bufferDataTuples.iterator(), notNullValue()));
  }

  @Test
  public void testRemoveChangeListener() {
    addListeners();

    this.dataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples -> assertThat(bufferDataTuples.iterator(), notNullValue()));

    this.dataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples -> assertThat(bufferDataTuples.iterator(), notNullValue()));

    assertFails(() -> this.dataset.consumeMutationsThenRemoveListener(0, new ChangeListener(), bufferDataTuples -> {
    }), IllegalStateException.class);
    assertFails(() -> this.dataset.consumeMutationsThenRemoveListener(1, new ChangeListener(), bufferDataTuples -> {
    }), IllegalStateException.class);
  }

  @Test
  public void testAddWithListener() {

    addListeners();

    addRecords();

    this.dataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples -> assertThat(bufferDataTuples.iterator().next(), notNullValue()));

    this.dataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples -> assertThat(bufferDataTuples.iterator().next(), notNullValue()));

  }

  @Test
  public void testDeleteWithListener() {
    addRecords();
    addListeners();

    this.dataset.delete(IMMEDIATE, "Albin");

    this.dataset.delete(IMMEDIATE, "Abhilash");

    assertFails(() -> this.dataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples -> bufferDataTuples.iterator().next()), NoSuchElementException.class);

    this.dataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples -> assertThat(bufferDataTuples.iterator().next(), notNullValue()));

  }

  @Test
  public void testApplyMutationWithListener() {
    addRecords();
    addListeners();

    this.dataset.applyMutation(IMMEDIATE, "Abhilash", r -> false, r -> r);

    this.dataset.applyMutation(IMMEDIATE, "Albin", r -> true, alterRecord(compute(nameCell, c -> c.definition().newCell("Albin"))));

    this.dataset.applyMutation(IMMEDIATE, "Sairam", r -> true, alterRecord(compute(nameCell, c -> c.definition().newCell("Venkat"))));

    this.dataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples -> assertThat(bufferDataTuples.iterator().next(), notNullValue()));
    assertFails(() -> this.dataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples -> bufferDataTuples.iterator().next()), NoSuchElementException.class);

  }

  @Test
  public void testDataSyncToNewDataSet() {
    addRecords();

    SovereignDatasetImpl<String> passiveDataset = (SovereignDatasetImpl<String>) new SovereignBuilder<>(Type.STRING, SystemTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();

    passiveDataset.getRuntime().getSchema().initialize(this.dataset.getRuntime()
                                                         .getSchema()
                                                         .getBackend()
                                                         .getPersistable());

    SovereignPrimaryMap<String> passiveDatasetPrimary = passiveDataset.getPrimary();

    //FIRST SHARD
    AbstractRecordContainer<String> passieRecordContainer0 = passiveDataset.getContainer().getShards().get(0);
    BufferDataTuple existingRecord = this.dataset.iterate(0).next();
    String k0 = passieRecordContainer0.restore(existingRecord.index(), existingRecord.getData());
    passiveDatasetPrimary.reinstall(k0, new PersistentMemoryLocator(existingRecord.index(), null));
    passieRecordContainer0.getBufferContainer().finishRestart();

    //SECOND SHARD
    existingRecord = this.dataset.iterate(1).next();
    AbstractRecordContainer<String> passieRecordContainer1 = passiveDataset.getContainer().getShards().get(1);
    String k1 = passieRecordContainer1.restore(existingRecord.index(), existingRecord.getData());
    passiveDatasetPrimary.reinstall(k1, new PersistentMemoryLocator(existingRecord.index(), null));
    passieRecordContainer1.getBufferContainer().finishRestart();

    assertThat(passiveDataset.get("Abhilash").cells().size(), is(3));
    assertThat(passiveDataset.get("Sairam").cells().size(), is(3));
    assertThat(passiveDataset.records().filter(r -> r.get(phoneCell).get() == 2345).findFirst().get().getKey(), is("Abhilash"));
    assertThat(passiveDataset.records().filter(r -> r.get(statusCell).get() == 'B').findFirst().get().getKey(), is("Sairam"));

    addListeners();

    this.dataset.applyMutation(IMMEDIATE, "Sairam", r -> true, alterRecord(compute(nameCell, c -> c.definition().newCell("Venkat"))));
    this.dataset.applyMutation(IMMEDIATE, "Abhilash", r -> true, alterRecord(compute(statusCell, c -> c.definition().newCell('B'))));

    this.dataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples -> {
      BufferDataTuple mutation = bufferDataTuples.iterator().next();
      String key = passieRecordContainer0.restore(mutation.index(), mutation.getData());
      passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(mutation.index(), null));
    });

    this.dataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples -> {
      BufferDataTuple mutation = bufferDataTuples.iterator().next();
      String key = passieRecordContainer1.restore(mutation.index(), mutation.getData());
      passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(mutation.index(), null));
    });

    assertThat(passiveDataset.get("Abhilash").cells().size(), is(3));
    assertThat(passiveDataset.get("Sairam").cells().size(), is(3));
    assertThat(passiveDataset.records().filter(r -> r.get(nameCell).get().equals("Venkat")).findFirst().get().getKey(), is("Sairam"));
    assertThat(passiveDataset.records().filter(r -> r.get(statusCell).get() == 'B').count(), is(2l));

    //Check Order of Records seen
    List<String> keys = new ArrayList<>();
    this.dataset.records().forEach(x -> keys.add(x.getKey()));

    Iterator<String> iterator = keys.iterator();
    passiveDataset.records().forEach(x -> assertThat(x.getKey(), equalTo(iterator.next())));

  }

  @Test
  public void testRecordVisitationOrder() throws ExecutionException, InterruptedException {

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

    //Add to Active
    random.ints(100).forEach(x -> activeDataset.add(IMMEDIATE, x, nameCell.newCell("Cell" + x), phoneCell.newCell(x), statusCell.newCell((char) x)));

    listener0 = new ChangeListener();
    listener1 = new ChangeListener();
    activeDataset.addChangeListener(0, listener0);
    activeDataset.addChangeListener(1, listener1);

    passiveDataset.getRuntime().getSchema().initialize(activeDataset.getRuntime()
                                                         .getSchema()
                                                         .getBackend()
                                                         .getPersistable());

    SovereignPrimaryMap<Integer> passiveDatasetPrimary = passiveDataset.getPrimary();

    //FIRST SHARD
    AbstractRecordContainer<Integer> passiveRecordContainer0 = passiveDataset.getContainer().getShards().get(0);

    //SECOND SHARD
    AbstractRecordContainer<Integer> passiveRecordContainer1 = passiveDataset.getContainer().getShards().get(1);

    Future<?> f2 = Executors.newSingleThreadExecutor().submit(() -> {
      ShardIterator shardIterator = activeDataset.iterate(0);
      while (shardIterator.hasNext()) {
        BufferDataTuple dataTuple = shardIterator.next();
        Integer key = passiveRecordContainer0.restore(dataTuple.index(), dataTuple.getData());
        passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(dataTuple.index(), null));
      }
    });

    Future<?> f3 = Executors.newSingleThreadExecutor().submit(() -> {
      ShardIterator shardIterator = activeDataset.iterate(1);
      while (shardIterator.hasNext()) {
        BufferDataTuple dataTuple = shardIterator.next();
        Integer key = passiveRecordContainer1.restore(dataTuple.index(), dataTuple.getData());
        passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(dataTuple.index(), null));
      }
    });

    Future<?> f1 = Executors.newSingleThreadExecutor().submit(() -> {
      random.ints(100).forEach(x -> activeDataset.add(IMMEDIATE, x, nameCell.newCell("Cell" + x), phoneCell.newCell(x), statusCell.newCell((char) x)));
    });

    //Wait for all to complete
    f1.get();
    f2.get();
    f3.get();

    activeDataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples ->
            bufferDataTuples.forEach(x -> {
              Integer key = passiveRecordContainer0.restore(x.index(), x.getData());
              passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(x.index(), null));
            }));

    activeDataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples ->
            bufferDataTuples.forEach(x -> {
              Integer key = passiveRecordContainer1.restore(x.index(), x.getData());
              passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(x.index(), null));
            }));

    passiveRecordContainer0.getBufferContainer().finishRestart();
    passiveRecordContainer1.getBufferContainer().finishRestart();

    //Check Order of Records seen
    List<Integer> keys = new ArrayList<>();
    activeDataset.records().forEach(x -> keys.add(x.getKey()));

    Iterator<Integer> iterator = keys.iterator();
    passiveDataset.records().forEach(x -> assertThat(x.getKey(), equalTo(iterator.next())));

  }

  @Test
  public void testIteratingAndUpdatingSameKeyDuringSync() {
    addRecords();
    addListeners();

    this.dataset.applyMutation(IMMEDIATE, "Sairam", r -> true, alterRecord(compute(nameCell, c -> c.definition().newCell("Venkat"))));
    this.dataset.applyMutation(IMMEDIATE, "Abhilash", r -> true, alterRecord(compute(statusCell, c -> c.definition().newCell('B'))));

    this.dataset.add(IMMEDIATE, "Albin", nameCell.newCell("Suresh"), phoneCell.newCell(2345), statusCell.newCell('A'));
    this.dataset.add(IMMEDIATE, "Ravi", nameCell.newCell("Chaturvedi"), phoneCell.newCell(5687), statusCell.newCell('B'));

    SovereignDatasetImpl<String> passiveDataset = (SovereignDatasetImpl<String>) new SovereignBuilder<>(Type.STRING, SystemTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();

    passiveDataset.getRuntime().getSchema().initialize(this.dataset.getRuntime()
                                                         .getSchema()
                                                         .getBackend()
                                                         .getPersistable());

    SovereignPrimaryMap<String> passiveDatasetPrimary = passiveDataset.getPrimary();

    //FIRST SHARD
    AbstractRecordContainer<String> passieRecordContainer0 = passiveDataset.getContainer().getShards().get(0);

    //SECOND SHARD
    AbstractRecordContainer<String> passieRecordContainer1 = passiveDataset.getContainer().getShards().get(1);

    ShardIterator shardIterator = this.dataset.iterate(0);
    while (shardIterator.hasNext()) {
      BufferDataTuple dataTuple = shardIterator.next();
      String key = passieRecordContainer0.restore(dataTuple.index(), dataTuple.getData());
      passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(dataTuple.index(), null));
    }

    this.dataset.consumeMutationsThenRemoveListener(0, listener0, bufferDataTuples -> bufferDataTuples.forEach(x -> {
      String key = passieRecordContainer0.restore(x.index(), x.getData());
      passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(x.index(), null));
    }));

    shardIterator = this.dataset.iterate(1);
    while (shardIterator.hasNext()) {
      BufferDataTuple dataTuple = shardIterator.next();
      String key = passieRecordContainer1.restore(dataTuple.index(), dataTuple.getData());
      passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(dataTuple.index(), null));
    }

    this.dataset.consumeMutationsThenRemoveListener(1, listener1, bufferDataTuples -> bufferDataTuples.forEach(x -> {
      String key = passieRecordContainer1.restore(x.index(), x.getData());
      passiveDatasetPrimary.reinstall(key, new PersistentMemoryLocator(x.index(), null));
    }));

    passieRecordContainer0.getBufferContainer().finishRestart();
    passieRecordContainer1.getBufferContainer().finishRestart();

    passiveDataset.records().forEach(System.out::println);

    //Check Order of Records seen
    List<String> keys = new ArrayList<>();
    this.dataset.records().forEach(x -> keys.add(x.getKey()));

    Iterator<String> iterator = keys.iterator();
    passiveDataset.records().forEach(x -> assertThat(x.getKey(), equalTo(iterator.next())));

  }

  private void addListeners() {
    listener0 = new ChangeListener();
    listener1 = new ChangeListener();
    this.dataset.addChangeListener(0, listener0);
    this.dataset.addChangeListener(1, listener1);
  }

  private void addRecords() {
    this.dataset.add(IMMEDIATE, "Abhilash", nameCell.newCell("Abhilash"), phoneCell.newCell(2345), statusCell.newCell('A'));
    this.dataset.add(IMMEDIATE, "Sairam", nameCell.newCell("Sairam"), phoneCell.newCell(5687), statusCell.newCell('B'));
  }

  private static <T extends Exception> void assertFails(TestFunction function, Class<T> expected) {
    try {
      function.apply();
      fail("Expecting " + expected.getName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }

  private static interface TestFunction {
    void apply();
  }

}
