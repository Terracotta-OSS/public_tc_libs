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

package com.terracottatech.sovereign.impl.persistence;

import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.impl.persistence.base.AbstractRestartabilityBasedStorage;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.PersistableTimeReferenceGenerator;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.tool.Diagnostics;

import org.hamcrest.Matchers;
import org.hamcrest.core.AllOf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 5/19/2015.
 */
public abstract class AbstractBaseStorageTest {

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  protected File file;

  @Before
  public void befer() throws IOException {
    System.out.format("%n%n*********************************************************%n" +
        "      %s%n" +
        "*********************************************************%n", testName.getMethodName());
    System.out.format("maxDirectMemory=%,d%n", Diagnostics.getMaxDirectMemory());
    this.file = tempFolder.newFolder();
  }

  @After
  public void efter() throws IOException {
    //FileUtils.deleteRecursively(file);
    file = null;
  }

  @SuppressWarnings("unused")
  private void dumpHeap() {
    try {
      Diagnostics.dumpHeap(true,
          String.format("%1$s_%2$tFT%2$tH%2$tM%2$tS.%2$tL.hprof", testName.getMethodName(), new Date()));
    } catch (IOException e) {
      System.err.format("Failed to write heap dump for %s: %s%n", testName.getMethodName(), e);
    }
  }

  @Test
  public void testMetaCreateEmpty() throws IOException, InterruptedException, RestartStoreException, ExecutionException {
    Diagnostics.dumpBufferPoolInfo(System.out);

    AbstractRestartabilityBasedStorage storage = getOrCreateStorage(false);
    storage.startupMetadata().get();
    assertThat(storage.getDataSetDescriptions().size(), is(0));
    shutdownStorage();

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  public abstract AbstractRestartabilityBasedStorage getOrCreateStorage(boolean reopen) throws IOException;

  public abstract void shutdownStorage() throws IOException, InterruptedException;

  public SovereignDatasetImpl<String> makeDataSet(SovereignStorage<?, ?> storage) {
    SovereignDataset<String> impl;
    impl = new SovereignBuilder<>(Type.STRING, FixedTimeReference.class)
             .heap()
             .alias("singleversion")
             .storage(storage)
             .concurrency(8).build();
    return (SovereignDatasetImpl<String>) impl;
  }

  public SovereignDatasetImpl<String> makeVersionedDataSet(int versions, SovereignStorage<?, ?> storage) {
    SovereignDataset<String> impl;
    impl = new SovereignBuilder<>(Type.STRING, FixedTimeReference.class)
             .heap().limitVersionsTo(versions)
             .storage(storage)
             .concurrency(8).build();
    return (SovereignDatasetImpl<String>) impl;
  }

  @Test
  public void testSingleDatasetWithImmediate() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds = makeDataSet(storage);

      for (int i = 0; i < 10; i++) {
        ds.add(SovereignDataset.Durability.IMMEDIATE, "foo" + i, Cell.cell("index", i));
      }

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();

      assertThat(ds.getAlias(), is("singleversion"));

      for (int i = 0; i < 10; i++) {
        assertThat(ds.get("foo" + i).get(CellDefinition.define("index", Type.INT)).get(), is(i));
      }

      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testSingleDatasetWithLazy() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    long lastmsn;
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds = makeDataSet(storage);

      for (int i = 0; i < 10; i++) {
        ds.add(SovereignDataset.Durability.LAZY, "foo" + i, Cell.cell("index", i));
      }

      lastmsn = ds.getCurrentMSN();
      System.out.println("Last1: " + ds.getCurrentMSN());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();
      assertThat(ds.getCurrentMSN(), Matchers.greaterThanOrEqualTo(lastmsn));

      long max = Long.MIN_VALUE;
      for (int i = 0; i < 10; i++) {
        SovereignPersistentRecord<String> rec = ds.get("foo" + i);
        assertThat(rec.get(CellDefinition.define("index", Type.INT)).get(), is(i));
        max = Math.max(max, rec.getMSN());
      }

      ds.add(SovereignDataset.Durability.LAZY, "bar", Cell.cell("index", 100));
      SovereignPersistentRecord<String> rec = ds.get("bar");
      assertThat(rec.getMSN(), Matchers.greaterThan(max));
      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testSingleDatasetWithRestoreIndexes() throws Exception {

    Diagnostics.dumpBufferPoolInfo(System.out);

    IntCellDefinition d1 = CellDefinition.defineInt("index1");
    StringCellDefinition d2 = CellDefinition.defineString("index2");

    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();
      SovereignDatasetImpl<String> ds = makeDataSet(storage);
      ds.getIndexing().createIndex(d1, SovereignIndexSettings.BTREE).call();
      ds.getIndexing().createIndex(d2, SovereignIndexSettings.BTREE).call();

      for (int i = 0; i < 10; i++) {
        ds.add(SovereignDataset.Durability.LAZY, "foo" + i, d1.newCell(i), d2.newCell("v"+i));
      }

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();

      assertThat(ds.getIndexing().getIndexes().size(), is(2));

      for (int i = 0; i < 10; i++) {
        assertThat(ds.get("foo" + i).get(d1).get(), is(i));
      }

      SovereignSortedIndexMap<Integer, String> si = ds.getSortedIndexFor(d1);
      long est = si.estimateSize();
      long cnt = ds.records().filter(d1.value().isGreaterThan(-100)).count();
      Assert.assertThat(ds.records().count(), is(10l));
      Assert.assertThat(est, is(ds.records().count()));
      Assert.assertThat(cnt, is(ds.records().count()));

      SovereignSortedIndexMap<String, String> si2 = ds.getSortedIndexFor(d2);
      Assert.assertThat(si2.estimateSize(), is(ds.records().count()));

      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testSingleDatasetMultipleVersionRestore() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds = makeVersionedDataSet(4, storage);

      for (int i = 0; i < 10; i++) {
        ds.add(SovereignDataset.Durability.LAZY, "foo" + i, Cell.cell("index", i));
      }
      ds.records().forEach(ds.applyMutation(SovereignDataset.Durability.IMMEDIATE, row -> {
        int oldVal = row.get(CellDefinition.define("index", Type.INT)).get();
        int newVal = oldVal + 1;
        return Collections.singletonList(Cell.cell("index", newVal));
      }));
      for (int i = 0; i < 10; i++) {
        assertThat(ds.get("foo" + i).get(CellDefinition.define("index", Type.INT)).get(), is(i + 1));
        assertThat(ds.get("foo" + i).versions().count(), is(2l));
      }

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();

      assertThat(ds.getAlias(), is(ds.getUUID().toString()));
      for (int i = 0; i < 10; i++) {
        SovereignPersistentRecord<String> rec = ds.get("foo" + i);
        assertThat(rec.versions().count(), is(2l));
        int ii = rec.get(CellDefinition.define("index", Type.INT)).get();
        assertThat(ii, is(i + 1));
      }

      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testSingleDatasetSingleVersionRestore() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    int MANY = 10;
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds = makeDataSet(storage);

      for (int i = 0; i < MANY; i++) {
        ds.add(SovereignDataset.Durability.LAZY, "foo" + i, Cell.cell("index", i));
      }
//      for (int i = 0; i < MANY; i++) {
//        System.out.println(ds.get("foo" + i).toString());
//      }
      ds.records().forEach(ds.applyMutation(SovereignDataset.Durability.IMMEDIATE, row -> {
        int oldVal = row.get(CellDefinition.define("index", Type.INT)).get();
        int newVal = oldVal + 1;
        return Collections.singletonList(Cell.cell("index", newVal));
      }));
//      System.out.println(ds.get("foo" + 6).toString());
//      for (int i = 0; i < MANY; i++) {
//        System.out.println(ds.get("foo" + i).toString());
//      }
      for (int i = 0; i < MANY; i++) {
        assertThat(ds.get("foo" + i).get(CellDefinition.define("index", Type.INT)).get(), is(i + 1));
      }

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();

//      for (int i = 0; i < MANY; i++) {
//        System.out.println(ds.get("foo" + i).toString());
//      }

      for (int i = 0; i < MANY; i++) {
        SovereignPersistentRecord<String> rec = ds.get("foo" + i);
        int ii = rec.get(CellDefinition.define("index", Type.INT)).get();
        assertThat(ii, is(i + 1));
      }

      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testSingleDatasetMultipleVersionDeleteRestore() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds = makeVersionedDataSet(4, storage);

      for (int i = 0; i < 10; i++) {
        ds.add(SovereignDataset.Durability.LAZY, "foo" + i, Cell.cell("index", i));
      }
      ds.records().forEach(ds.applyMutation(SovereignDataset.Durability.IMMEDIATE, row -> {
        int oldVal = row.get(CellDefinition.define("index", Type.INT)).get();
        int newVal = oldVal + 1;
        return Collections.singletonList(Cell.cell("index", newVal));
      }));
      ds.records().forEach(rec -> ds.delete(SovereignDataset.Durability.IMMEDIATE, rec.getKey()));
      assertThat(ds.records().count(), is(0l));
      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();

      assertThat(ds.records().count(), is(0l));

      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testSingleDatasetMultipleVersionGCRestore() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds = makeVersionedDataSet(2, storage);

      for (int i = 0; i < 10; i++) {
        ds.add(SovereignDataset.Durability.LAZY, "foo" + i, Cell.cell("index", i));
      }

      for (int i = 0; i < 5; i++) {
        ds.records().forEach(ds.applyMutation(SovereignDataset.Durability.IMMEDIATE, row -> {
          int oldVal = row.get(CellDefinition.define("index", Type.INT)).get();
          int newVal = oldVal + 1;
          return Collections.singletonList(Cell.cell("index", newVal));
        }));
      }

      for (int i = 0; i < 10; i++) {
        SovereignPersistentRecord<String> rec = ds.get("foo" + i);
        int ii = rec.get(CellDefinition.define("index", Type.INT)).get();
        assertThat(ii, is(i + 5));
      }
      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();

      for (int i = 0; i < 10; i++) {
        SovereignPersistentRecord<String> rec = ds.get("foo" + i);
        int ii = rec.get(CellDefinition.define("index", Type.INT)).get();
        assertThat(ii, is(i + 5));
        assertThat(rec.versions().count(), is(2l));
      }

      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testSingleDatasetSingleVersionDeleteRestore() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds = makeDataSet(storage);

      for (int i = 0; i < 10; i++) {
        ds.add(SovereignDataset.Durability.LAZY, "foo" + i, Cell.cell("index", i));
      }

      ds.records().forEach(ds.applyMutation(SovereignDataset.Durability.IMMEDIATE, row -> {
        int oldVal = row.get(CellDefinition.define("index", Type.INT)).get();
        int newVal = oldVal + 1;
        return Collections.singletonList(Cell.cell("index", newVal));
      }));

      ds.records().forEach(rec -> ds.delete(SovereignDataset.Durability.IMMEDIATE, rec.getKey()));
      assertThat(ds.records().count(), is(0l));
      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> ds =
          (SovereignDatasetImpl<String>) storage.getManagedDatasets().iterator().next();

      assertThat(ds.records().count(), is(0l));

      storage.destroyDataSet(ds.getUUID());

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }


  @Test
  public void testDatasetDeleteRestore() throws IOException, ExecutionException, InterruptedException, RestartStoreException {

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);

      storage.startupMetadata().get();

      storage.startupData().get();

      SovereignDatasetImpl<String> ds1 = makeDataSet(storage);

      for (int i = 0; i < 10; i++) {
        ds1.add(SovereignDataset.Durability.LAZY, "foo" + i, Cell.cell("index", i));
      }

      SovereignDatasetImpl<String> ds2 = makeDataSet(storage);

      for (int i = 0; i < 10; i++) {
        ds2.add(SovereignDataset.Durability.LAZY, "bar" + i, Cell.cell("index", i));
      }

      assertThat(storage.getDataSetDescriptions().size(), is(2));

      ((AbstractRestartabilityBasedStorage)storage).destroyDataSet(ds2.getUUID(), true);
      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);

      storage.startupMetadata().get();

      storage.startupData().get();

      assertThat(storage.getManagedDatasets().size(), is(1));

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }


  /**
   * Ensures a {@link PersistableTimeReferenceGenerator} implementation is properly handled
   * by the persistence infrastructure as metadata.
   */
  @Test
  public void testPersistentTimeReferenceGenerator() throws Exception {

    Diagnostics.dumpBufferPoolInfo(System.out);
    int carriedClock;
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(false);
      storage.startupMetadata().get();
      storage.startupData().get();

      final TestTimeReference.Generator timeReferenceGenerator = new TestTimeReference.Generator();
      assertThat(timeReferenceGenerator.isSetCallbackCalled(), is(false));
      assertThat(timeReferenceGenerator.isCallbackCalled(), is(false));

      final SovereignDataset<String> dataset =
          new SovereignBuilder<>(Type.STRING, TestTimeReference.class)
              .timeReferenceGenerator(timeReferenceGenerator)
              .heap()
              .storage(storage)
              .build();
      assertThat(timeReferenceGenerator.isSetCallbackCalled(), is(true));
      assertThat(timeReferenceGenerator.isCallbackCalled(), is(false));

      for (int i = 0; i < (timeReferenceGenerator.getMaxDelta() * 15 / 10); i++) {
        dataset.add(SovereignDataset.Durability.LAZY, String.format("key%04d", i), Cell.cell("value", i));
      }

      assertThat(timeReferenceGenerator.isCallbackCalled(), is(true));
      carriedClock = timeReferenceGenerator.getClock();

      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
    {
      SovereignStorage<?, ?> storage = getOrCreateStorage(true);
      storage.startupMetadata().get();
      storage.startupData().get();

      @SuppressWarnings("unchecked")
      final SovereignDataset<String> dataset =
          (SovereignDataset<String>)storage.getManagedDatasets().iterator().next();

      final TestTimeReference.Generator timeReferenceGenerator =
          (TestTimeReference.Generator)dataset.getTimeReferenceGenerator();
      assertThat(timeReferenceGenerator.isSetCallbackCalled(), is(true));
      assertThat(timeReferenceGenerator.isCallbackCalled(), is(false));
      assertThat(timeReferenceGenerator.getClock(), is(carriedClock));

      storage.destroyDataSet(dataset.getUUID());
      shutdownStorage();
    }

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  @Test
  public void testPersistentByteSizeOnDiskForRestore() throws Exception {

    Diagnostics.dumpBufferPoolInfo(System.out);

    SovereignStorage<?, ?> storage = getOrCreateStorage(false);

    storage.startupMetadata().get();

    storage.startupData().get();

    SovereignDatasetImpl<Integer> ds = (SovereignDatasetImpl<Integer>) new SovereignBuilder<>(Type.INT, FixedTimeReference.class)
            .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024)
            .storage(storage)
            .build();
    ds.flush();

    long bytesUsed = 1048000L;

    LinkedList<PersistentMemoryLocator> ll=new LinkedList<>();
    IntStream.rangeClosed(1, 1000).forEach(value -> {
      PersistentMemoryLocator l = ds.getContainer()
        .getShards()
        .get(0)
        .getBufferContainer().add(ByteBuffer.allocate(1024));
      ll.add(l);
    });

    ds.flush();
    assertThat(ds.getPersistentBytesUsed(), Matchers.is(bytesUsed));

    ll.stream().forEach(value -> {
      ds.getContainer().getShards().get(0).getBufferContainer().restore(value.index(),
                                                                        ByteBuffer.allocate(1024),
                                                                        ByteBuffer.allocate(1024));
    });

    long test1 = ds.getPersistentBytesUsed() - bytesUsed;
    assertThat(test1, AllOf.allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(100L)));

    shutdownStorage();

    Diagnostics.dumpBufferPoolInfo(System.out);
  }

  /**
   * A {@link TimeReference} implementation supported by a generator that
   * implements {@link PersistableTimeReferenceGenerator}.
   */
  private static final class TestTimeReference implements TimeReference<TestTimeReference>, Serializable {
    private static final long serialVersionUID = -5384345952327071834L;
    private static final Comparator<TestTimeReference> COMPARATOR = Comparator.comparingInt(t -> t.timeReference);

    private final int timeReference;

    private TestTimeReference(final int timeReference) {
      this.timeReference = timeReference;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compareTo(final TimeReference t) {
      return COMPARATOR.compare(this, (TestTimeReference) t);
    }

    public static final class Generator implements PersistableTimeReferenceGenerator<TestTimeReference> {
      private static final long serialVersionUID = 8123727471220120219L;

      private final int maxDelta = 100;
      private transient int delta = this.maxDelta;

      private int clock = 0;
      private transient int clockBase = 0;

      private transient Callable<Void> callback = () -> null;

      private transient boolean setCallbackCalled;
      private transient boolean callbackCalled;

      @Override
      public Class<TestTimeReference> type() {
        return TestTimeReference.class;
      }

      @Override
      public synchronized TestTimeReference get() {
        if (this.delta >= this.maxDelta) {
          this.clockBase = this.clock;
          this.clock += this.maxDelta;
          this.delta = 0;

          try {
            this.callback.call();
            this.callbackCalled = true;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        return new TestTimeReference(this.clockBase + this.delta++);
      }

      @Override
      public int maxSerializedLength() {
        return 0;
      }

      @Override
      public synchronized void setPersistenceCallback(final Callable<Void> persistenceCallback) {
        Objects.requireNonNull(persistenceCallback);
        this.callback = persistenceCallback;
        this.setCallbackCalled = true;
      }

      private void readObject(final ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();

        this.delta = this.maxDelta;
        this.callback = () -> null;
      }

      public int getClock() {
        return this.clock;
      }

      public int getMaxDelta() {
        return this.maxDelta;
      }

      public boolean isSetCallbackCalled() {
        return this.setCallbackCalled;
      }

      public boolean isCallbackCalled() {
        return this.callbackCalled;
      }
    }
  }
}
