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

import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.PersistenceMode;
import com.terracottatech.tool.Diagnostics;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.UpdateOperation.custom;
import static com.terracottatech.store.UpdateOperation.write;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.NEW;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BasicPersistenceIT {

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder dataDir = new TemporaryFolder();

  DatasetManager datasetManager;
  Dataset<Long> dataset;
  DatasetWriterReader<Long> access;

  @Before
  public void setUp() throws Exception {
    if (getStorageType() == null) {
      String name = getMode().name();
      datasetManager = embedded()
          .disk("disk", dataDir.newFolder(name + "-persistence-it").toPath(), getMode(), REOPEN_OR_NEW)
          .offheap("offheap", 4, MemoryUnit.MB)
          .offheap("another-offheap", 4, MemoryUnit.MB)
          .build();
      datasetManager.newDataset("dummyDataset", Type.LONG, datasetManager.datasetConfiguration()
          .disk("disk").offheap("offheap").build());
    } else {
      String name = getStorageType().getShortName();
      datasetManager = embedded()
          .disk("disk", dataDir.newFolder(name + "-persistence-it").toPath(), REOPEN_OR_NEW)
          .offheap("offheap", 4, MemoryUnit.MB)
          .offheap("another-offheap", 4, MemoryUnit.MB)
          .build();
      datasetManager.newDataset("dummyDataset", Type.LONG, datasetManager.datasetConfiguration()
          .disk("disk", getStorageType()).offheap("offheap").build());

    }
    dataset = datasetManager.getDataset("dummyDataset", LONG);
    access = dataset.writerReader();
  }

  protected PersistenceMode getMode() {
    return PersistenceMode.INMEMORY;
  }

  protected PersistentStorageType getStorageType() {
    return null;
  }

  @After
  public void tearDown() throws Exception {
    if (dataset != null) {
      dataset.close();
    }
    datasetManager.destroyDataset("dummyDataset");

    if (datasetManager != null) {
      datasetManager.close();
      datasetManager = null;
    }

    // attempt an explicit GC to handle slow build machines
    Runtime runtime = Runtime.getRuntime();
    for (int i = 0; i < 10; i++) {
      runtime.gc();
      runtime.runFinalization();
      Thread.yield(); Thread.yield(); Thread.yield();
    }
  }

  @Test
  public void testLastDatasetDestroyDisposesStorage() throws Exception {
    CellDefinition<String> foo = define("foo", Type.STRING);
    access.add(1L, foo.newCell("fooVal"));
    assertThat(access.get(1L).isPresent(), is(true));

    DatasetConfiguration configuration = datasetManager.datasetConfiguration().disk("disk").offheap("offheap").build();
    datasetManager.newDataset("datasetString", Type.STRING, configuration);
    datasetManager.destroyDataset("datasetString");

    //First dataset can still be used
    assertThat(access.get(1L).isPresent(), is(true));
  }

  @Test
  public void testOffheapDiskAssociationBrokenAfterDatasetDestroy() throws Exception {
    dataset.close();
    datasetManager.destroyDataset("dummyDataset");
    datasetManager.newDataset("dummyDataset", Type.LONG, datasetManager.datasetConfiguration().disk("disk").offheap("another-offheap").build());
  }

  @Test
  public void testListDatasets() throws Exception {
    assertThat(datasetManager.listDatasets().size(), is(1));

    DatasetConfiguration configuration = datasetManager.datasetConfiguration().disk("disk").offheap("offheap").build();
    datasetManager.newDataset("datasetLong", Type.LONG, configuration);
    datasetManager.newDataset("datasetString", Type.STRING, configuration);

    Map<String, Type<?>> datasets = datasetManager.listDatasets();
    assertThat(datasets.size(), is(3));
    assertThat(datasets.get("dummyDataset"), is(Type.LONG));
    assertThat(datasets.get("datasetLong"), is(Type.LONG));
    assertThat(datasets.get("datasetString"), is(Type.STRING));

    datasetManager.destroyDataset("datasetString");
    datasets = datasetManager.listDatasets();
    assertThat(datasets.size(), is(2));
    assertThat(datasets.get("dummyDataset"), is(Type.LONG));
    assertThat(datasets.get("datasetLong"), is(Type.LONG));

    datasetManager.destroyDataset("datasetLong");
    datasets = datasetManager.listDatasets();
    assertThat(datasets.size(), is(1));
    assertThat(datasets.get("dummyDataset"), is(Type.LONG));
  }

  @Test
  public void testAdd() {
    access.add(0L, cell("foo", "bar"));
    assertThat(access.get(0L).flatMap(r -> r.get(define("foo", Type.STRING))).get(), is("bar"));
  }

  @Test
  public void testAddWhenPresent() {
    access.add(0L, cell("foo", "bar"));
    assertThat(access.add(0L, cell("foo", "baz")), is(false));
    assertThat(access.get(0L).flatMap(r -> r.get(define("foo", Type.STRING))).get(), is("bar"));
  }

  @Test
  public void testDeletion() {
    CellDefinition<String> foo = define("foo", Type.STRING);
    CellDefinition<String> bar = define("bar", Type.STRING);
    access.add(0L, foo.newCell("bar"), bar.newCell("foo"));
    access.delete(0L);
    access.get(0L).ifPresent(r -> {
      throw new AssertionError(r);
    });
  }

  @Test
  public void testDeletionWithPredicatePassing() {
    CellDefinition<String> foo = define("foo", Type.STRING);
    CellDefinition<String> bar = define("bar", Type.STRING);
    access.add(0L, foo.newCell("bar"), bar.newCell("foo"));
    access.on(0L).iff(foo.value().is("bar")).delete().orElseThrow(AssertionError::new);
    access.get(0L).ifPresent(r -> {
      throw new AssertionError(r);
    });
  }

  @Test
  public void testDeletionWithPredicateFailing() {
    CellDefinition<String> foo = define("foo", Type.STRING);
    CellDefinition<String> bar = define("bar", Type.STRING);
    access.add(0L, foo.newCell("bar"), bar.newCell("foo"));
    access.on(0L).iff(foo.value().is("foo")).delete().ifPresent(AssertionError::new);
    access.get(0L).orElseThrow(AssertionError::new);
  }

  @Test
  public void testApplyMutation() {
    CellDefinition<String> foo = define("foo", Type.STRING);
    access.add(0L, foo.newCell("bar"));
    access.update(0L, write(foo).value("bat"));
    assertThat(access.get(0L).flatMap(r -> r.get(foo)).get(), is("bat"));
  }

  @Test
  public void testApplyMutationWithResult() {
    CellDefinition<String> foo = define("foo", Type.STRING);
    access.add(0L, foo.newCell("bar"));
    assertThat(access.on(0L).update(write(foo).value("bat")).map(Tuple.first()).flatMap(foo.value()).get(), is("bar"));
    assertThat(access.get(0L).flatMap(r -> r.get(foo)).get(), is("bat"));
  }

  @Test
  public void basicSearchTest() {
    IntCellDefinition foo = defineInt("foo");
    access.add(0L, foo.newCell(0));
    access.add(1L, foo.newCell(1));

    assertThat(access.records().filter(foo.value().isLessThan(1)).count(), is(1L));
    assertThat(access.records().filter(foo.value().isGreaterThan(1)).count(), is(0L));
  }

  @Test
  public void basicStreamTest() {
    IntCellDefinition foo = defineInt("foo");
    access.add(0L, foo.newCell(0));
    access.add(1L, foo.newCell(1));

    access.records().mutate(write(foo).intResultOf(foo.intValueOr(-1).increment()));

    assertThat(access.get(0L).flatMap(r -> r.get(foo)).get(), is(1));
    assertThat(access.get(1L).flatMap(r -> r.get(foo)).get(), is(2));
  }

  @Test
  public void testMutationThatFails() {
    CellDefinition<String> foo = define("foo", Type.STRING);
    access.add(0L, foo.newCell("bar"));
    try {
      access.update(0L, custom(r -> {
        throw new IllegalStateException();
      }));
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }
    assertThat(access.get(0L).flatMap(r -> r.get(foo)).get(), is("bar"));
  }

  @Test
  public void testPersistentDataRetrieval() throws Exception {
    DatasetConfiguration embeddedConfig = datasetManager.datasetConfiguration().disk("disk").offheap("offheap").build();

    Path dataFile = dataDir.newFolder("terracotta").toPath();

      //Creating a persistent dataset and adding some records
      try (DatasetManager datasetManager = embedded().disk("disk", dataFile, getMode(), NEW).offheap("offheap", 10, MemoryUnit.MB).build()) {
        assertThat(datasetManager.newDataset("dummyDataset", STRING, embeddedConfig), is(true));
        try (Dataset<String> dummyDataset = datasetManager.getDataset("dummyDataset", STRING)) {
          dummyDataset.writerReader().add("dummyKey");
        }
      }

      //Retrieving the already created persistent dataset and checking if the persisted data can be retrieved
      try (DatasetManager datasetManager = embedded().disk("disk", dataFile, getMode(), REOPEN).offheap("offheap", 10, MemoryUnit.MB).build()) {
        try (Dataset<String> dummyDataset = datasetManager.getDataset("dummyDataset", Type.STRING)) {
          assertNotNull(dummyDataset.reader().get("dummyKey"));
        }
      }
  }

  @SuppressWarnings("unused")
  protected void dumpHeap(String annotation) {
    try {
      Diagnostics.dumpHeap(true,
          String.format("%1$s_%2$tFT%2$tH%2$tM%2$tS.%2$tL_%3$s.hprof", testName.getMethodName(), new Date(), annotation));
    } catch (IOException e) {
      System.err.format("Failed to write heap dump for %s: %s%n", testName.getMethodName(), e);
    }
  }

}
