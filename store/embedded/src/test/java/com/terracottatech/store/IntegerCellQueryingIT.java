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

import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.manager.DatasetManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class IntegerCellQueryingIT {

  @Parameters(name = "Indexing = {0}")
  public static Iterable<Object[]> settings() {
    return Arrays.asList(new Object[][] {{null}, {IndexSettings.btree()}});
  }

  @Parameter
  public IndexSettings indexSettings;
  private DatasetManager datasetManager;

  @Before
  public void setUp() throws StoreException {
    datasetManager = embedded()
        .offheap("offheap", 10, MemoryUnit.MB)
        .build();
  }

  @After
  public void tearDown() {
    if (datasetManager != null) {
      datasetManager.close();
      datasetManager = null;
    }
  }

  @Test
  public void testGreaterThan() throws InterruptedException, ExecutionException, StoreException {
    IntCellDefinition foo = defineInt("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();

      access.add(0L, foo.newCell(2));
      access.add(1L, foo.newCell(4));
      access.add(2L, foo.newCell(6));

      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      assertThat(access.records().filter(foo.value().isGreaterThan(1)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThan(2)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThan(3)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThan(4)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThan(5)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThan(6)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isGreaterThan(7)).count(), is(0L));
    }
  }

  @Test
  public void testGreaterThanOrEqual() throws InterruptedException, ExecutionException, StoreException {
    IntCellDefinition foo = defineInt("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2));
      access.add(1L, foo.newCell(4));
      access.add(2L, foo.newCell(6));

      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(1)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(2)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(3)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(4)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(5)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(6)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(7)).count(), is(0L));
    }
  }

  @Test
  public void testLessThan() throws InterruptedException, ExecutionException, StoreException {
    IntCellDefinition foo = defineInt("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2));
      access.add(1L, foo.newCell(4));
      access.add(2L, foo.newCell(6));

      assertThat(access.records().filter(foo.value().isLessThan(1)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isLessThan(2)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isLessThan(3)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThan(4)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThan(5)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThan(6)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThan(7)).count(), is(3L));
    }
  }

  @Test
  public void testLessThanOrEqual() throws InterruptedException, ExecutionException, StoreException {
    IntCellDefinition foo = defineInt("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2));
      access.add(1L, foo.newCell(4));
      access.add(2L, foo.newCell(6));

      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(1)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(2)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(3)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(4)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(5)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(6)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(7)).count(), is(3L));
    }
  }

  @Test
  public void testLambdaExpression() throws InterruptedException, ExecutionException, StoreException {
    IntCellDefinition foo = defineInt("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2));
      access.add(1L, foo.newCell(4));
      access.add(2L, foo.newCell(6));

      assertThat(access.records().filter(r -> r.get(foo).get() > 1).count(), is(3L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 2).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 3).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 4).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 5).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 6).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 7).count(), is(0L));
    }
  }

  @Test
  public void testEquality() throws InterruptedException, ExecutionException, StoreException {
    IntCellDefinition foo = defineInt("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2));
      access.add(1L, foo.newCell(4));
      access.add(2L, foo.newCell(6));

      assertThat(access.records().filter(foo.value().is(4)).count(), is(1L));
      assertThat(access.records().filter(foo.value().is(3)).count(), is(0L));
    }
  }

  @Test
  public void testSorting() throws InterruptedException, ExecutionException, StoreException {
    IntCellDefinition foo = defineInt("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(6));
      access.add(1L, foo.newCell(2));
      access.add(2L, foo.newCell(4));

      List<Long> sortedKeys = access.records()
          .sorted(foo.valueOr(-1).asComparator())
          .map(Record::getKey)
          .collect(toList());
      assertThat(sortedKeys, contains(1L, 2L, 0L));

      sortedKeys = access.records()
          .sorted(foo.valueOr(-1).asComparator().reversed())
          .map(Record::getKey)
          .collect(toList());
      assertThat(sortedKeys, contains(0L, 2L, 1L));
    }
  }
}
