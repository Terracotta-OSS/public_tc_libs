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

import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.definition.LongCellDefinition;
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
import static com.terracottatech.store.configuration.MemoryUnit.MB;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class LongCellQueryingIT {

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
        .offheap("offheap", 10, MB)
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
    LongCellDefinition foo = defineLong("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();

      access.add(0L, foo.newCell(2L));
      access.add(1L, foo.newCell(4L));
      access.add(2L, foo.newCell(6L));

      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      assertThat(access.records().filter(foo.value().isGreaterThan(1L)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThan(2L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThan(3L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThan(4L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThan(5L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThan(6L)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isGreaterThan(7L)).count(), is(0L));
    }
  }

  @Test
  public void testGreaterThanOrEqual() throws InterruptedException, ExecutionException, StoreException {
    LongCellDefinition foo = defineLong("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2L));
      access.add(1L, foo.newCell(4L));
      access.add(2L, foo.newCell(6L));

      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(1L)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(2L)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(3L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(4L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(5L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(6L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(7L)).count(), is(0L));
    }
  }

  @Test
  public void testLessThan() throws InterruptedException, ExecutionException, StoreException {
    LongCellDefinition foo = defineLong("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2L));
      access.add(1L, foo.newCell(4L));
      access.add(2L, foo.newCell(6L));

      assertThat(access.records().filter(foo.value().isLessThan(1L)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isLessThan(2L)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isLessThan(3L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThan(4L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThan(5L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThan(6L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThan(7L)).count(), is(3L));
    }
  }

  @Test
  public void testLessThanOrEqual() throws InterruptedException, ExecutionException, StoreException {
    LongCellDefinition foo = defineLong("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2L));
      access.add(1L, foo.newCell(4L));
      access.add(2L, foo.newCell(6L));

      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(1L)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(2L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(3L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(4L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(5L)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(6L)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(7L)).count(), is(3L));
    }
  }

  @Test
  public void testLambdaExpression() throws InterruptedException, ExecutionException, StoreException {
    LongCellDefinition foo = defineLong("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2L));
      access.add(1L, foo.newCell(4L));
      access.add(2L, foo.newCell(6L));

      assertThat(access.records().filter(r -> r.get(foo).get() > 1L).count(), is(3L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 2L).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 3L).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 4L).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 5L).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 6L).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 7L).count(), is(0L));
    }
  }

  @Test
  public void testEquality() throws InterruptedException, ExecutionException, StoreException {
    LongCellDefinition foo = defineLong("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(2L));
      access.add(1L, foo.newCell(4L));
      access.add(2L, foo.newCell(6L));

      assertThat(access.records().filter(foo.value().is(4L)).count(), is(1L));
      assertThat(access.records().filter(foo.value().is(3L)).count(), is(0L));
    }
  }

  @Test
  public void testSorting() throws InterruptedException, ExecutionException, StoreException {
    LongCellDefinition foo = defineLong("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(6L));
      access.add(1L, foo.newCell(2L));
      access.add(2L, foo.newCell(4L));

      List<Long> sortedKeys = access.records()
          .sorted(foo.valueOr(-1L).asComparator())
          .map(Record::getKey)
          .collect(toList());
      assertThat(sortedKeys, contains(1L, 2L, 0L));

      sortedKeys = access.records()
          .sorted(foo.valueOr(-1L).asComparator().reversed())
          .map(Record::getKey)
          .collect(toList());
      assertThat(sortedKeys, contains(0L, 2L, 1L));
    }
  }
}
