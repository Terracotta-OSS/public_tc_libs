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
import com.terracottatech.store.definition.DoubleCellDefinition;
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
import static com.terracottatech.store.definition.CellDefinition.defineDouble;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class DoubleCellQueryingIT {

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
        .offheap("offheap", 20, MemoryUnit.MB)
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
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }
      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(1L, foo.newCell(4.0));
      access.add(2L, foo.newCell(6.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));
      access.add(4L, foo.newCell(NaN));

      assertThat(access.records().filter(foo.value().isGreaterThan(NEGATIVE_INFINITY)).count(), is(5L));
      assertThat(access.records().filter(foo.value().isGreaterThan(1.0)).count(), is(5L));
      assertThat(access.records().filter(foo.value().isGreaterThan(2.0)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isGreaterThan(3.0)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isGreaterThan(4.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThan(5.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThan(6.0)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThan(7.0)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThan(POSITIVE_INFINITY)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isGreaterThan(NaN)).count(), is(0L));
    }
  }

  @Test
  public void testGreaterThanOrEqual() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(1L, foo.newCell(4.0));
      access.add(2L, foo.newCell(6.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));
      access.add(4L, foo.newCell(NaN));

      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(NEGATIVE_INFINITY)).count(), is(6L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(1.0)).count(), is(5L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(2.0)).count(), is(5L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(3.0)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(4.0)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(5.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(6.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(7.0)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(POSITIVE_INFINITY)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(NaN)).count(), is(1L));
    }
  }

  @Test
  public void testLessThan() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(1L, foo.newCell(4.0));
      access.add(2L, foo.newCell(6.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));
      access.add(4L, foo.newCell(NaN));

      assertThat(access.records().filter(foo.value().isLessThan(NEGATIVE_INFINITY)).count(), is(0L));
      assertThat(access.records().filter(foo.value().isLessThan(1.0)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThan(2.0)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThan(3.0)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThan(4.0)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThan(5.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isLessThan(6.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isLessThan(7.0)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isLessThan(POSITIVE_INFINITY)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isLessThan(NaN)).count(), is(5L));
    }
  }

  @Test
  public void testLessThanOrEqual() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(1L, foo.newCell(4.0));
      access.add(2L, foo.newCell(6.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));
      access.add(4L, foo.newCell(NaN));

      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(NEGATIVE_INFINITY)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(1.0)).count(), is(1L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(2.0)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(3.0)).count(), is(2L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(4.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(5.0)).count(), is(3L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(6.0)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(7.0)).count(), is(4L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(POSITIVE_INFINITY)).count(), is(5L));
      assertThat(access.records().filter(foo.value().isLessThanOrEqualTo(NaN)).count(), is(6L));
    }
  }

  @Test
  public void testLambdaExpressionUnboxed() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(1L, foo.newCell(4.0));
      access.add(2L, foo.newCell(6.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));

      assertThat(access.records().filter(r -> r.get(foo).get() > NEGATIVE_INFINITY).count(), is(4L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 1.0).count(), is(4L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 2.0).count(), is(3L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 3.0).count(), is(3L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 4.0).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 5.0).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 6.0).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get() > 7.0).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get() > POSITIVE_INFINITY).count(), is(0L));
    }
  }

  /**
   * Note: primitive comparisons in lambda expressions behave differently from
   * DSL comparison methods. The former conform with IEEE 754 standard, whereas the latter
   * follow the {@link java.lang.Double} specification of equals and compareTo methods.
   */
  @Test
  public void testLambdaExpressionsUnboxedWithNan() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
            .offheap("offheap")
            .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));
      access.add(4L, foo.newCell(NaN));

      assertThat(access.records().filter(r -> r.get(foo).get() > NaN).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get() >= NaN).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get() < NaN).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get() <= NaN).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get() == NaN).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get() != NaN).count(), is(4L));
    }
  }

  @Test
  public void testLambdaExpressionBoxed() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
            .offheap("offheap")
            .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(1L, foo.newCell(4.0));
      access.add(2L, foo.newCell(6.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));

      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(NEGATIVE_INFINITY) > 0).count(), is(4L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(1.0) > 0).count(), is(4L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(2.0) > 0).count(), is(3L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(3.0) > 0).count(), is(3L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(4.0) > 0).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(5.0) > 0).count(), is(2L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(6.0) > 0).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(7.0) > 0).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(POSITIVE_INFINITY) > 0).count(), is(0L));
    }
  }

  @Test
  public void testLambdaExpressionsBoxedWithNan() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
            .offheap("offheap")
            .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));
      access.add(4L, foo.newCell(NaN));

      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(NaN) > 0).count(), is(0L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(NaN) >= 0).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(NaN) < 0).count(), is(3L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(NaN) <= 0).count(), is(4L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(NaN) == 0).count(), is(1L));
      assertThat(access.records().filter(r -> r.get(foo).get().compareTo(NaN) != 0).count(), is(3L));
    }
  }

  @Test
  public void testEquality() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(-1L, foo.newCell(NEGATIVE_INFINITY));
      access.add(0L, foo.newCell(2.0));
      access.add(1L, foo.newCell(4.0));
      access.add(2L, foo.newCell(6.0));
      access.add(3L, foo.newCell(POSITIVE_INFINITY));
      access.add(4L, foo.newCell(NaN));

      assertThat(access.records().filter(foo.value().is(NEGATIVE_INFINITY)).count(), is(1L));
      assertThat(access.records().filter(foo.value().is(4.0)).count(), is(1L));
      assertThat(access.records().filter(foo.value().is(3.0)).count(), is(0L));
      assertThat(access.records().filter(foo.value().is(POSITIVE_INFINITY)).count(), is(1L));
      assertThat(access.records().filter(foo.value().is(NaN)).count(), is(1L));
    }
  }

  @Test
  public void testSorting() throws InterruptedException, ExecutionException, StoreException {
    DoubleCellDefinition foo = defineDouble("foo");
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(POSITIVE_INFINITY));
      access.add(1L, foo.newCell(6.0));
      access.add(2L, foo.newCell(NaN));
      access.add(3L, foo.newCell(2.0));
      access.add(4L, foo.newCell(NEGATIVE_INFINITY));
      access.add(5L, foo.newCell(4.0));

      List<Double> sortedKeys = access.records().sorted(foo.valueOr(NaN).asComparator()).map(foo.valueOrFail()).collect(toList());
      assertThat(sortedKeys, contains(NEGATIVE_INFINITY, 2.0, 4.0, 6.0, POSITIVE_INFINITY, NaN));

      List<Double> reverseSortedKeys = access.records().sorted(foo.valueOr(NaN).asComparator().reversed()).map(foo.valueOrFail()).collect(toList());
      assertThat(reverseSortedKeys, contains(NaN, POSITIVE_INFINITY, 6.0, 4.0, 2.0, NEGATIVE_INFINITY));
    }
  }
}
