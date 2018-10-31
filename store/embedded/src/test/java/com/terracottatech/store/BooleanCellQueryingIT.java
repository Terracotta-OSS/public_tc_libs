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

import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.definition.BoolCellDefinition;
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

import static com.terracottatech.store.Type.BOOL;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class BooleanCellQueryingIT {

  @Parameters(name = "Indexing = {0}")
  public static Iterable<Object[]> settings() {
    return Arrays.asList(new Object[][] {{null}, {IndexSettings.btree()}});
  }

  @Parameter
  public IndexSettings indexSettings;
  private DatasetManager datasetManager;
  private DatasetConfiguration configuration;

  @Before
  public void setUp() throws StoreException {
    datasetManager = embedded().offheap("offheap", 10, MemoryUnit.MB).build();
    DatasetConfigurationBuilder configurationBuilder = datasetManager.datasetConfiguration().offheap("offheap");
    configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
    configuration = configurationBuilder.build();
  }

  @After
  public void tearDown() throws Exception {
    if (datasetManager != null) {
      datasetManager.close();
      datasetManager = null;
    }
  }

  @Test
  public void testLambdaExpression() throws InterruptedException, ExecutionException, StoreException {
    CellDefinition<Boolean> foo = define("foo", BOOL);
    assertThat(datasetManager.newDataset("test", LONG, configuration), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(true));
      access.add(1L, foo.newCell(false));

      assertThat(access.records().filter(r -> r.get(foo).get()).count(), is(1L));
      assertThat(access.records().filter(r -> !r.get(foo).get()).count(), is(1L));
    }
  }

  @Test
  public void testAsPredicate() throws InterruptedException, ExecutionException, StoreException {
    BoolCellDefinition foo = defineBool("foo");
    assertThat(datasetManager.newDataset("test", LONG, configuration), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(true));
      access.add(1L, foo.newCell(false));

      assertThat(access.records().filter(foo.isTrue()).count(), is(1L));
      assertThat(access.records().filter(foo.isFalse()).count(), is(1L));
    }
  }

  @Test
  public void testSorting() throws InterruptedException, ExecutionException, StoreException {
    BoolCellDefinition foo = defineBool("foo");
    assertThat(datasetManager.newDataset("test", LONG, configuration), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
      }

      access.add(0L, foo.newCell(true));
      access.add(1L, foo.newCell(false));

      List<Long> sortedKeys = access.records()
          .sorted(foo.valueOr(TRUE).asComparator())
          .map(Record::getKey)
          .collect(toList());
      assertThat(sortedKeys, contains(1L, 0L));

      sortedKeys = access.records()
          .sorted(foo.valueOr(TRUE).asComparator().reversed())
          .map(Record::getKey)
          .collect(toList());
      assertThat(sortedKeys, contains(0L, 1L));
    }
  }

  @Test
  public void testCrossCellComparison() throws InterruptedException, ExecutionException, StoreException {
    BoolCellDefinition foo = defineBool("foo");
    BoolCellDefinition bar = defineBool("bar");
    assertThat(datasetManager.newDataset("test", LONG, configuration), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      if (indexSettings != null) {
        dataset.getIndexing().createIndex(foo, indexSettings).get();
        dataset.getIndexing().createIndex(bar, indexSettings).get();
      }

      access.add(0L, foo.newCell(false), bar.newCell(false));
      access.add(1L, foo.newCell(false), bar.newCell(true));
      access.add(3L, foo.newCell(true), bar.newCell(false));
      access.add(2L, foo.newCell(true), bar.newCell(true));

      assertThat(access.records().filter(foo.isTrue().and(bar.isTrue())).count(), is(1L));
    }
  }
}
