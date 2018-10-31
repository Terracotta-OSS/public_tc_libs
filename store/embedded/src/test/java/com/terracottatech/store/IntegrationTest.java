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

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.MutableRecordStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Stream;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.configuration.MemoryUnit.MB;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static com.terracottatech.store.UpdateOperation.write;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class IntegrationTest {

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
  public void basicCreationTest() throws StoreException {
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      access.add(0L, cell("foo", "bar"));
      assertThat(access.get(0L).flatMap(r -> r.get(define("foo", STRING))).get(), is("bar"));
    }
  }

  @Test
  public void basicDeletionTest() throws StoreException {
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      CellDefinition<String> foo = define("foo", STRING);
      CellDefinition<String> bar = define("bar", STRING);
      access.add(0L, foo.newCell("bar"), bar.newCell("foo"));
      access.delete(0L);
      access.get(0L).ifPresent(AssertionError::new);
    }
  }

  @Test
  public void basicMutationTest() throws StoreException {
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      CellDefinition<String> foo = define("foo", STRING);
      access.add(0L, foo.newCell("bar"));
      access.update(0L, write(foo).value("bat"));
      assertThat(access.get(0L).flatMap(r -> r.get(foo)).get(), is("bat"));
    }
  }

  @Test
  public void basicSearchTest() throws StoreException {
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      IntCellDefinition foo = defineInt("foo");
      access.add(0L, foo.newCell(0));
      access.add(1L, foo.newCell(1));

      try (final Stream<Record<Long>> recordStream = access.records()) {
        assertThat(recordStream.filter(foo.value().isLessThan(1)).count(), is(1L));
      }
      try (final Stream<Record<Long>> recordStream = access.records()) {
        assertThat(recordStream.filter(foo.value().isGreaterThan(1)).count(), is(0L));
      }
      ;
    }
  }

  @Test
  public void basicStreamTest() throws StoreException {
    assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
      DatasetWriterReader<Long> access = dataset.writerReader();
      IntCellDefinition foo = defineInt("foo");
      access.add(0L, foo.newCell(0));
      access.add(1L, foo.newCell(1));

      try (final MutableRecordStream<Long> recordStream = access.records()) {
        recordStream.mutate(write(foo).intResultOf(foo.intValueOr(0).increment()));
      }

      assertThat(access.get(0L).map(r -> r.get(foo)).get(), is(of(1)));
      assertThat(access.get(1L).map(r -> r.get(foo)).get(), is(of(2)));
    }
  }
}
