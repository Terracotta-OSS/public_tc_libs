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
import org.junit.Test;

import java.util.Set;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.Record.keyFunction;
import static com.terracottatech.store.Tuple.first;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.configuration.MemoryUnit.MB;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static com.terracottatech.store.UpdateOperation.custom;
import static com.terracottatech.store.UpdateOperation.write;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BasicIT {

  @Test
  public void testAdd() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        access.add(0L, cell("foo", "bar"));
        assertThat(access.get(0L).flatMap(r -> r.get(define("foo", STRING))).get(), is("bar"));
      }
    }
  }

  @Test
  public void testAddWhenPresent() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        access.add(0L, cell("foo", "bar"));
        assertThat(access.add(0L, cell("foo", "baz")), is(false));
        assertThat(access.get(0L).flatMap(r -> r.get(define("foo", STRING))).get(), is("bar"));
      }
    }
  }

  @Test
  public void testDeletion() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        CellDefinition<String> foo = define("foo", STRING);
        CellDefinition<String> bar = define("bar", STRING);
        access.add(0L, foo.newCell("bar"), bar.newCell("foo"));
        access.delete(0L);
        access.get(0L).ifPresent(r -> {
          throw new AssertionError(r);
        });
      }
    }
  }

  @Test
  public void testMappedDeletion() throws Exception {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        access.add(0L, cell("foo", "bar"), cell("bar", "foo"));
        Set<Long> expected = access.records().map(Record::getKey).collect(toSet());
        assertThat(access.records().deleteThen().map(keyFunction()).collect(toSet()), is(expected));
        access.get(0L).ifPresent(r -> {
          throw new AssertionError(r);
        });
      }
    }
  }

  @Test
  public void testDeletionWithPredicatePassing() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        CellDefinition<String> foo = define("foo", STRING);
        CellDefinition<String> bar = define("bar", STRING);
        access.add(0L, foo.newCell("bar"), bar.newCell("foo"));
        access.on(0L).iff(foo.value().is("bar")).delete().orElseThrow(AssertionError::new);
        access.get(0L).ifPresent(r -> {
          throw new AssertionError(r);
        });
      }
    }
  }

  @Test
  public void testDeletionWithPredicateFailing() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        CellDefinition<String> foo = define("foo", STRING);
        CellDefinition<String> bar = define("bar", STRING);
        access.add(0L, foo.newCell("bar"), bar.newCell("foo"));
        access.on(0L).iff(foo.value().is("foo")).delete().ifPresent(r -> {
          throw new AssertionError(r);
        });
        access.get(0L).orElseThrow(AssertionError::new);
      }
    }
  }

  @Test
  public void testApplyMutation() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
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
  }

  @Test
  public void testApplyMutationWithResult() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        CellDefinition<String> foo = define("foo", STRING);
        access.add(0L, foo.newCell("bar"));
        assertThat(access.on(0L).update(write(foo).value("bat")).map(first()).flatMap((foo.value())).get(), is("bar"));
        assertThat(access.get(0L).flatMap(r -> r.get(foo)).get(), is("bat"));
      }
    }
  }

  @Test
  public void basicSearchTest() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        IntCellDefinition foo = defineInt("foo");
        access.add(0L, foo.newCell(0));
        access.add(1L, foo.newCell(1));

        assertThat(access.records().filter(foo.value().isLessThan(1)).count(), is(1L));
        assertThat(access.records().filter(foo.value().isGreaterThan(1)).count(), is(0L));
      }
    }
  }

  @Test
  public void basicStreamTest() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        IntCellDefinition foo = defineInt("foo");
        access.add(0L, foo.newCell(0));
        access.add(1L, foo.newCell(1));

        access.records().mutate(write(foo).intResultOf(foo.intValueOr(-1).increment()));
        assertThat(access.get(0L).flatMap(r -> r.get(foo)).get(), is(1));
        assertThat(access.get(1L).flatMap(r -> r.get(foo)).get(), is(2));
      }
    }
  }

  @Test
  public void testMutationThatFails() throws StoreException {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MB).build()) {
      assertThat(datasetManager.newDataset("test", LONG, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset("test", LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        CellDefinition<String> foo = define("foo", STRING);
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
    }
  }
}
